import ballerina/log;
import ballerinax/kafka;

const string ORDER_PROCESSING_GROUP = "order-processing-service";
const string ORDERS_CREATED_TOPIC = "orders.created";

kafka:ConsumerConfiguration orderConsumerConfiguration = {
    groupId: ORDER_PROCESSING_GROUP,
    topics: [ORDERS_CREATED_TOPIC],
    offsetReset: "earliest",
    autoCommit: false,
    pollingInterval: 1
};

listener kafka:Listener orderKafkaListener = new (kafkaBootstrapServers, orderConsumerConfiguration);

service kafka:Service on orderKafkaListener {

    remote function onConsumerRecord(kafka:Caller caller, OrderEventConsumerRecord[] records) returns error? {
        foreach OrderEventConsumerRecord orderEventRecord in records {
            handleOrderEventRecord(orderEventRecord);
        }

        kafka:Error? commitResult = caller->commit();
        if commitResult is kafka:Error {
            log:printError("Failed to commit offsets for the processed batch", 'error = commitResult);
            return commitResult;
        }
        log:printInfo("Successfully processed batch", batchSize = records.length());
    }

    remote function onError(kafka:Error err) returns error? {
        log:printError("Error while consuming order events", 'error = err);
    }
}

// Handles a single order event record end-to-end: malformed payloads are routed
// straight to the DLQ, valid ones are enriched and published with retries, and any
// exhausted retries fall back to the DLQ as well. A single bad record never
// prevents the rest of the batch, or the batch commit, from proceeding.
function handleOrderEventRecord(OrderEventConsumerRecord orderEventRecord) {
    OrderEvent orderEvent = orderEventRecord.value;
    byte[] rawValue = orderEvent.toJsonString().toBytes();

    InvalidOrderEventError? validationError = validateOrderEvent(orderEvent);
    if validationError is InvalidOrderEventError {
        log:printWarn("Order event failed validation, routing to DLQ without retry",
                orderId = orderEvent.orderId, 'error = validationError);
        error? dlqResult = publishToDlq(rawValue, validationError.message(), orderEvent.orderId);
        if dlqResult is error {
            log:printError("Failed to route invalid order event to DLQ", 'error = dlqResult,
                    orderId = orderEvent.orderId);
        }
        return;
    }

    error? processResult = enrichAndPublishWithRetry(orderEvent);
    if processResult is error {
        log:printError("Order event enrichment/publish failed after retries, routing to DLQ",
                'error = processResult, orderId = orderEvent.orderId);
        error? dlqResult = publishToDlq(rawValue, processResult.message(), orderEvent.orderId);
        if dlqResult is error {
            log:printError("Failed to route failed order event to DLQ", 'error = dlqResult,
                    orderId = orderEvent.orderId);
        }
    }
}
