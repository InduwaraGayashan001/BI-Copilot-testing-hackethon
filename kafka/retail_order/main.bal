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
            OrderEvent orderEvent = orderEventRecord.value;
            error? processResult = processOrderEvent(orderEvent);
            if processResult is error {
                log:printError("Failed to process order event, batch will not be committed",
                        'error = processResult, orderId = orderEvent.orderId);
                return processResult;
            }
        }

        kafka:Error? commitResult = caller->commit();
        if commitResult is kafka:Error {
            log:printError("Failed to commit offsets for the processed batch", 'error = commitResult);
            return commitResult;
        }
        log:printInfo("Successfully processed and committed batch", batchSize = records.length());
    }

    remote function onError(kafka:Error err) returns error? {
        log:printError("Error while consuming order events", 'error = err);
    }
}
