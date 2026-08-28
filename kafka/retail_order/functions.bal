import ballerina/lang.runtime;
import ballerina/log;
import ballerina/sql;
import ballerinax/kafka;

const string ORDERS_ENRICHED_TOPIC = "orders.enriched";
const string ORDERS_DLQ_TOPIC = "orders.dlq";

// Validates the structural integrity of an order event. Returns an
// `InvalidOrderEventError` when the payload is malformed and must not be retried.
function validateOrderEvent(OrderEvent orderEvent) returns InvalidOrderEventError? {
    if orderEvent.orderId.trim().length() == 0 {
        return error InvalidOrderEventError("Order event is missing orderId");
    }
    if orderEvent.customerId.trim().length() == 0 {
        return error InvalidOrderEventError("Order event is missing customerId", orderId = orderEvent.orderId);
    }
    if orderEvent.orderAmount <= 0d {
        return error InvalidOrderEventError("Order event has a non-positive orderAmount",
                orderId = orderEvent.orderId);
    }
    if orderEvent.currency.trim().length() == 0 {
        return error InvalidOrderEventError("Order event is missing currency", orderId = orderEvent.orderId);
    }
    if orderEvent.itemCount <= 0 {
        return error InvalidOrderEventError("Order event has a non-positive itemCount",
                orderId = orderEvent.orderId);
    }
    if orderEvent.channel.trim().length() == 0 {
        return error InvalidOrderEventError("Order event is missing channel", orderId = orderEvent.orderId);
    }
    return;
}

// Fetches customer tier, email, and country from the MySQL `customers` table.
function fetchCustomerInfo(string customerId) returns CustomerInfo|error {
    sql:ParameterizedQuery query = `SELECT tier, email, country FROM customers WHERE customer_id = ${customerId}`;
    CustomerInfo|sql:Error customerInfo = customerDbClient->queryRow(query);
    if customerInfo is sql:Error {
        return error RetryableProcessingError("Failed to fetch customer information", customerInfo,
                customerId = customerId);
    }
    return customerInfo;
}

// Enriches an order event with customer tier, email, and country.
function enrichOrderEvent(OrderEvent orderEvent) returns EnrichedOrder|error {
    CustomerInfo customerInfo = check fetchCustomerInfo(orderEvent.customerId);
    EnrichedOrder enrichedOrder = {
        orderId: orderEvent.orderId,
        customerId: orderEvent.customerId,
        orderAmount: orderEvent.orderAmount,
        currency: orderEvent.currency,
        itemCount: orderEvent.itemCount,
        channel: orderEvent.channel,
        customerTier: customerInfo.tier,
        customerEmail: customerInfo.email,
        customerCountry: customerInfo.country
    };
    return enrichedOrder;
}

// Publishes the enriched order to the `orders.enriched` topic.
function publishEnrichedOrder(EnrichedOrder enrichedOrder) returns error? {
    kafka:Error? sendResult = orderEventProducer->send({
        topic: ORDERS_ENRICHED_TOPIC,
        key: enrichedOrder.orderId.toBytes(),
        value: enrichedOrder.toJson().toJsonString().toBytes()
    });
    if sendResult is kafka:Error {
        return error RetryableProcessingError("Failed to publish enriched order", sendResult,
                orderId = enrichedOrder.orderId);
    }
    return;
}

// Enriches and publishes a single order event, retrying transient failures with
// exponential backoff up to `maxRetryAttempts` times.
function enrichAndPublishWithRetry(OrderEvent orderEvent) returns error? {
    decimal currentDelaySeconds = initialRetryDelaySeconds;
    int attempt = 1;
    while true {
        EnrichedOrder|error enrichResult = enrichOrderEvent(orderEvent);
        if enrichResult is EnrichedOrder {
            error? publishResult = publishEnrichedOrder(enrichResult);
            if publishResult is () {
                return;
            }
            error? retryOutcome = handleRetryOutcome(orderEvent.orderId, publishResult, attempt, currentDelaySeconds);
            if retryOutcome is error {
                return retryOutcome;
            }
        } else {
            error? retryOutcome = handleRetryOutcome(orderEvent.orderId, enrichResult, attempt, currentDelaySeconds);
            if retryOutcome is error {
                return retryOutcome;
            }
        }
        attempt += 1;
        if currentDelaySeconds * 2d < maxRetryDelaySeconds {
            currentDelaySeconds = currentDelaySeconds * 2d;
        } else {
            currentDelaySeconds = maxRetryDelaySeconds;
        }
    }
}

// Decides whether to retry or give up, based on the current attempt count.
// Returns `()` to signal a retry should happen, or the terminal error otherwise.
function handleRetryOutcome(string orderId, error failure, int attempt, decimal delaySeconds) returns error? {
    if attempt >= maxRetryAttempts {
        return failure;
    }
    log:printWarn("Retrying order enrichment/publish after failure", orderId = orderId,
            attempt = attempt, nextDelaySeconds = delaySeconds, 'error = failure);
    runtime:sleep(delaySeconds);
    return;
}

// Publishes a failed record to the `orders.dlq` topic with the failure reason in the headers.
function publishToDlq(byte[] rawValue, string failureReason, string orderId) returns error? {
    kafka:Error? sendResult = orderEventProducer->send({
        topic: ORDERS_DLQ_TOPIC,
        key: orderId.toBytes(),
        value: rawValue,
        headers: {
            "x-failure-reason": failureReason,
            "x-order-id": orderId
        }
    });
    if sendResult is kafka:Error {
        log:printError("Failed to publish record to the DLQ", 'error = sendResult, orderId = orderId);
        return sendResult;
    }
    return;
}
