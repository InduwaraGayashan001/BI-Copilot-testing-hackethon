import ballerina/lang.runtime;
import ballerina/log;
import ballerinax/nats;

// Creates the ORDERS stream if it does not exist, or loads the existing one otherwise.
function initOrdersStream() returns nats:Error? {
    check jetStreamClient->addStream(ordersStreamConfig);
}

// Publishes a typed order event to the subject orders.created.
function publishOrderCreatedEvent(OrderEvent orderEvent) returns nats:Error? {
    byte[] content = orderEvent.toJsonString().toBytes();
    nats:JetStreamMessage message = {
        content,
        subject: "orders.created"
    };
    check jetStreamClient->publishMessage(message);
}

// Converts a JetStream message payload delivered as raw bytes into a typed OrderEvent.
function bytesToOrderEvent(byte[] content) returns OrderEvent|error {
    string payload = check string:fromBytes(content);
    json orderEventJson = check payload.fromJsonString();
    return orderEventJson.cloneWithType(OrderEvent);
}

// Converts the anydata content of a consumed JetStream service message into a typed OrderEvent.
function anydataToOrderEvent(anydata content) returns OrderEvent|error {
    if content is byte[] {
        return bytesToOrderEvent(content);
    }
    return content.cloneWithType(OrderEvent);
}

// Persists the order, sending periodic in-progress signals via the supplied caller so the
// server keeps resetting the redelivery timer while work is still under way. A transient
// downstream failure is surfaced as a TransientPersistenceError so the caller can nak and
// have the message redelivered.
function persistOrderWithProgress(OrderEvent orderEvent, nats:JetStreamCaller caller) returns TransientPersistenceError? {
    future<error?> inProgressSignaller = start signalInProgressPeriodically(caller);
    error? persistResult = persistOrder(orderEvent);
    check inProgressSignaller.cancel();
    if persistResult is error {
        return error TransientPersistenceError(string `Failed to persist order ${orderEvent.orderId}`,
                persistResult);
    }
}

// Repeatedly signals the server that the message is still being worked on, at the
// configured interval, until cancelled by the caller once processing completes.
function signalInProgressPeriodically(nats:JetStreamCaller caller) returns error? {
    while true {
        runtime:sleep(inProgressSignalInterval);
        caller->inProgress();
        log:printDebug("Sent in-progress signal to reset the redelivery timer");
    }
}

// Simulates persisting the order to a datastore. Replace with the actual persistence logic.
function persistOrder(OrderEvent orderEvent) returns error? {
    log:printInfo(string `Persisting order ${orderEvent.orderId} for customer ${orderEvent.customerId}`);
}

// Pulls a single message synchronously from the orders.created subject and acknowledges it.
function replayNextOrderEvent() returns ReplayResult|error {
    nats:JetStreamMessage message = check jetStreamClient->consumeMessage("orders.created", replayConsumeTimeout);
    OrderEvent orderEvent = check bytesToOrderEvent(message.content);
    jetStreamClient->ack(message);
    return {subject: message.subject, orderEvent};
}

// Empties the ORDERS stream if the purge feature flag is enabled.
function purgeOrdersStream() returns PurgeResult|error {
    if !purgeEnabled {
        return error("Purging the orders stream is disabled; enable the purgeEnabled configuration to allow it");
    }
    check jetStreamClient->purgeStream(ordersStreamName);
    return {streamName: ordersStreamName, status: "purged"};
}
