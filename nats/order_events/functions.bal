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
