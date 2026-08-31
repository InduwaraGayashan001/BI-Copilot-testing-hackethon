import ballerina/log;
import ballerinax/nats;

// Consumes orders.created with automatic acknowledgement turned off. The message is only
// acknowledged once the order has been successfully persisted. While persistence is still
// in progress the redelivery timer is periodically reset via inProgress, and a transient
// persistence failure triggers a nak so the server redelivers the message.
@nats:StreamServiceConfig {
    subject: "orders.created",
    autoAck: false
}
service on new nats:Listener(natsUrl, connectionName = connectionName) {

    remote function onMessage(nats:AnydataMessage message) returns error? {
        nats:JetStreamCaller caller = check new (natsClient);
        OrderEvent|error orderEvent = anydataToOrderEvent(message.content);
        if orderEvent is error {
            log:printError(string `Discarding unparsable message on subject ${message.subject}`,
                    'error = orderEvent);
            caller->ack();
            return;
        }

        TransientPersistenceError? persistResult = persistOrderWithProgress(orderEvent, caller);
        if persistResult is TransientPersistenceError {
            log:printWarn(string `Transient failure while persisting order ${orderEvent.orderId}, requesting redelivery`,
                    'error = persistResult);
            caller->nak();
            return;
        }

        caller->ack();
        log:printInfo(string `Order ${orderEvent.orderId} persisted and acknowledged`);
    }
}
