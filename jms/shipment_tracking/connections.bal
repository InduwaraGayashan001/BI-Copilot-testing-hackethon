import ballerinax/activemq.driver as _;
import ballerinax/java.jms;

// Listener bound to the SHIPMENT.STATUS.IN queue. The consuming service is configured with
// client acknowledgement mode so messages are only removed once they have either been processed
// successfully or forwarded to the invalid-message queue.
listener jms:Listener shipmentStatusListener = check new (
    connectionConfig = {
        initialContextFactory: "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
        providerUrl: providerUrl
    },
    acknowledgementMode = jms:CLIENT_ACKNOWLEDGE,
    consumerOptions = {
        destination: {
            'type: jms:QUEUE,
            name: shipmentStatusInQueue
        }
    }
);

// Separate connection and session used to publish messages that fail fixed-width parsing onward
// to SHIPMENT.STATUS.INVALID, and to publish accepted/exception events to their routed queues.
final jms:Connection shipmentStatusPublishConnection = check new (
    initialContextFactory = "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
    providerUrl = providerUrl
);

final jms:Session shipmentStatusPublishSession = check createJmsSession(shipmentStatusPublishConnection, jms:AUTO_ACKNOWLEDGE);

final jms:MessageProducer shipmentStatusInvalidProducer = check shipmentStatusPublishSession.createProducer({
    'type: jms:QUEUE,
    name: shipmentStatusInvalidQueue
});

// Unbound producer used with sendTo() to publish accepted and exception events to whichever
// queue carrier-based routing resolves to.
final jms:MessageProducer shipmentStatusRoutedProducer = check shipmentStatusPublishSession.createProducer();

// Unbound producer used with sendTo() to route poison replay messages to the DLQ during
// reconciliation.
final jms:MessageProducer shipmentStatusDlqProducer = check shipmentStatusPublishSession.createProducer();

// Separate connection and transacted session used to drain SHIPMENT.STATUS.REPLAY during the
// nightly reconciliation window, with one commit per batch.
final jms:Connection shipmentStatusReplayConnection = check new (
    initialContextFactory = "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
    providerUrl = providerUrl
);

final jms:Session shipmentStatusReplaySession = check createJmsSession(shipmentStatusReplayConnection, jms:SESSION_TRANSACTED);

final jms:MessageConsumer shipmentStatusReplayConsumer = check shipmentStatusReplaySession.createConsumer(
    destination = {
        'type: jms:QUEUE,
        name: shipmentStatusReplayQueue
    }
);

function createJmsSession(jms:Connection connection, jms:AcknowledgementMode ackMode) returns jms:Session|error {
    return connection->createSession(ackMode);
}
