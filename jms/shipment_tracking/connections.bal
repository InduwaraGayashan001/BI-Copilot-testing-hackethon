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
// to SHIPMENT.STATUS.INVALID.
final jms:Connection shipmentStatusPublishConnection = check new (
    initialContextFactory = "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
    providerUrl = providerUrl
);

final jms:Session shipmentStatusPublishSession = check createJmsSession(shipmentStatusPublishConnection);

final jms:MessageProducer shipmentStatusInvalidProducer = check shipmentStatusPublishSession.createProducer({
    'type: jms:QUEUE,
    name: shipmentStatusInvalidQueue
});

function createJmsSession(jms:Connection connection) returns jms:Session|error {
    return connection->createSession(jms:AUTO_ACKNOWLEDGE);
}
