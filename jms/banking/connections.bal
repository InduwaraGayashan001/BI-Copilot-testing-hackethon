import ballerinax/activemq.driver as _;
import ballerinax/java.jms;

final jms:Connection jmsConnection = check new (
    initialContextFactory = "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
    providerUrl = providerUrl
);

final jms:Session jmsSession = check createJmsSession(jmsConnection);

final jms:MessageProducer coreTransferRequestProducer = check jmsSession.createProducer({
    'type: jms:QUEUE,
    name: "CORE.TRANSFER.REQUEST"
});

function createJmsSession(jms:Connection connection) returns jms:Session|error {
    return connection->createSession();
}
