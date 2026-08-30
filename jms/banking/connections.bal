import ballerinax/activemq.driver as _;
import ballerinax/java.jms;

final jms:Connection jmsConnection = check new (
    initialContextFactory = "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
    providerUrl = providerUrl
);

// Transacted session used for the transfer submission path so that the JMS send and the
// audit write can be committed or rolled back together as a single unit of work.
final jms:Session jmsTransferSession = check createJmsSession(jmsConnection, jms:SESSION_TRANSACTED);

final jms:MessageProducer coreTransferRequestProducer = check jmsTransferSession.createProducer({
    'type: jms:QUEUE,
    name: "CORE.TRANSFER.REQUEST"
});

// Separate, non-transacted session used to forward replies that could not be correlated to a
// pending transfer onward to the unmatched queue.
final jms:Session jmsUnmatchedSession = check createJmsSession(jmsConnection, jms:AUTO_ACKNOWLEDGE);

final jms:MessageProducer coreTransferUnmatchedProducer = check jmsUnmatchedSession.createProducer({
    'type: jms:QUEUE,
    name: "CORE.TRANSFER.UNMATCHED"
});

// Dedicated session used to drive the synchronous balance enquiry request-reply exchange.
final jms:Session jmsEnquirySession = check createJmsSession(jmsConnection, jms:AUTO_ACKNOWLEDGE);

final jms:MessageProducer coreEnquiryRequestProducer = check jmsEnquirySession.createProducer({
    'type: jms:QUEUE,
    name: "CORE.ENQUIRY.REQUEST"
});

function createJmsSession(jms:Connection connection, jms:AcknowledgementMode ackMode) returns jms:Session|error {
    return connection->createSession(ackMode);
}
