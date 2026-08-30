import ballerinax/activemq.driver as _;
import ballerinax/java.jms;

// Builds a message selector that restricts delivery to the configured instrument classes, e.g.
// instrumentClass IN ('EQUITY', 'FX').
function buildInstrumentClassSelector(string[] instrumentClasses) returns string {
    string[] quotedClasses = from string instrumentClass in instrumentClasses
        select string `'${instrumentClass}'`;
    string classList = string:'join(", ", ...quotedClasses);
    return string `instrumentClass IN (${classList})`;
}

final string instrumentClassSelector = buildInstrumentClassSelector(instrumentClasses);

// ActiveMQ connection factories accept the JMS client id as a query parameter on the broker URL
// since jms:ConnectionConfiguration has no dedicated clientId field. The client id must stay
// stable across restarts so the broker can identify and resume the durable subscription.
final string providerUrlWithClientId = string `${providerUrl}?jms.clientID=${clientId}`;

// Durable subscription on MARKET.DATA.PRICES so ticks published while this service is down are
// retained by the broker and delivered once it reconnects with the same clientId/subscriberName.
listener jms:Listener marketDataPricesListener = check new (
    connectionConfig = {
        initialContextFactory: "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
        providerUrl: providerUrlWithClientId
    },
    acknowledgementMode = jms:CLIENT_ACKNOWLEDGE,
    consumerOptions = {
        'type: jms:DURABLE,
        destination: {
            'type: jms:TOPIC,
            name: "MARKET.DATA.PRICES"
        },
        subscriberName: subscriberName,
        messageSelector: instrumentClassSelector
    }
);

final jms:Connection jmsPublishConnection = check new (
    initialContextFactory = "org.apache.activemq.jndi.ActiveMQInitialContextFactory",
    providerUrl = providerUrl
);

// Session used to republish normalised ticks and spread alerts. Auto-acknowledge is sufficient
// here since these are outbound publishes rather than consumed messages.
final jms:Session jmsPublishSession = check createJmsPublishSession(jmsPublishConnection);

function createJmsPublishSession(jms:Connection connection) returns jms:Session|error {
    return connection->createSession(jms:AUTO_ACKNOWLEDGE);
}

final jms:MessageProducer normalisedTickProducer = check jmsPublishSession.createProducer({
    'type: jms:TOPIC,
    name: "MARKET.DATA.NORMALISED"
});

final jms:MessageProducer spreadAlertProducer = check jmsPublishSession.createProducer({
    'type: jms:TOPIC,
    name: "MARKET.DATA.ALERTS"
});
