// ActiveMQ Artemis broker connection URL.
configurable string providerUrl = "tcp://localhost:61616";

// JMS client identifier used to establish the durable topic subscription. Must remain the same
// across restarts so the broker can identify and resume this subscription.
configurable string clientId = ?;

// Durable subscriber name used together with the client id to uniquely identify the durable
// subscription on the broker.
configurable string subscriberName = ?;

// Instrument classes this subscriber is interested in. Used to build a JMS message selector so
// only ticks for these instrument classes are delivered to this durable subscription.
configurable string[] instrumentClasses = ["EQUITY", "FX", "FIXED_INCOME"];
