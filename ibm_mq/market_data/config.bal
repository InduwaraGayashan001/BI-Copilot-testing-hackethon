// IBM MQ queue manager connection configurations.
configurable string queueManagerName = ?;
configurable string host = ?;
configurable int port = ?;
configurable string channel = ?;
configurable string userID = ?;
configurable string password = ?;

// TLS truststore holding the certificate(s) trusted for the IBM MQ server's
// TLS handshake.
configurable string truststorePath = ?;
configurable string truststorePassword = ?;

// SSL/TLS cipher suite pinned for the connection. Defaults to a modern
// TLS 1.3 cipher suite. See connections.bal for the alternatives considered.
configurable string sslCipherSuite = "TLS_AES_256_GCM_SHA384";

// Topic on which market data price ticks are published.
configurable string marketDataTopicName = "MARKET.DATA.PRICES";

// Durable subscription name used so the queue manager retains ticks
// published while this subscriber is offline.
configurable string subscriberName = ?;

// List of instrument classes this subscriber is interested in. Used to
// build a JMS message selector so the queue manager filters ticks before
// delivery.
configurable string[] instrumentClasses = ?;

// Interval, in seconds, the consumer waits between successive poll
// attempts when no message is immediately available.
configurable decimal pollingInterval = 5;

// Maximum time, in seconds, to wait for a message on each poll before
// giving up and retrying.
configurable decimal receiveTimeout = 10;

