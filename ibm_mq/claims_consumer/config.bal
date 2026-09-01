// IBM MQ queue manager connection configurations.
configurable string queueManagerName = ?;
configurable string host = ?;
configurable int port = ?;
configurable string channel = ?;
configurable string userID = ?;
configurable string password = ?;

// Inbound queue on which claim submissions are received.
configurable string claimsInboundQueueName = "CLAIMS.INBOUND";
