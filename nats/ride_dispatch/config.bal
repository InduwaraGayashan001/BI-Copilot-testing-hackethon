import ballerinax/nats;

configurable string natsUrl = "nats://localhost:4222";
configurable string connectionName = "ride-dispatch-service";
configurable string queueGroupName = "ride-dispatch-workers";

// Reconnect retry configuration for the NATS connection.
configurable int maxReconnect = 60;
configurable decimal reconnectWait = 2;
configurable decimal connectionTimeout = 2;

final nats:RetryConfig natsRetryConfig = {
    maxReconnect: maxReconnect,
    reconnectWait: reconnectWait,
    connectionTimeout: connectionTimeout
};

// Timeout (in seconds) to wait for a driver ETA reply on the request-reply call.
configurable decimal driverEtaTimeout = 5;

// Caps how many messages/bytes the dispatch subscription will buffer while awaiting processing.
configurable int subscriptionMaxPendingMessages = 1000;
configurable int subscriptionMaxPendingBytes = 1048576;

final nats:PendingLimits dispatchPendingLimits = {
    maxMessages: subscriptionMaxPendingMessages,
    maxBytes: subscriptionMaxPendingBytes
};
