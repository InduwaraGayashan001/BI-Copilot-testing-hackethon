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
