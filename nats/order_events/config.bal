import ballerinax/nats;

configurable string natsUrl = "nats://localhost:4222";
configurable string connectionName = "order-events-service";

// Configuration for the ORDERS JetStream stream, created or loaded at startup.
configurable string ordersStreamName = "ORDERS";
configurable string ordersStreamSubjects = "orders.>";
configurable nats:RetentionPolicy ordersRetentionPolicy = nats:WORKQUEUE;
configurable nats:StorageType ordersStorageType = nats:FILE;
configurable decimal ordersMaxAge = 86400;
configurable nats:DiscardPolicy ordersDiscardPolicy = nats:OLD;

final nats:StreamConfiguration ordersStreamConfig = {
    name: ordersStreamName,
    subjects: ordersStreamSubjects,
    retentionPolicy: ordersRetentionPolicy,
    storageType: ordersStorageType,
    maxAge: ordersMaxAge,
    discardPolicy: ordersDiscardPolicy
};

// Guards the destructive purge operation exposed over HTTP - disabled by default.
configurable boolean purgeEnabled = false;

// Timeout used when synchronously pulling messages from the stream during a replay.
configurable decimal replayConsumeTimeout = 5;

// Interval at which the in-progress signal is sent to the server while an order is
// still being processed, keeping the redelivery timer from expiring.
configurable decimal inProgressSignalInterval = 10;

// HTTP listener port for the replay/purge admin endpoints.
configurable int adminServicePort = 8080;
