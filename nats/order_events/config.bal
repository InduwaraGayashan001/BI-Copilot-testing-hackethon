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
