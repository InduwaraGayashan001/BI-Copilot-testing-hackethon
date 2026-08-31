import ballerinax/nats;

configurable string natsUrl = "nats://localhost:4222";
configurable string connectionName = "telemetry-collector-service";

// Token used to authenticate with the NATS server, replacing basic username/password auth.
configurable string natsAuthToken = ?;

// TLS truststore used to secure the connection to the NATS server.
configurable string tlsTrustStorePath = ?;
configurable string tlsTrustStorePassword = ?;

// Readings whose readingAt timestamp is older than this many seconds, relative to the
// time they are consumed, are considered stale - dropped (not processed) and counted
// rather than raising a processing error.
configurable decimal stalenessWindowSeconds = 300;

// Caps how many messages/bytes the telemetry subscription will buffer while awaiting
// processing, across every device subject under telemetry.>.
configurable int subscriptionMaxPendingMessages = 10000;
configurable int subscriptionMaxPendingBytes = 10485760;

final nats:PendingLimits telemetryPendingLimits = {
    maxMessages: subscriptionMaxPendingMessages,
    maxBytes: subscriptionMaxPendingBytes
};

// Per-device-type alert thresholds. When a reading's metric value crosses (exceeds) the
// threshold configured for its device type, an alert is published to telemetry.alerts.
// Device types without a configured threshold are never alerted on.
configurable map<decimal> deviceTypeAlertThresholds = {
    fridge: 8.0,
    freezer: -12.0
};

// Configuration for the TELEMETRY_HOT JetStream stream, created or loaded at startup.
// This is a memory-backed, capped-size stream that retains only the most recent readings.
configurable string telemetryHotStreamName = "TELEMETRY_HOT";
configurable string telemetryHotStreamSubjects = "telemetry.>";
configurable nats:RetentionPolicy telemetryHotRetentionPolicy = nats:LIMITS;
configurable nats:StorageType telemetryHotStorageType = nats:MEMORY;
configurable float telemetryHotMaxMsgs = 50000;
configurable nats:DiscardPolicy telemetryHotDiscardPolicy = nats:OLD;

final nats:StreamConfiguration telemetryHotStreamConfig = {
    name: telemetryHotStreamName,
    subjects: telemetryHotStreamSubjects,
    retentionPolicy: telemetryHotRetentionPolicy,
    storageType: telemetryHotStorageType,
    maxMsgs: telemetryHotMaxMsgs,
    discardPolicy: telemetryHotDiscardPolicy
};

