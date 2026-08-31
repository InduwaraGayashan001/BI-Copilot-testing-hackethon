import ballerinax/nats;

configurable string natsUrl = "nats://localhost:4222";
configurable string connectionName = "telemetry-collector-service";

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

