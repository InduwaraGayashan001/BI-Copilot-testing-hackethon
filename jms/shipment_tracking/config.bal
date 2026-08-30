// ActiveMQ Artemis broker connection URL.
configurable string providerUrl = "tcp://localhost:61616";

// HTTP listener port for the reconciliation control endpoint.
configurable int servicePort = 8080;

// Queue that the legacy system publishes fixed-width shipment status events to.
configurable string shipmentStatusInQueue = "SHIPMENT.STATUS.IN";

// Queue that messages which fail fixed-width parsing are routed to.
configurable string shipmentStatusInvalidQueue = "SHIPMENT.STATUS.INVALID";

// Default queue that accepted (non-exception) shipment status events are published to when the
// carrier is not present in carrierStatusOutQueues.
configurable string defaultShipmentStatusOutQueue = "SHIPMENT.STATUS.OUT";

// Default queue that exception shipment status events are published to when the carrier is not
// present in carrierExceptionQueues.
configurable string defaultShipmentExceptionQueue = "SHIPMENT.EXCEPTIONS";

// Per-carrier destination queues for accepted shipment status events, keyed by carrierCode.
// Carriers not present here fall back to defaultShipmentStatusOutQueue.
configurable map<string> carrierStatusOutQueues = {};

// Per-carrier destination queues for exception shipment status events, keyed by carrierCode.
// Carriers not present here fall back to defaultShipmentExceptionQueue.
configurable map<string> carrierExceptionQueues = {};

// JMS priority (0-9) used for exception events, higher than normal accepted events so they are
// dispatched ahead of routine status updates.
configurable int exceptionPriority = 7;

// Time-to-live, in milliseconds, applied to exception events so they expire instead of being
// processed indefinitely if left unconsumed.
configurable int exceptionTtlMillis = 86400000;

// Queue that the legacy system replays shipment status events to for nightly reconciliation.
configurable string shipmentStatusReplayQueue = "SHIPMENT.STATUS.REPLAY";

// Queue that replay messages exceeding maxProcessingAttempts are routed to instead of being
// retried further.
configurable string shipmentStatusDlqQueue = "SHIPMENT.STATUS.DLQ";

// Number of messages drained from SHIPMENT.STATUS.REPLAY per transacted batch during
// reconciliation, with one commit per batch.
configurable int reconcileBatchSize = 100;

// Maximum number of processing attempts for a single replay message before it is treated as a
// poison message and routed to shipmentStatusDlqQueue.
configurable int maxProcessingAttempts = 5;
