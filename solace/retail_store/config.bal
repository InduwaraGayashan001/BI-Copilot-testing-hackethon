// Solace PubSub+ broker connection configuration.
configurable string solaceBrokerUrl = ?;
configurable string solaceVpnName = ?;

// Basic authentication credentials for the broker connection.
configurable string solaceUsername = ?;
configurable string solacePassword = ?;

// Durable topic endpoint configuration for the store telemetry subscription.
// The topic uses the `*` single-level wildcard for the region, storeId and deviceType
// segments so that telemetry from every store and device is matched:
//   retail/telemetry/*/*/*
//   - segment 1 `*` matches any region (e.g. "us-east", "eu-west")
//   - segment 2 `*` matches any storeId (e.g. "store-042")
//   - segment 3 `*` matches any deviceType (e.g. "fridge", "pos", "hvac")
configurable string telemetryTopicName = "retail/telemetry/*/*/*";
configurable string telemetryEndpointName = "RETAIL.TELEMETRY.DTE";

// HTTP listener configuration.
configurable int servicePort = 8092;

// Nightly batch drain configuration: the durable queue is drained with a blocking receive per
// message, using this timeout to wait for the next message before falling back to a
// non-blocking receive to confirm the queue is actually empty.
configurable string batchQueueName = "RETAIL.TELEMETRY.BATCH";
configurable decimal batchReceiveTimeout = 5.0;

// Alerting configuration: published with DIRECT (at-most-once) delivery, a short time-to-live
// and top priority so that alerts are not queued behind routine telemetry traffic.
configurable decimal alertTimeToLive = 30.0;
configurable int alertPriority = 9;

// Per-device-type alert thresholds. A reading whose metric value exceeds the threshold
// configured for its deviceType triggers an alert.
configurable map<decimal> deviceTypeThresholds = {
    "fridge": 8.0,
    "hvac": 30.0,
    "pos": 100.0
};

// Compression level (0-9) applied to the connections to the broker. 0 disables compression
// (the default); higher values trade additional CPU time spent compressing/decompressing each
// message for reduced bytes transferred over the wire - see connections.bal for details.
configurable int solaceCompressionLevel = 1;

// Bounded in-memory buffer configuration for telemetry readings awaiting downstream processing.
// When the buffer is full, the oldest buffered reading is shed to make room for the newest one
// (shed-oldest backpressure) rather than blocking or rejecting the newest reading.
configurable int telemetryBufferCapacity = 100;

