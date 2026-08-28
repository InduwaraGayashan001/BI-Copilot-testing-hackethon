import ballerina/http;
import ballerinax/solace;

# Represents a single device telemetry reading published by a store device onto the
# `retail/telemetry/{region}/{storeId}/{deviceType}` topic hierarchy.
#
# + storeId - Unique identifier of the store the reading originated from
# + region - Region the originating store belongs to
# + deviceType - Category of the device that produced the reading
# + deviceId - Unique identifier of the device that produced the reading
# + metric - Name of the metric being reported
# + value - Measured value of the metric
# + unit - Unit of measure for the reported value
# + readingAt - Timestamp at which the reading was taken
public type DeviceTelemetry record {|
    string storeId;
    string region;
    string deviceType;
    string deviceId;
    string metric;
    decimal value;
    string unit;
    string readingAt;
|};

# Represents a device telemetry message consumed from the durable topic endpoint, narrowed so
# that the `DeviceTelemetry` payload is data-bound directly instead of being delivered as raw
# `anydata`. The `receiveTimestamp` and `expiration` fields are populated because the listener is
# configured with `generateReceiveTimestamps` and `calculateMessageExpiration` enabled.
#
# + payload - The device telemetry reading carried by the message
public type DeviceTelemetryMessage record {|
    *solace:Message;
    DeviceTelemetry payload;
|};

# Represents the outcome of draining the nightly batch queue `RETAIL.TELEMETRY.BATCH`.
#
# + drainedCount - Number of readings processed and acknowledged
# + skippedExpiredCount - Number of readings dropped because their expiration had already passed
public type DrainResult record {|
    int drainedCount;
    int skippedExpiredCount;
|};

# Represents the response returned when the nightly batch drain completes.
public type DrainCompleted record {|
    *http:Ok;
    DrainResult body;
|};

# Represents an error detail payload.
#
# + message - Human readable error description
public type ErrorDetail record {|
    string message;
|};

# Represents the response returned when the batch drain fails.
public type DrainError record {|
    *http:InternalServerError;
    ErrorDetail body;
|};

# Represents a compact correlation payload packed into a message's `userData` field, kept within
# Solace's 36-byte limit for that field: a 16-byte (UTF-8, truncated/padded) storeId, an 8-byte
# epoch-second timestamp and a 1-byte severity flag - 25 bytes in total.
#
# + storeId - Identifier of the store the alert originated from, fixed to 16 bytes
# + triggeredAt - Epoch second at which the threshold crossing was detected
# + severity - Severity flag for the alert (1 = threshold crossed)
public type AlertCorrelation record {|
    string storeId;
    int triggeredAt;
    int severity;
|};

# Represents a device telemetry alert published when a metric crosses its per-device-type
# threshold.
#
# + storeId - Unique identifier of the store the alert originated from
# + region - Region the originating store belongs to
# + deviceType - Category of the device that produced the reading
# + deviceId - Unique identifier of the device that produced the reading
# + metric - Name of the metric that crossed the threshold
# + value - Measured value that crossed the threshold
# + threshold - Configured threshold for the device type
# + unit - Unit of measure for the reported value
public type DeviceTelemetryAlert record {|
    string storeId;
    string region;
    string deviceType;
    string deviceId;
    string metric;
    decimal value;
    decimal threshold;
    string unit;
|};

# Represents the current state of the bounded telemetry buffer and processing counters.
#
# + bufferedCount - Number of readings currently held in the bounded buffer
# + bufferCapacity - Maximum number of readings the buffer can hold
# + shedCount - Number of readings discarded because the buffer was full when they arrived
# + processedCount - Number of readings processed from the durable topic endpoint subscription
# + drainedCount - Number of readings processed via the nightly batch drain
# + skippedExpiredCount - Number of readings dropped (from either source) because their
# expiration had already passed
public type TelemetryHealth record {|
    int bufferedCount;
    int bufferCapacity;
    int shedCount;
    int processedCount;
    int drainedCount;
    int skippedExpiredCount;
|};

# Holds the mutable, in-memory telemetry processing state: the bounded, shed-oldest buffer of
# readings awaiting downstream processing, and the counters surfaced on `GET /telemetry/health`.
# Grouped into a single record so that all fields can be accessed together within one `lock`
# statement.
#
# + telemetryBuffer - Bounded buffer of device telemetry readings awaiting downstream processing
# + shedCount - Number of readings discarded because the buffer was full when they arrived
# + processedCount - Number of readings processed from the durable topic endpoint subscription
# + drainedCount - Number of readings processed via the nightly batch drain
# + skippedExpiredCount - Number of readings dropped (from either source) because their
# expiration had already passed
public type TelemetryState record {|
    DeviceTelemetry[] telemetryBuffer;
    int shedCount;
    int processedCount;
    int drainedCount;
    int skippedExpiredCount;
|};

# Represents the response returned for a telemetry health check.
public type TelemetryHealthOk record {|
    *http:Ok;
    TelemetryHealth body;
|};

