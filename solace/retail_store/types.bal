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

