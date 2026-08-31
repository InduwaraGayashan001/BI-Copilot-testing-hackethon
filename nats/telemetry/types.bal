import ballerina/constraint;

// Represents a device telemetry reading published to a NATS subject of the form
// telemetry.{region}.{siteId}.{deviceType}, e.g. telemetry.eu-west.store-42.fridge
public type DeviceReading record {|
    @constraint:String {minLength: 1}
    string region;
    @constraint:String {minLength: 1}
    string siteId;
    @constraint:String {minLength: 1}
    string deviceType;
    @constraint:String {minLength: 1}
    string metric;
    decimal value;
    // ISO-8601 timestamp string indicating when the reading was taken at the device.
    @constraint:String {minLength: 1}
    string readingAt;
|};

