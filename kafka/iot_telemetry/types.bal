import ballerinax/kafka;

// Represents a device telemetry reading decoded from an Avro-encoded message
// on the `iot.telemetry.raw` Kafka topic.
public type TelemetryReading record {|
    string deviceId;
    string siteId;
    string metric;
    decimal value;
    string unit;
    string readingAt;
|};

// Represents a Kafka consumer record whose value is bound to the
// `TelemetryReading` type after Avro deserialization via the schema registry.
public type TelemetryReadingConsumerRecord record {|
    *kafka:AnydataConsumerRecord;
    TelemetryReading value;
|};
