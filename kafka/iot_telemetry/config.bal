// Kafka broker connection details.
configurable string kafkaBootstrapServers = ?;

// Confluent Schema Registry URL used to resolve Avro schemas for the
// `iot.telemetry.raw` topic.
configurable string schemaRegistryUrl = ?;

// Size of the tumbling aggregation window, in seconds. Readings are grouped
// per device and metric, and each window is flushed on this fixed interval.
configurable decimal windowSizeSeconds = 60;

// Per-metric mean thresholds, as a comma-separated list of "metric=threshold"
// pairs, e.g. "temperature=80.0,humidity=90.0". When a window's mean for a
// metric crosses (strictly exceeds) its configured threshold, an alert is
// published to `iot.alerts`. Metrics without a configured threshold are never
// alerted on.
configurable string metricAlertThresholds = "";
