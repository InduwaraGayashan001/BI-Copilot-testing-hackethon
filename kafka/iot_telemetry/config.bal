// Kafka broker connection details.
configurable string kafkaBootstrapServers = ?;

// Confluent Schema Registry URL used to resolve Avro schemas for the
// `iot.telemetry.raw` topic.
configurable string schemaRegistryUrl = ?;
