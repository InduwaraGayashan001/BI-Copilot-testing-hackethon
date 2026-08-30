import ballerinax/kafka;

const string TELEMETRY_RAW_TOPIC = "iot.telemetry.raw";
const string TELEMETRY_INGESTION_GROUP = "telemetry-ingestion";

// Consumer configuration bound to the Confluent Schema Registry so incoming
// Avro-encoded records are decoded directly into `TelemetryReading` values.
// `concurrentConsumers` runs 4 consumer threads within the same group,
// spreading the subscribed topic's partitions across them.
kafka:ConsumerConfiguration telemetryConsumerConfiguration = {
    groupId: TELEMETRY_INGESTION_GROUP,
    topics: [TELEMETRY_RAW_TOPIC],
    offsetReset: "earliest",
    autoCommit: false,
    pollingInterval: 1,
    concurrentConsumers: 4,
    valueDeserializerType: kafka:DES_AVRO,
    schemaRegistryUrl: schemaRegistryUrl
};

listener kafka:Listener telemetryIngestionListener = new (kafkaBootstrapServers, telemetryConsumerConfiguration);
