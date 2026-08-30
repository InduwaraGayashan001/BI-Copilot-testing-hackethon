import ballerinax/kafka;

const string TELEMETRY_RAW_TOPIC = "iot.telemetry.raw";
const string TELEMETRY_AGGREGATED_TOPIC = "iot.telemetry.aggregated";
const string TELEMETRY_ALERTS_TOPIC = "iot.alerts";
const string TELEMETRY_INGESTION_GROUP = "telemetry-ingestion";

// Raw consumer (rather than a `kafka:Listener`) so the ingestion loop can call
// `pause`/`resume` on the exact same consumer instance that is polling, which
// is required to implement partition-level backpressure. `concurrentConsumers`
// runs 4 consumer threads within the same group, spreading the subscribed
// topic's partitions across them. Avro-encoded records are decoded directly
// into `TelemetryReading` values via the Confluent Schema Registry.
final kafka:Consumer telemetryConsumer = check new (kafkaBootstrapServers, {
    groupId: TELEMETRY_INGESTION_GROUP,
    topics: [TELEMETRY_RAW_TOPIC],
    offsetReset: "earliest",
    autoCommit: false,
    pollingInterval: 1,
    concurrentConsumers: 4,
    valueDeserializerType: kafka:DES_AVRO,
    schemaRegistryUrl: schemaRegistryUrl
});

// Producer used to publish both the per-window aggregates and the threshold
// alerts derived from them.
final kafka:Producer telemetryProducer = check new (kafkaBootstrapServers, {
    clientId: "telemetry-aggregation-producer",
    acks: "all",
    enableIdempotence: true
});

// Aggregates readings per device and metric over the configured tumbling
// window.
final WindowAggregator windowAggregator = new ();

// Tracks whether alert publishing is currently failing, driving the
// pause/resume backpressure decisions applied to the telemetry consumer.
final AlertPublishHealth alertPublishHealth = new ();
