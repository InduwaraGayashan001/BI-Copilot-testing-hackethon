// Kafka broker connection details.
configurable string kafkaBootstrapServers = ?;

// Unique transactional ID for the exactly-once producer. Must be stable across
// restarts of the same logical producer instance so Kafka can fence zombies.
configurable string paymentSettlementTransactionalId = ?;
