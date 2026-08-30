// Kafka broker connection details.
configurable string kafkaBootstrapServers = ?;

// Unique transactional ID for the exactly-once producer. Must be stable across
// restarts of the same logical producer instance so Kafka can fence zombies.
configurable string paymentSettlementTransactionalId = ?;

// Time-to-live, in seconds, for entries in the duplicate-suppression cache keyed by paymentId.
configurable decimal duplicatePaymentTtlSeconds = 3600;

// HTTP listener port for the reconciliation API.
configurable int reconciliationServicePort = 9090;
