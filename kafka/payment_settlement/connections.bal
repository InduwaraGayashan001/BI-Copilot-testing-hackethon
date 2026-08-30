import ballerinax/kafka;

const string PAYMENTS_AUTHORIZED_TOPIC = "payments.authorized";
const string PAYMENTS_SETTLEMENT_TOPIC = "payments.settlement";
const string PAYMENT_SETTLEMENT_GROUP = "payment-settlement-service";

// Transactional producer configured for exactly-once semantics: idempotence
// on, acks=all, and a stable transactional ID so records published within a
// transaction are only visible to read-committed consumers once committed.
final kafka:Producer paymentSettlementProducer = check new (kafkaBootstrapServers, {
    clientId: "payment-settlement-producer",
    acks: "all",
    enableIdempotence: true,
    transactionalId: paymentSettlementTransactionalId
});

kafka:ConsumerConfiguration paymentAuthorizedConsumerConfiguration = {
    groupId: PAYMENT_SETTLEMENT_GROUP,
    topics: [PAYMENTS_AUTHORIZED_TOPIC],
    offsetReset: "earliest",
    autoCommit: false,
    isolationLevel: "read_committed",
    pollingInterval: 1
};

listener kafka:Listener paymentAuthorizedListener = new (kafkaBootstrapServers, paymentAuthorizedConsumerConfiguration);

// Synchronous consumer used only for reconciliation (health snapshot and manual
// replay). It is never subscribed via the consumer group; partitions are
// assigned manually so it does not interfere with the group's partition balance.
final kafka:Consumer reconciliationConsumer = check new (kafkaBootstrapServers, {
    clientId: "payment-settlement-reconciliation",
    isolationLevel: "read_committed",
    autoCommit: false
});

// Duplicate-suppression cache: paymentId -> expiry epoch seconds. Guarded by a
// lock since it is read-modify-written from concurrent listener dispatches.
isolated map<int> processedPaymentIds = {};
