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
