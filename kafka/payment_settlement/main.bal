import ballerina/log;
import ballerinax/kafka;

service kafka:Service on paymentAuthorizedListener {

    remote function onConsumerRecord(kafka:Caller caller, PaymentAuthorizedConsumerRecord[] records) returns error? {
        foreach PaymentAuthorizedConsumerRecord paymentAuthorizedRecord in records {
            error? settleResult = settlePaymentInTransaction(caller, paymentAuthorizedRecord);
            if settleResult is error {
                log:printError("Failed to settle payment, aborting transaction",
                        'error = settleResult, paymentId = paymentAuthorizedRecord.value.paymentId);
                return settleResult;
            }
        }
        log:printInfo("Successfully processed batch", batchSize = records.length());
    }

    remote function onError(kafka:Error err) returns error? {
        log:printError("Error while consuming payment authorized events", 'error = err);
    }
}

// Publishes the settlement event and commits the consumer offset for the
// processed record inside a single Ballerina transaction, giving exactly-once
// semantics: either both the publish and the offset commit succeed, or the
// transaction is aborted and neither is visible to read-committed consumers.
// Records whose paymentId was already settled within the TTL window are
// skipped entirely (including the offset commit, which still proceeds so the
// duplicate is not redelivered forever).
function settlePaymentInTransaction(kafka:Caller caller, PaymentAuthorizedConsumerRecord paymentAuthorizedRecord)
        returns error? {
    PaymentAuthorized paymentAuthorized = paymentAuthorizedRecord.value;

    boolean shouldSettle = markProcessedIfAbsent(paymentAuthorized.paymentId);
    if !shouldSettle {
        log:printInfo("Skipping duplicate payment authorized event", paymentId = paymentAuthorized.paymentId);
        check caller->commit();
        return;
    }

    PaymentSettlement paymentSettlement = toPaymentSettlement(paymentAuthorized);

    transaction {
        check paymentSettlementProducer->send({
            topic: PAYMENTS_SETTLEMENT_TOPIC,
            key: paymentSettlement.paymentId.toBytes(),
            value: paymentSettlement.toJson().toJsonString().toBytes()
        });
        check caller->commit();
        check commit;
    }
    return;
}
