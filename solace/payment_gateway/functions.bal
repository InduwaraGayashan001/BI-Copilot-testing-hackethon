import ballerina/time;
import ballerinax/solace;

# In-memory audit trail of processed payment instructions.
final table<AuditEntry> key(instructionId) auditEntries = table [];

# In-memory set of sequence numbers already settled, used to suppress duplicate processing of a
# payment instruction that is redelivered (for example, after a rollback of the settlement
# producer's transaction).
final map<boolean> settledSequenceNumbers = {};

# Builds an audit entry for a payment instruction, timestamped with the current UTC time.
#
# + paymentInstruction - The payment instruction to build the audit entry for
# + return - The audit entry to be recorded
function buildAuditEntry(PaymentInstruction paymentInstruction) returns AuditEntry => {
    instructionId: paymentInstruction.instructionId,
    debtorIban: paymentInstruction.debtorIban,
    creditorIban: paymentInstruction.creditorIban,
    amount: paymentInstruction.amount,
    currency: paymentInstruction.currency,
    executionDate: paymentInstruction.executionDate,
    paymentScheme: paymentInstruction.paymentScheme,
    recordedTime: time:utcToString(time:utcNow())
};

# Records the audit entry for a payment instruction in the in-memory audit store.
#
# + auditEntry - The audit entry to record
function writeAuditEntry(AuditEntry auditEntry) {
    auditEntries.add(auditEntry);
}

# Publishes a payment instruction onto the guaranteed `PAYMENTS.INSTRUCTIONS.IN` queue and records
# its audit entry within a single transacted producer session, committing only if both the publish
# and the audit write succeed, and rolling back otherwise.
#
# + paymentInstruction - The payment instruction to process
# + return - `()` if both the publish and the audit write succeeded and were committed, or a
# `solace:Error` if either step failed and the transaction was rolled back
function processPaymentInstruction(PaymentInstruction paymentInstruction) returns solace:Error? {
    solace:Message message = {
        payload: paymentInstruction,
        deliveryMode: solace:PERSISTENT,
        correlationId: paymentInstruction.instructionId
    };

    solace:Error? sendResult = paymentInstructionProducer->send(message, {queueName: paymentInstructionsQueueName});
    if sendResult is solace:Error {
        check paymentInstructionProducer->'rollback();
        return sendResult;
    }

    AuditEntry auditEntry = buildAuditEntry(paymentInstruction);
    writeAuditEntry(auditEntry);

    solace:Error? commitResult = paymentInstructionProducer->'commit();
    if commitResult is solace:Error {
        check paymentInstructionProducer->'rollback();
        AuditEntry|() removedEntry = auditEntries.removeIfHasKey(paymentInstruction.instructionId);
        return commitResult;
    }
}

# Validates a payment instruction consumed from `PAYMENTS.INSTRUCTIONS.IN`.
#
# + paymentInstruction - The payment instruction to validate
# + return - A `ValidationError` describing the first validation failure found, or `()` if the
# instruction is valid
function validatePaymentInstruction(PaymentInstruction paymentInstruction) returns ValidationError? {
    if paymentInstruction.debtorIban.trim().length() == 0 {
        return error ValidationError("debtorIban must not be empty");
    }
    if paymentInstruction.creditorIban.trim().length() == 0 {
        return error ValidationError("creditorIban must not be empty");
    }
    if paymentInstruction.amount <= 0d {
        return error ValidationError("amount must be greater than zero");
    }
    if paymentInstruction.currency.trim().length() == 0 {
        return error ValidationError("currency must not be empty");
    }
    if paymentInstruction.paymentScheme.trim().length() == 0 {
        return error ValidationError("paymentScheme must not be empty");
    }
}

# Builds the settled payment instruction to be published onto `PAYMENTS.SETTLEMENT.OUT`.
#
# + paymentInstruction - The payment instruction that was validated
# + return - The settled payment instruction
function buildSettledPaymentInstruction(PaymentInstruction paymentInstruction) returns SettledPaymentInstruction => {
    instructionId: paymentInstruction.instructionId,
    debtorIban: paymentInstruction.debtorIban,
    creditorIban: paymentInstruction.creditorIban,
    amount: paymentInstruction.amount,
    currency: paymentInstruction.currency,
    executionDate: paymentInstruction.executionDate,
    paymentScheme: paymentInstruction.paymentScheme
};

# Checks whether a payment instruction message has already been settled, based on its broker
# assigned sequence number.
#
# + message - The payment instruction message received from `PAYMENTS.INSTRUCTIONS.IN`
# + return - `true` if a message with the same sequence number has already been settled
function isDuplicateMessage(PaymentInstructionMessage message) returns boolean {
    int? sequenceNumber = message?.sequenceNumber;
    if sequenceNumber is () {
        return false;
    }
    return settledSequenceNumbers.hasKey(sequenceNumber.toString());
}

# Records a payment instruction message's sequence number as settled, so a subsequent redelivery
# of the same message is recognized and suppressed.
#
# + message - The payment instruction message that was settled
function markMessageSettled(PaymentInstructionMessage message) {
    int? sequenceNumber = message?.sequenceNumber;
    if sequenceNumber is int {
        settledSequenceNumbers[sequenceNumber.toString()] = true;
    }
}

# Determines whether a payment instruction message has exceeded the configured maximum delivery
# count and should be treated as poison, routed to the dead letter queue instead of being retried
# again.
#
# + message - The payment instruction message received from `PAYMENTS.INSTRUCTIONS.IN`
# + return - `true` if the message's delivery count has exceeded `maxDeliveryCount`
function isPoisonMessage(PaymentInstructionMessage message) returns boolean {
    int? deliveryCount = message?.deliveryCount;
    if deliveryCount is () {
        return false;
    }
    return deliveryCount > maxDeliveryCount;
}

# Publishes a message onto the dead letter queue within the settlement producer's transacted
# session.
#
# + paymentInstruction - The payment instruction to dead-letter
# + reason - Human readable description of why the instruction was dead-lettered
# + return - A `solace:Error` if the publish fails
function publishToDlq(PaymentInstruction paymentInstruction, string reason) returns solace:Error? {
    solace:Message dlqMessage = {
        payload: paymentInstruction,
        deliveryMode: solace:PERSISTENT,
        correlationId: paymentInstruction.instructionId,
        properties: {
            dlqReason: reason
        }
    };
    check settlementProducer->send(dlqMessage, {queueName: paymentInstructionsDlqName});
}

# Processes a payment instruction message consumed from `PAYMENTS.INSTRUCTIONS.IN`: suppresses
# duplicates, routes poison messages (those whose delivery count has exceeded `maxDeliveryCount`)
# and instructions that fail validation to the dead letter queue, and otherwise republishes a
# settled instruction onto `PAYMENTS.SETTLEMENT.OUT`.
#
# The settlement producer's transaction is committed first so the outbound publish (to the
# settlement queue or the DLQ) is made durable, and only then is the consumer's transaction
# committed via `caller->commit` to remove the source message from `PAYMENTS.INSTRUCTIONS.IN`. If
# the producer commit fails, both transactions are rolled back so the instruction is redelivered.
#
# + message - The payment instruction message received from `PAYMENTS.INSTRUCTIONS.IN`
# + caller - Handle used to commit or roll back the consumer's transaction
# + return - A `solace:Error` if a commit or rollback operation itself fails
function processSettlement(PaymentInstructionMessage message, solace:Caller caller) returns solace:Error? {
    PaymentInstruction paymentInstruction = message.payload;

    if isDuplicateMessage(message) {
        // Already settled on a prior delivery; discard this redelivery by committing the consume
        // without republishing again.
        check caller->'commit();
        return;
    }

    if isPoisonMessage(message) {
        solace:Error? dlqResult = publishToDlq(paymentInstruction,
                string `Exceeded maximum delivery count of ${maxDeliveryCount}`);
        return finalizeSettlement(dlqResult, message, caller);
    }

    ValidationError? validationResult = validatePaymentInstruction(paymentInstruction);
    if validationResult is ValidationError {
        solace:Error? dlqResult = publishToDlq(paymentInstruction, validationResult.message());
        return finalizeSettlement(dlqResult, message, caller);
    }

    SettledPaymentInstruction settledPaymentInstruction = buildSettledPaymentInstruction(paymentInstruction);
    solace:Message settlementMessage = {
        payload: settledPaymentInstruction,
        deliveryMode: solace:PERSISTENT,
        correlationId: paymentInstruction.instructionId
    };

    solace:Error? sendResult = settlementProducer->send(settlementMessage, {queueName: settlementOutQueueName});
    return finalizeSettlement(sendResult, message, caller);
}

# Commits or rolls back the settlement producer's transaction and the consumer's transaction
# together, based on the outcome of the producer-side publish (to the settlement queue or the
# DLQ).
#
# + publishResult - The outcome of the settlement producer's publish operation
# + message - The payment instruction message being settled
# + caller - Handle used to commit or roll back the consumer's transaction
# + return - A `solace:Error` if a commit or rollback operation itself fails
function finalizeSettlement(solace:Error? publishResult, PaymentInstructionMessage message, solace:Caller caller)
        returns solace:Error? {
    if publishResult is solace:Error {
        check settlementProducer->'rollback();
        check caller->'rollback();
        return publishResult;
    }

    solace:Error? producerCommitResult = settlementProducer->'commit();
    if producerCommitResult is solace:Error {
        check caller->'rollback();
        return producerCommitResult;
    }

    check caller->'commit();
    markMessageSettled(message);
}
