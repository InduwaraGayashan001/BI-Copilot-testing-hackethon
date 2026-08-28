import ballerina/time;
import ballerinax/solace;

# In-memory audit trail of processed payment instructions.
final table<AuditEntry> key(instructionId) auditEntries = table [];

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
