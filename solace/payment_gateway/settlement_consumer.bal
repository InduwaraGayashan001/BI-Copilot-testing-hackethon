import ballerina/log;
import ballerinax/solace;

# Consumes payment instructions from the durable queue `PAYMENTS.INSTRUCTIONS.IN` within a
# transacted session, validates each instruction, and republishes it onto
# `PAYMENTS.SETTLEMENT.OUT` (or the dead letter queue for poison/invalid instructions). See
# `processSettlement` in functions.bal for how the consume and the outbound publish - each backed
# by its own transacted session in this connector - are coordinated to settle together.
@solace:ServiceConfig {
    queueName: paymentInstructionsQueueName
}
service on settlementListener {

    # Invoked for every payment instruction delivered from `PAYMENTS.INSTRUCTIONS.IN`.
    #
    # + message - The payment instruction message, with the payload data-bound into
    # `PaymentInstruction`
    # + caller - Handle used to commit or roll back the consumer's transaction
    # + return - A `solace:Error` if a commit/rollback operation itself fails
    remote function onMessage(PaymentInstructionMessage message, solace:Caller caller) returns solace:Error? {
        PaymentInstruction paymentInstruction = message.payload;

        solace:Error? result = processSettlement(message, caller);
        if result is solace:Error {
            log:printError("Failed to settle payment instruction, transaction rolled back",
                    instructionId = paymentInstruction.instructionId, 'error = result);
            return result;
        }

        log:printInfo("Payment instruction settlement processed", instructionId = paymentInstruction.instructionId);
    }

    # Invoked when a delivered message cannot be dispatched to `onMessage`, most commonly because
    # the underlying guaranteed consumer flow is disrupted.
    #
    # + err - The failure that prevented dispatch
    remote function onError(solace:Error err) returns solace:Error? {
        if err is solace:FlowDownError {
            log:printError("Settlement consumer flow is down; the underlying connection was lost " +
                    "and the flow will be re-established once connectivity is restored", 'error = err);
            return;
        }

        if err is solace:InactiveFlowError {
            log:printWarn("Settlement consumer flow is inactive; another instance in the client " +
                    "cluster is likely active and this instance will remain on standby", 'error = err);
            return;
        }

        log:printError("Unexpected error while consuming payment instructions", 'error = err);
    }
}
