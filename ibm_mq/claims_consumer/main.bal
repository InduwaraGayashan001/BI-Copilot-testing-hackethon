import ballerina/log;
import ballerinax/ibm.ibmmq;

@ibmmq:ServiceConfig {
    queueName: claimsInboundQueueName,
    sessionAckMode: ibmmq:SESSION_TRANSACTED
}
service ibmmq:Service on claimsInboundListener {

    # Handles an incoming claim submission message on a transacted session.
    # The transaction is committed through the caller only after the claim
    # has been processed and the audit entry has been written; any failure
    # rolls back the transaction so the claim is redelivered.
    #
    # + message - the received IBM MQ message
    # + caller - the caller used to commit or roll back the transaction
    # + return - an error if the commit or rollback itself fails
    remote function onMessage(ibmmq:Message message, ibmmq:Caller caller) returns error? {
        ClaimSubmission|error claimSubmission = mapToClaimSubmission(message);
        if claimSubmission is error {
            log:printError("Failed to bind the claim submission payload", claimSubmission);
            ibmmq:Error? rollbackResult = caller->'rollback();
            if rollbackResult is ibmmq:Error {
                log:printError("Failed to roll back the transaction", rollbackResult);
                return rollbackResult;
            }
            return;
        }

        error? processResult = processClaimSubmission(claimSubmission);
        if processResult is error {
            log:printError("Failed to process the claim submission", processResult,
                    claimId = claimSubmission.claimId);
            ibmmq:Error? rollbackResult = caller->'rollback();
            if rollbackResult is ibmmq:Error {
                log:printError("Failed to roll back the transaction", rollbackResult);
                return rollbackResult;
            }
            return;
        }

        error? auditResult = writeAuditEntry(claimSubmission);
        if auditResult is error {
            log:printError("Failed to write the audit entry for the claim submission", auditResult,
                    claimId = claimSubmission.claimId);
            ibmmq:Error? rollbackResult = caller->'rollback();
            if rollbackResult is ibmmq:Error {
                log:printError("Failed to roll back the transaction", rollbackResult);
                return rollbackResult;
            }
            return;
        }

        ibmmq:Error? commitResult = caller->'commit();
        if commitResult is ibmmq:Error {
            log:printError("Failed to commit the transaction", commitResult,
                    claimId = claimSubmission.claimId);
            return commitResult;
        }

        log:printInfo("Claim submission committed", claimId = claimSubmission.claimId);
    }

    # Handles runtime errors that occur while receiving or dispatching
    # messages from CLAIMS.INBOUND.
    #
    # + mqError - the error encountered by the listener
    remote function onError(ibmmq:Error mqError) returns error? {
        log:printError("Error while receiving claim submission from IBM MQ", mqError);
    }
}
