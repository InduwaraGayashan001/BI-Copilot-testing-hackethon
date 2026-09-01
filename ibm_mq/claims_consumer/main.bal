import ballerina/log;
import ballerinax/ibm.ibmmq;

@ibmmq:ServiceConfig {
    queueName: claimsInboundQueueName,
    sessionAckMode: ibmmq:SESSION_TRANSACTED
}
service ibmmq:Service on claimsInboundListener {

    # Handles an incoming claim submission message on a transacted session.
    # Messages that have exceeded the maximum delivery attempts are treated
    # as poison messages: they are routed to CLAIMS.DLQ with the failure
    # reason and attempt count as properties, and the transaction is
    # committed so the claim leaves the input queue. Otherwise, the
    # transaction is committed through the caller only after the claim has
    # been processed, the audit entry has been written, and the accepted
    # claim has been published to CLAIMS.ACCEPTED; any failure rolls back
    # the transaction so the claim is redelivered.
    #
    # + message - the received IBM MQ message
    # + caller - the caller used to commit or roll back the transaction
    # + return - an error if the commit or rollback itself fails
    remote function onMessage(ibmmq:Message message, ibmmq:Caller caller) returns error? {
        int deliveryCount = recordDeliveryAttempt(message);

        ClaimSubmission|error claimSubmission = mapToClaimSubmission(message);
        if claimSubmission is error {
            return handleFailedClaim(message, deliveryCount, "Failed to bind the claim submission payload: "
                    + claimSubmission.message(), caller);
        }

        error? processResult = processClaimSubmission(claimSubmission);
        if processResult is error {
            return handleFailedClaim(message, deliveryCount, "Failed to process the claim submission: "
                    + processResult.message(), caller);
        }

        error? auditResult = writeAuditEntry(claimSubmission);
        if auditResult is error {
            return handleFailedClaim(message, deliveryCount, "Failed to write the audit entry: "
                    + auditResult.message(), caller);
        }

        ibmmq:Message claimAcceptedMessage = mapToClaimAcceptedMessage(claimSubmission);
        ibmmq:Error? publishResult = claimsAcceptedTopic->send(claimAcceptedMessage);
        if publishResult is ibmmq:Error {
            return handleFailedClaim(message, deliveryCount, "Failed to publish the accepted claim: "
                    + publishResult.message(), caller);
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

// Handles a claim that failed to be processed. Once the delivery count
// exceeds the configured maximum, the claim is considered a poison message:
// it is put on CLAIMS.DLQ with the failure reason and attempt count as
// properties, and the transaction is committed so it leaves CLAIMS.INBOUND.
// Otherwise, the transaction is rolled back so the claim is redelivered.
function handleFailedClaim(ibmmq:Message originalMessage, int deliveryCount, string failureReason,
        ibmmq:Caller caller) returns error? {
    log:printError(failureReason, deliveryCount = deliveryCount);

    if deliveryCount > maxDeliveryAttempts {
        ibmmq:Message deadLetterMessage = mapToDeadLetterMessage(originalMessage, failureReason, deliveryCount);
        ibmmq:Error? putResult = claimsDlq->put(deadLetterMessage);
        if putResult is ibmmq:Error {
            log:printError("Failed to put the claim on CLAIMS.DLQ", putResult);
            ibmmq:Error? rollbackResult = caller->'rollback();
            if rollbackResult is ibmmq:Error {
                log:printError("Failed to roll back the transaction", rollbackResult);
                return rollbackResult;
            }
            return;
        }

        ibmmq:Error? commitResult = caller->'commit();
        if commitResult is ibmmq:Error {
            log:printError("Failed to commit the transaction after routing the claim to CLAIMS.DLQ", commitResult);
            return commitResult;
        }

        clearDeliveryAttempts(originalMessage);
        log:printWarn("Claim routed to CLAIMS.DLQ after exceeding the maximum delivery attempts",
                deliveryCount = deliveryCount, failureReason = failureReason);
        return;
    }

    ibmmq:Error? rollbackResult = caller->'rollback();
    if rollbackResult is ibmmq:Error {
        log:printError("Failed to roll back the transaction", rollbackResult);
        return rollbackResult;
    }
}
