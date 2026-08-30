import ballerina/lang.value;
import ballerinax/rabbitmq;

# In-memory holding area for messages that have exhausted their retries and landed on
# the dead-letter queue. Populated by the `claims.dead-letter` consumer service below.
isolated map<DeadLetterMessage> deadLetterStore = {};

isolated function addDeadLetterMessage(DeadLetterMessage deadLetterMessage) {
    lock {
        deadLetterStore[deadLetterMessage.claimId] = deadLetterMessage.clone();
    }
}

isolated function listDeadLetterMessages() returns DeadLetterMessage[] {
    lock {
        return deadLetterStore.toArray().clone();
    }
}

isolated function removeDeadLetterMessage(string claimId) returns DeadLetterMessage? {
    lock {
        return deadLetterStore.removeIfHasKey(claimId).clone();
    }
}

isolated function clearDeadLetterMessages() {
    lock {
        deadLetterStore.removeAll();
    }
}

# Handles a claim message that failed processing: retries it (via the delayed retry queue)
# while under the retry cap, or nacks it without requeue so it dead-letters permanently.
#
# + claimSubmission - the claim submission that failed processing
# + routingKey - the routing key the message originally arrived with
# + properties - the message's basic properties, carrying the correlation ID and retry header
# + processingError - the error raised while processing the claim
# + caller - handle used to ack/nack the original delivery
function handleClaimFailure(ClaimSubmission claimSubmission, string routingKey,
        rabbitmq:BasicProperties? properties, error processingError, rabbitmq:Caller caller) returns error? {
    int retryCount = extractRetryCount(properties);

    if retryCount < maxRetryCount {
        int nextRetryCount = retryCount + 1;
        rabbitmq:BasicProperties retryProperties = {
            correlationId: claimSubmission.claimId,
            contentType: "application/json",
            headers: {[RETRY_COUNT_HEADER]: nextRetryCount}
        };
        rabbitmq:AnydataMessage retryMessage = {
            content: claimSubmission,
            routingKey: CLAIMS_RETRY_QUEUE,
            properties: retryProperties
        };
        check rabbitmqClient->publishMessage(retryMessage);
        check caller->basicAck();
    } else {
        DeadLetterMessage deadLetterMessage = {
            claimId: claimSubmission.claimId,
            routingKey,
            retryCount,
            failureReason: processingError.message(),
            claim: claimSubmission
        };
        addDeadLetterMessage(deadLetterMessage);
        check caller->basicNack(requeue = false);
    }
}

@rabbitmq:ServiceConfig {
    queueName: CLAIMS_AUTO_QUEUE,
    autoAck: false
}
service rabbitmq:Service on claimsQueueListener {
    remote function onMessage(rabbitmq:AnydataMessage message, rabbitmq:Caller caller) returns error? {
        ClaimSubmission claimSubmission = check value:ensureType(message.content);
        error? processingResult = processClaim(claimSubmission);
        if processingResult is error {
            check handleClaimFailure(claimSubmission, message.routingKey, message?.properties, processingResult, caller);
            return;
        }
        check caller->basicAck();
    }
}

@rabbitmq:ServiceConfig {
    queueName: CLAIMS_HEALTH_QUEUE,
    autoAck: false
}
service rabbitmq:Service on claimsQueueListener {
    remote function onMessage(rabbitmq:AnydataMessage message, rabbitmq:Caller caller) returns error? {
        ClaimSubmission claimSubmission = check value:ensureType(message.content);
        error? processingResult = processClaim(claimSubmission);
        if processingResult is error {
            check handleClaimFailure(claimSubmission, message.routingKey, message?.properties, processingResult, caller);
            return;
        }
        check caller->basicAck();
    }
}

@rabbitmq:ServiceConfig {
    queueName: CLAIMS_PROPERTY_QUEUE,
    autoAck: false
}
service rabbitmq:Service on claimsQueueListener {
    remote function onMessage(rabbitmq:AnydataMessage message, rabbitmq:Caller caller) returns error? {
        ClaimSubmission claimSubmission = check value:ensureType(message.content);
        error? processingResult = processClaim(claimSubmission);
        if processingResult is error {
            check handleClaimFailure(claimSubmission, message.routingKey, message?.properties, processingResult, caller);
            return;
        }
        check caller->basicAck();
    }
}
