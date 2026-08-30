import ballerina/http;
import ballerinax/rabbitmq;

function init() returns error? {
    check initClaimsTopology();
}

service /claims on new http:Listener(httpListenerPort) {

    # Accepts a claim submission and publishes it to the claims topic exchange.
    #
    # + claimSubmission - the claim submission payload
    # + return - 202 Accepted with the routing key used, or an error response
    resource function post .(ClaimSubmission claimSubmission) returns http:Accepted|http:InternalServerError {
        string routingKey = buildRoutingKey(claimSubmission.claimType, claimSubmission.priority);

        rabbitmq:BasicProperties properties = {
            correlationId: claimSubmission.claimId,
            contentType: "application/json"
        };

        rabbitmq:AnydataMessage claimMessage = {
            content: claimSubmission,
            routingKey: routingKey,
            exchange: CLAIMS_EXCHANGE,
            properties: properties
        };

        rabbitmq:Error? publishResult = rabbitmqClient->publishMessage(claimMessage);
        if publishResult is rabbitmq:Error {
            return <http:InternalServerError>{
                body: {message: "Failed to publish claim submission: " + publishResult.message()}
            };
        }

        ClaimAccepted claimAccepted = {
            claimId: claimSubmission.claimId,
            routingKey: routingKey
        };
        return <http:Accepted>{body: claimAccepted};
    }

    # Drains and returns all claim messages currently sitting on the dead-letter queue.
    #
    # + return - 200 OK with the dead-lettered messages
    resource function get claims/dead\-letter() returns DeadLetterListing {
        DeadLetterMessage[] messages = listDeadLetterMessages();
        return {count: messages.length(), messages};
    }

    # Replays dead-lettered claims back into the main exchange for reprocessing.
    # When `claimIds` is omitted, every dead-lettered message is replayed.
    #
    # + claimIds - optional subset of claim IDs to replay; replays all when absent
    # + return - 200 OK with the list of claim IDs that were replayed, or an error response
    resource function post claims/dead\-letter/replay(@http:Payload ReplayRequest? replayRequest = ())
            returns ReplayResult|http:InternalServerError {
        string[]? claimIds = replayRequest?.claimIds;
        DeadLetterMessage[] candidates = [];
        if claimIds is string[] {
            foreach string claimId in claimIds {
                DeadLetterMessage? deadLetterMessage = removeDeadLetterMessage(claimId);
                if deadLetterMessage is DeadLetterMessage {
                    candidates.push(deadLetterMessage);
                }
            }
        } else {
            candidates = listDeadLetterMessages();
            foreach DeadLetterMessage deadLetterMessage in candidates {
                _ = removeDeadLetterMessage(deadLetterMessage.claimId);
            }
        }

        string[] replayedClaimIds = [];
        foreach DeadLetterMessage deadLetterMessage in candidates {
            rabbitmq:BasicProperties properties = {
                correlationId: deadLetterMessage.claimId,
                contentType: "application/json",
                headers: {[RETRY_COUNT_HEADER]: 0}
            };
            rabbitmq:AnydataMessage replayMessage = {
                content: deadLetterMessage.claim,
                routingKey: deadLetterMessage.routingKey,
                exchange: CLAIMS_EXCHANGE,
                properties: properties
            };
            rabbitmq:Error? publishResult = rabbitmqClient->publishMessage(replayMessage);
            if publishResult is rabbitmq:Error {
                return <http:InternalServerError>{
                    body: {message: "Failed to replay claim " + deadLetterMessage.claimId + ": " + publishResult.message()}
                };
            }
            replayedClaimIds.push(deadLetterMessage.claimId);
        }

        return {replayedCount: replayedClaimIds.length(), claimIds: replayedClaimIds};
    }

    # Purges the dead-letter queue. Disabled by default, guarded by the `allowDeadLetterPurge` flag.
    #
    # + return - 200 OK on success, or 403 Forbidden when purging is disabled
    resource function delete claims/dead\-letter() returns PurgeResult|http:Forbidden|http:InternalServerError {
        if !allowDeadLetterPurge {
            return <http:Forbidden>{
                body: {message: "Dead-letter purge is disabled. Set 'allowDeadLetterPurge' to true to enable it."}
            };
        }

        rabbitmq:Error? purgeResult = rabbitmqClient->queuePurge(CLAIMS_DEAD_LETTER_QUEUE);
        if purgeResult is rabbitmq:Error {
            return <http:InternalServerError>{
                body: {message: "Failed to purge dead-letter queue: " + purgeResult.message()}
            };
        }
        clearDeadLetterMessages();
        return {message: "Dead-letter queue purged successfully."};
    }
}
