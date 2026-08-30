import ballerina/log;
import ballerinax/rabbitmq;

final rabbitmq:Client rabbitmqClient = check new (rabbitmqHost, rabbitmqPort, connectionData = {
    username: rabbitmqUsername,
    password: rabbitmqPassword,
    virtualHost: rabbitmqVhost
});

// Shared listener for all claim intake consumer services, with configurable prefetch (QoS).
listener rabbitmq:Listener claimsQueueListener = new (rabbitmqHost, rabbitmqPort, {
    prefetchCount: consumerPrefetchCount
}, {
    username: rabbitmqUsername,
    password: rabbitmqPassword,
    virtualHost: rabbitmqVhost
});

function initClaimsTopology() returns error? {
    // Best-effort cleanup so re-declaring queues with new dead-letter arguments does not fail
    // with a PRECONDITION_FAILED error when a queue already exists with different arguments
    // (e.g. from a previous version of this topology). A `queueDelete` on a queue that does not
    // exist closes the underlying AMQP channel with a protocol error, so each delete attempt
    // uses its own short-lived client/channel to avoid poisoning the shared `rabbitmqClient`.
    check deleteQueueIfExists(CLAIMS_AUTO_QUEUE);
    check deleteQueueIfExists(CLAIMS_HEALTH_QUEUE);
    check deleteQueueIfExists(CLAIMS_PROPERTY_QUEUE);
    check deleteQueueIfExists(CLAIMS_RETRY_QUEUE);
    check deleteQueueIfExists(CLAIMS_DEAD_LETTER_QUEUE);

    // Main topic exchange that claim submissions are published to.
    check rabbitmqClient->exchangeDeclare(CLAIMS_EXCHANGE, rabbitmq:TOPIC_EXCHANGE, {durable: true});

    // Dead-letter exchange that rejected/exhausted messages are routed through.
    check rabbitmqClient->exchangeDeclare(CLAIMS_DLX_EXCHANGE, rabbitmq:TOPIC_EXCHANGE, {durable: true});

    // Terminal dead-letter queue, catches everything published to the DLX.
    check rabbitmqClient->queueDeclare(CLAIMS_DEAD_LETTER_QUEUE, {durable: true});
    check rabbitmqClient->queueBind(CLAIMS_DEAD_LETTER_QUEUE, CLAIMS_DLX_EXCHANGE, CLAIMS_DEAD_LETTER_BINDING_KEY);

    // Retry queue: messages sit here for `retryTtlMillis` then dead-letter back into the main
    // exchange (using their original routing key) for a delayed reprocessing attempt.
    check rabbitmqClient->queueDeclare(CLAIMS_RETRY_QUEUE, {
        durable: true,
        arguments: {
            [ARG_DEAD_LETTER_EXCHANGE]: CLAIMS_EXCHANGE,
            [ARG_MESSAGE_TTL]: retryTtlMillis
        }
    });

    // Claim queues: on nack(requeue = false) messages are dead-lettered into the DLX.
    check rabbitmqClient->queueDeclare(CLAIMS_AUTO_QUEUE, {
        durable: true,
        arguments: {
            [ARG_DEAD_LETTER_EXCHANGE]: CLAIMS_DLX_EXCHANGE
        }
    });
    check rabbitmqClient->queueDeclare(CLAIMS_HEALTH_QUEUE, {
        durable: true,
        arguments: {
            [ARG_DEAD_LETTER_EXCHANGE]: CLAIMS_DLX_EXCHANGE
        }
    });
    check rabbitmqClient->queueDeclare(CLAIMS_PROPERTY_QUEUE, {
        durable: true,
        arguments: {
            [ARG_DEAD_LETTER_EXCHANGE]: CLAIMS_DLX_EXCHANGE
        }
    });

    check rabbitmqClient->queueBind(CLAIMS_AUTO_QUEUE, CLAIMS_EXCHANGE, CLAIMS_AUTO_BINDING_KEY);
    check rabbitmqClient->queueBind(CLAIMS_HEALTH_QUEUE, CLAIMS_EXCHANGE, CLAIMS_HEALTH_BINDING_KEY);
    check rabbitmqClient->queueBind(CLAIMS_PROPERTY_QUEUE, CLAIMS_EXCHANGE, CLAIMS_PROPERTY_BINDING_KEY);
}

# Deletes a queue if it exists, tolerating the case where it does not. A dedicated, short-lived
# client/channel is used so that a NOT_FOUND protocol error (which closes the AMQP channel it
# occurred on) does not poison the shared `rabbitmqClient` used for the rest of the topology
# setup and for publishing.
#
# + queueName - the name of the queue to delete
# + return - () when the queue was deleted or did not exist, or an error for unexpected failures
function deleteQueueIfExists(string queueName) returns error? {
    rabbitmq:Client|rabbitmq:Error cleanupClient = new (rabbitmqHost, rabbitmqPort, connectionData = {
        username: rabbitmqUsername,
        password: rabbitmqPassword,
        virtualHost: rabbitmqVhost
    });
    if cleanupClient is rabbitmq:Error {
        return cleanupClient;
    }
    rabbitmq:Error? deleteResult = cleanupClient->queueDelete(queueName);
    if deleteResult is rabbitmq:Error {
        log:printInfo(string `Skipping delete for queue '${queueName}' (it may not exist yet): ${deleteResult.message()}`);
    }
}
