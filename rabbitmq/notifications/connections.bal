import ballerinax/rabbitmq;

# Creates the shared client used for publishing notifications and declaring topology. Extracted
# into its own function (rather than an inline `new (...)` in the module-level declaration) so
# tests can replace it via compile-time function mocking without needing a live broker to be
# reachable.
#
# + return - a new RabbitMQ client, or an error if the connection could not be established
function initRabbitmqClient() returns rabbitmq:Client|error {
    return new (rabbitmqHost, rabbitmqPort, connectionData = {
        username: rabbitmqUsername,
        password: rabbitmqPassword,
        virtualHost: rabbitmqVhost
    });
}

final rabbitmq:Client rabbitmqClient = check initRabbitmqClient();

# Dedicated listener for the email channel consumer, with its own prefetch (QoS) so a slow or
# backlogged email consumer cannot starve the sms/push consumers.
listener rabbitmq:Listener emailQueueListener = new (rabbitmqHost, rabbitmqPort, {
    prefetchCount: emailPrefetchCount
}, {
    username: rabbitmqUsername,
    password: rabbitmqPassword,
    virtualHost: rabbitmqVhost
});

# Dedicated listener for the sms channel consumer, with its own prefetch (QoS).
listener rabbitmq:Listener smsQueueListener = new (rabbitmqHost, rabbitmqPort, {
    prefetchCount: smsPrefetchCount
}, {
    username: rabbitmqUsername,
    password: rabbitmqPassword,
    virtualHost: rabbitmqVhost
});

# Dedicated listener for the push channel consumer, with its own prefetch (QoS).
listener rabbitmq:Listener pushQueueListener = new (rabbitmqHost, rabbitmqPort, {
    prefetchCount: pushPrefetchCount
}, {
    username: rabbitmqUsername,
    password: rabbitmqPassword,
    virtualHost: rabbitmqVhost
});

# Declares the `notifications.broadcast` fanout exchange and the three durable, quorum channel
# queues (email, sms, push) bound to it. A fanout exchange delivers every message published to
# it to all bound queues regardless of routing key, so a single publish reaches every channel.
#
# Quorum queues are declared with `durable: true` and the `x-queue-type: quorum` argument.
# `x-max-priority` is intentionally NOT set: it is a classic-queue-only argument that quorum
# queues silently ignore. On RabbitMQ 4.3+, quorum queues support strict message priority
# (0-31) natively with no declare-time argument -- the broker honors the AMQP `priority`
# property on each published message directly.
function initNotificationsTopology() returns error? {
    check rabbitmqClient->exchangeDeclare(NOTIFICATIONS_EXCHANGE, rabbitmq:FANOUT_EXCHANGE, {durable: true});

    rabbitmq:QueueConfig quorumQueueConfig = {
        durable: true,
        arguments: {
            [ARG_QUEUE_TYPE]: QUEUE_TYPE_QUORUM
        }
    };

    check rabbitmqClient->queueDeclare(NOTIFICATIONS_EMAIL_QUEUE, quorumQueueConfig);
    check rabbitmqClient->queueDeclare(NOTIFICATIONS_SMS_QUEUE, quorumQueueConfig);
    check rabbitmqClient->queueDeclare(NOTIFICATIONS_PUSH_QUEUE, quorumQueueConfig);

    // Fanout exchanges ignore the binding key entirely; an empty string is conventional.
    check rabbitmqClient->queueBind(NOTIFICATIONS_EMAIL_QUEUE, NOTIFICATIONS_EXCHANGE, "");
    check rabbitmqClient->queueBind(NOTIFICATIONS_SMS_QUEUE, NOTIFICATIONS_EXCHANGE, "");
    check rabbitmqClient->queueBind(NOTIFICATIONS_PUSH_QUEUE, NOTIFICATIONS_EXCHANGE, "");
}
