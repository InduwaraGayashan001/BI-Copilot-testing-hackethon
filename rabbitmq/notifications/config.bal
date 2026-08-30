configurable string rabbitmqHost = "localhost";
configurable int rabbitmqPort = 5672;
configurable string rabbitmqVhost = "/";
configurable string rabbitmqUsername = ?;
configurable string rabbitmqPassword = ?;

configurable int httpListenerPort = 8080;

# Number of unacknowledged messages each channel consumer will prefetch. Each channel has its
# own listener/QoS so one channel's backlog cannot starve another.
configurable int emailPrefetchCount = 10;
configurable int smsPrefetchCount = 20;
configurable int pushPrefetchCount = 20;

# Maximum number of notifications a single tenant may have dispatched, per channel, within
# `rateLimitWindowSeconds`. Exceeding this causes the message to be requeued (not dropped) so
# it is retried once the tenant's window rolls over.
configurable int tenantRateLimitPerWindow = 100;
configurable decimal rateLimitWindowSeconds = 60;

# Fanout exchange that broadcasts every notification to all channel queues (email, sms, push).
# A fanout exchange ignores routing keys, so the same publish reaches every bound queue.
const string NOTIFICATIONS_EXCHANGE = "notifications.broadcast";

const string NOTIFICATIONS_EMAIL_QUEUE = "notifications.email";
const string NOTIFICATIONS_SMS_QUEUE = "notifications.sms";
const string NOTIFICATIONS_PUSH_QUEUE = "notifications.push";

# RabbitMQ queue argument that selects the quorum queue type. Quorum queues are durable,
# Raft-replicated queues (RabbitMQ 3.8+) and are declared with `durable: true`.
#
# NOTE: quorum queues do NOT support the classic `x-max-priority` argument -- it is silently
# ignored on this queue type. Native strict message priority (0-31) for quorum queues requires
# RabbitMQ 4.3+, and needs no declare-time argument at all: the broker honors the message's
# `priority` property directly. This deployment assumes RabbitMQ 4.3+.
const string ARG_QUEUE_TYPE = "x-queue-type";
const string QUEUE_TYPE_QUORUM = "quorum";

# Custom application header carrying the tenant that a notification belongs to, so downstream
# channel consumers can apply per-tenant handling without deserializing the payload.
const string TENANT_ID_HEADER = "x-tenant-id";

# Custom application header carrying the notificationId, so channel consumers can track/dedupe
# deliveries without deserializing the payload.
const string NOTIFICATION_ID_HEADER = "x-notification-id";

# Custom application header carrying the numeric urgency-derived priority (0-31). The Ballerina
# RabbitMQ connector's `BasicProperties` record does not expose the AMQP `priority` property, so
# this header is the observable signal consumers can use; it does not itself trigger the
# broker's native quorum-queue priority reordering (RabbitMQ 4.3+ reads the AMQP `priority`
# property, not application headers, for that).
const string PRIORITY_HEADER = "x-priority";
