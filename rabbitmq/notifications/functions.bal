import ballerinax/rabbitmq;

# Extracts the tenantId and notificationId carried in a notification message's headers.
# Both are required for a message to be processed by a channel consumer: the tenantId drives
# rate limiting and the notificationId drives delivery-dedup tracking.
#
# + properties - the message's basic properties, if present
# + return - the (tenantId, notificationId) pair, or an error describing which header is missing
function extractNotificationHeaders(rabbitmq:BasicProperties? properties) returns [string, string]|error {
    if properties is () {
        return error("Notification message is missing properties");
    }
    map<anydata>? headers = properties?.headers;
    if headers is () {
        return error("Notification message is missing headers");
    }
    anydata tenantIdValue = headers[TENANT_ID_HEADER];
    anydata notificationIdValue = headers[NOTIFICATION_ID_HEADER];
    if tenantIdValue is string && notificationIdValue is string {
        return [tenantIdValue, notificationIdValue];
    }
    return error("Notification message is missing the tenantId/notificationId headers");
}

# Maps a notification's requested urgency to a numeric message priority. Quorum queues on
# RabbitMQ 4.3+ support strict priorities in the 0-31 range; these levels are spread across
# that range so `urgent` notifications are dispatched ahead of lower-urgency ones.
#
# + urgency - the requested urgency level
# + return - the numeric priority (0-31) corresponding to the urgency
function mapUrgencyToPriority(NotificationUrgency urgency) returns int {
    match urgency {
        URGENCY_LOW => {
            return 0;
        }
        URGENCY_NORMAL => {
            return 8;
        }
        URGENCY_HIGH => {
            return 16;
        }
        URGENCY_URGENT => {
            return 31;
        }
    }
    return 8;
}
