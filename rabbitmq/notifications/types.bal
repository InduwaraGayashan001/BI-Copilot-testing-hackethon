# Delivery channels a notification can be dispatched over.
public enum NotificationChannel {
    CHANNEL_EMAIL = "email",
    CHANNEL_SMS = "sms",
    CHANNEL_PUSH = "push"
}

# Urgency levels a notification submission can request. Mapped to a numeric message priority
# before publishing.
public enum NotificationUrgency {
    URGENCY_LOW = "low",
    URGENCY_NORMAL = "normal",
    URGENCY_HIGH = "high",
    URGENCY_URGENT = "urgent"
}

# Represents an incoming multi-tenant notification dispatch request.
public type NotificationRequest record {|
    string tenantId;
    string notificationId;
    string[] recipients;
    string subject;
    string body;
    NotificationChannel[] channels;
    NotificationUrgency urgency;
|};

# Response body returned once a notification has been published to the broadcast exchange.
public type NotificationAccepted record {|
    string notificationId;
    string tenantId;
    int priority;
|};

# Records that a single channel consumer has completed delivery of a notification, used to
# suppress reprocessing the same (notificationId, channel) pair on redelivery.
public type ChannelDelivery record {|
    NotificationChannel channel;
    string deliveredAt;
|};

# Reports the delivery status of a notification across all channels it was dispatched to,
# based on which (notificationId, channel) pairs have been recorded as delivered so far.
public type NotificationStatus record {|
    string notificationId;
    ChannelDelivery[] deliveries;
|};

# Generic error message body used by error responses.
public type ErrorMessage record {|
    string message;
|};
