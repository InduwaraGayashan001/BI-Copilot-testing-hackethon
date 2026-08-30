import ballerina/http;
import ballerinax/rabbitmq;

function init() returns error? {
    check initNotificationsTopology();
}

service /notifications on new http:Listener(httpListenerPort) {

    # Accepts a multi-tenant notification dispatch request and publishes it once to the
    # `notifications.broadcast` fanout exchange, which fans the message out to the email, sms,
    # and push queues. The tenant ID is carried in the message headers so channel consumers can
    # apply per-tenant handling, and the requested urgency is mapped to a numeric priority
    # (0-31) carried both as a header and, on RabbitMQ 4.3+ quorum queues, honored natively by
    # the broker for queue ordering.
    #
    # + notificationRequest - the notification dispatch request payload
    # + return - 202 Accepted with the priority used, or a 500 if publishing failed
    resource function post .(NotificationRequest notificationRequest)
            returns http:Accepted|http:InternalServerError {
        int priority = mapUrgencyToPriority(notificationRequest.urgency);

        rabbitmq:BasicProperties properties = {
            correlationId: notificationRequest.notificationId,
            contentType: "application/json",
            headers: {
                [TENANT_ID_HEADER]: notificationRequest.tenantId,
                [NOTIFICATION_ID_HEADER]: notificationRequest.notificationId,
                [PRIORITY_HEADER]: priority
            }
        };

        rabbitmq:AnydataMessage notificationMessage = {
            content: notificationRequest,
            routingKey: "",
            exchange: NOTIFICATIONS_EXCHANGE,
            properties: properties
        };

        rabbitmq:Error? publishResult = rabbitmqClient->publishMessage(notificationMessage);
        if publishResult is rabbitmq:Error {
            ErrorMessage errorMessage = {
                message: "Failed to publish notification: " + publishResult.message()
            };
            return <http:InternalServerError>{body: errorMessage};
        }

        NotificationAccepted notificationAccepted = {
            notificationId: notificationRequest.notificationId,
            tenantId: notificationRequest.tenantId,
            priority
        };
        return <http:Accepted>{body: notificationAccepted};
    }

    # Reports the delivery status of a notification: which channels have completed delivery so
    # far, based on the in-memory delivery tracker each channel consumer records into.
    #
    # + notificationId - the notification to look up
    # + return - 200 OK with the recorded per-channel deliveries, or 404 if nothing has been
    # recorded for this notification yet
    resource function get [string notificationId]() returns NotificationStatus|http:NotFound {
        ChannelDelivery[] deliveries = getDeliveries(notificationId);
        if deliveries.length() == 0 {
            ErrorMessage errorMessage = {message: string `No deliveries recorded for notification ${notificationId}`};
            return <http:NotFound>{body: errorMessage};
        }
        return {notificationId, deliveries};
    }
}
