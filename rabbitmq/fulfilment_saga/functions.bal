import ballerina/lang.runtime;
import ballerina/lang.value;
import ballerina/uuid;
import ballerinax/rabbitmq;

# Error reason used when the inventory service does not reply within the configured timeout.
public const string RESERVATION_TIMEOUT_ERROR = "ReservationTimeoutError";

# Sentinel value returned by the timeout guard worker when the reservation reply does not
# arrive within `reservationReplyTimeoutMillis`.
type TimeoutSignal record {|
    string signal;
|};

# Publishes a reservation request for the given fulfilment request to the inventory service on a
# server-named exclusive reply queue, then waits for the reply up to `reservationReplyTimeoutMillis`.
#
# + fulfilmentRequest - the fulfilment request to reserve inventory for
# + return - the reservation result on success, or an error (timeout or protocol failure)
function requestInventoryReservation(FulfilmentRequest fulfilmentRequest) returns ReservationResult|error {
    string replyQueueName = check rabbitmqClient->queueAutoGenerate();
    string correlationId = uuid:createType1AsString();

    rabbitmq:BasicProperties properties = {
        replyTo: replyQueueName,
        correlationId: correlationId,
        contentType: "application/json"
    };
    rabbitmq:AnydataMessage reservationMessage = {
        content: fulfilmentRequest,
        routingKey: INVENTORY_RESERVE_QUEUE,
        properties: properties
    };
    check rabbitmqClient->publishMessage(reservationMessage);

    decimal timeoutSeconds = reservationReplyTimeoutMillis / 1000.0d;

    worker ReplyWaiter returns ReservationResult|error {
        return consumeReservationReply(replyQueueName);
    }

    worker TimeoutGuard returns TimeoutSignal {
        runtime:sleep(timeoutSeconds);
        return {signal: "TIMEOUT"};
    }

    ReservationResult|error|TimeoutSignal waitResult = wait ReplyWaiter | TimeoutGuard;

    if waitResult is TimeoutSignal {
        return error(RESERVATION_TIMEOUT_ERROR,
                message = string `Timed out waiting for inventory reservation reply for order ${fulfilmentRequest.orderId}`);
    }
    return waitResult;
}

# Polls the given server-named reply queue until a message arrives, then decodes it into a
# `ReservationResult`. Runs inside a worker so the caller can race it against a timeout.
#
# + replyQueueName - the exclusive, server-named queue to poll for the reply
# + return - the decoded reservation result, or an error if consuming/decoding fails
isolated function consumeReservationReply(string replyQueueName) returns ReservationResult|error {
    while true {
        rabbitmq:AnydataMessage|rabbitmq:Error consumeResult = rabbitmqClient->consumeMessage(replyQueueName);
        if consumeResult is rabbitmq:AnydataMessage {
            return value:ensureType(consumeResult.content);
        }
        runtime:sleep(0.1);
    }
}
