import ballerina/lang.runtime;
import ballerina/lang.value;
import ballerina/log;
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
# + return - the reservation response on success, or an error (timeout or protocol failure)
function requestInventoryReservation(FulfilmentRequest fulfilmentRequest) returns ReservationResponse|error {
    string replyQueueName = check rabbitmqClient->queueAutoGenerate();
    string correlationId = uuid:createType1AsString();

    ReservationRequest reservationRequest = {
        orderId: fulfilmentRequest.orderId,
        warehouseId: fulfilmentRequest.warehouseId,
        items: fulfilmentRequest.items
    };
    rabbitmq:BasicProperties properties = {
        replyTo: replyQueueName,
        correlationId: correlationId,
        contentType: "application/json"
    };
    rabbitmq:AnydataMessage reservationMessage = {
        content: reservationRequest,
        routingKey: INVENTORY_RESERVE_QUEUE,
        properties: properties
    };
    check rabbitmqClient->publishMessage(reservationMessage);

    decimal timeoutSeconds = reservationReplyTimeoutMillis / 1000.0d;

    worker ReplyWaiter returns ReservationResponse|error {
        return consumeReservationReply(replyQueueName);
    }

    worker TimeoutGuard returns TimeoutSignal {
        runtime:sleep(timeoutSeconds);
        return {signal: "TIMEOUT"};
    }

    ReservationResponse|error|TimeoutSignal waitResult = wait ReplyWaiter | TimeoutGuard;

    if waitResult is TimeoutSignal {
        return error(RESERVATION_TIMEOUT_ERROR,
                message = string `Timed out waiting for inventory reservation reply for order ${fulfilmentRequest.orderId}`);
    }
    return waitResult;
}

# Polls the given server-named reply queue until a message arrives, then decodes it into a
# `ReservationResponse`. Runs inside a worker so the caller can race it against a timeout.
#
# + replyQueueName - the exclusive, server-named queue to poll for the reply
# + return - the decoded reservation response, or an error if consuming/decoding fails
isolated function consumeReservationReply(string replyQueueName) returns ReservationResponse|error {
    while true {
        rabbitmq:AnydataMessage|rabbitmq:Error consumeResult = rabbitmqClient->consumeMessage(replyQueueName);
        if consumeResult is rabbitmq:AnydataMessage {
            return value:ensureType(consumeResult.content);
        }
        runtime:sleep(0.1);
    }
}

# Simulates charging payment for an order. The sentinel warehouse ID `PAYMENT-FAIL` always
# fails, making the failure/compensation path easy to exercise from tests.
#
# + fulfilmentRequest - the fulfilment request being paid for
# + return - () on success, or an error describing why the charge failed
isolated function chargePayment(FulfilmentRequest fulfilmentRequest) returns error? {
    if fulfilmentRequest.warehouseId == "PAYMENT-FAIL" {
        return error(string `Payment charge failed for order ${fulfilmentRequest.orderId}`);
    }
}

# Simulates dispatching shipping for an order. The sentinel warehouse ID `SHIPPING-FAIL` always
# fails, making the failure/compensation path easy to exercise from tests.
#
# + fulfilmentRequest - the fulfilment request being shipped
# + return - () on success, or an error describing why dispatch failed
isolated function dispatchShipping(FulfilmentRequest fulfilmentRequest) returns error? {
    if fulfilmentRequest.warehouseId == "SHIPPING-FAIL" {
        return error(string `Shipping dispatch failed for order ${fulfilmentRequest.orderId}`);
    }
}

# Simulates releasing a previously reserved inventory hold. This is the compensating action for
# a successful inventory reservation, run when a later saga step (payment) fails.
#
# + fulfilmentRequest - the fulfilment request whose inventory reservation should be released
isolated function releaseInventory(FulfilmentRequest fulfilmentRequest) {
    log:printInfo(string `Releasing inventory reservation for order ${fulfilmentRequest.orderId}`);
}

# Simulates refunding a previously captured payment. This is the compensating action for a
# successful payment charge, run when a later saga step (shipping) fails.
#
# + fulfilmentRequest - the fulfilment request whose payment should be refunded
isolated function refundPayment(FulfilmentRequest fulfilmentRequest) {
    log:printInfo(string `Refunding payment for order ${fulfilmentRequest.orderId}`);
}

# Runs the full order fulfilment saga for a single request: reserve inventory, charge payment,
# then dispatch shipping. Each step's progress is tracked in the saga state store. If payment
# fails after a successful reservation, the inventory is released. If shipping fails after a
# successful charge, the payment is refunded (which implicitly leaves the inventory released
# as well, since a shipment can only be attempted after payment succeeds).
#
# + fulfilmentRequest - the order fulfilment request driving the saga
# + return - the final reservation response when the saga fails at the reservation step, () on
# full success (or on failure with recorded compensation), or an error for infrastructure
# failures unrelated to the business steps themselves
function runFulfilmentSaga(FulfilmentRequest fulfilmentRequest) returns ReservationResponse|error? {
    string orderId = fulfilmentRequest.orderId;
    _ = startSaga(orderId);

    ReservationResponse|error reservationResponse = requestInventoryReservation(fulfilmentRequest);
    if reservationResponse is error {
        failSaga(orderId, reservationResponse.message());
        return reservationResponse;
    }
    if !reservationResponse.reserved {
        string reservationFailureReason = reservationResponse?.message ?: "Inventory reservation was declined";
        failSaga(orderId, reservationFailureReason);
        return reservationResponse;
    }
    recordSagaStep(orderId, SAGA_INVENTORY_RESERVED, "reserve-inventory");

    error? paymentResult = chargePayment(fulfilmentRequest);
    if paymentResult is error {
        releaseInventory(fulfilmentRequest);
        recordCompensation(orderId, "release-inventory");
        failSaga(orderId, paymentResult.message());
        return;
    }
    recordSagaStep(orderId, SAGA_PAYMENT_CHARGED, "charge-payment");

    error? shippingResult = dispatchShipping(fulfilmentRequest);
    if shippingResult is error {
        refundPayment(fulfilmentRequest);
        recordCompensation(orderId, "refund-payment");
        releaseInventory(fulfilmentRequest);
        recordCompensation(orderId, "release-inventory");
        failSaga(orderId, shippingResult.message());
        return;
    }
    recordSagaStep(orderId, SAGA_SHIPPING_DISPATCHED, "dispatch-shipping");

    completeSaga(orderId);
    return;
}
