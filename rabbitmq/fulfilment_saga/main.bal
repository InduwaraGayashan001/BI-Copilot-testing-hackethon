import ballerina/http;

service /fulfilment on new http:Listener(httpListenerPort) {

    # Accepts an order fulfilment request, requests an inventory reservation over RabbitMQ using
    # the request-reply pattern, and returns the reservation outcome.
    #
    # + fulfilmentRequest - the order fulfilment request payload
    # + return - 200 OK with the reservation result, 504 on reservation timeout, or a 500 on other failures
    resource function post orders(FulfilmentRequest fulfilmentRequest)
            returns ReservationResult|http:GatewayTimeout|http:InternalServerError {
        ReservationResult|error reservationResult = requestInventoryReservation(fulfilmentRequest);

        if reservationResult is ReservationResult {
            return reservationResult;
        }

        if reservationResult.message() == RESERVATION_TIMEOUT_ERROR {
            ErrorMessage errorMessage = {message: string `Inventory reservation timed out for order ${fulfilmentRequest.orderId}`};
            return <http:GatewayTimeout>{body: errorMessage};
        }

        ErrorMessage errorMessage = {message: "Failed to reserve inventory: " + reservationResult.message()};
        return <http:InternalServerError>{body: errorMessage};
    }
}
