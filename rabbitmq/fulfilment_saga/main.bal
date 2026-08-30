import ballerina/http;

service /fulfilment on new http:Listener(httpListenerPort) {

    # Accepts an order fulfilment request and runs the fulfilment saga: reserve inventory,
    # charge payment, then dispatch shipping — compensating prior steps if a later step fails.
    #
    # + fulfilmentRequest - the order fulfilment request payload
    # + return - 202 Accepted once the saga has run to completion or failure (with compensation),
    # 504 if the inventory reservation reply timed out, or a 500 on other infrastructure failures
    resource function post orders(FulfilmentRequest fulfilmentRequest)
            returns http:Accepted|http:GatewayTimeout|http:InternalServerError {
        ReservationResponse|error? sagaResult = runFulfilmentSaga(fulfilmentRequest);

        if sagaResult is error {
            if sagaResult.message() == RESERVATION_TIMEOUT_ERROR {
                ErrorMessage errorMessage = {
                    message: string `Inventory reservation timed out for order ${fulfilmentRequest.orderId}`
                };
                return <http:GatewayTimeout>{body: errorMessage};
            }
            ErrorMessage errorMessage = {message: "Failed to run fulfilment saga: " + sagaResult.message()};
            return <http:InternalServerError>{body: errorMessage};
        }

        SagaState? sagaState = getSagaState(fulfilmentRequest.orderId);
        return <http:Accepted>{body: sagaState};
    }

    # Reports the current saga state (progress and any compensating actions taken) for an order.
    #
    # + orderId - the order to look up
    # + return - 200 OK with the saga state, or 404 if no saga has been started for this order
    resource function get orders/[string orderId]/saga() returns SagaState|http:NotFound {
        SagaState? sagaState = getSagaState(orderId);
        if sagaState is () {
            ErrorMessage errorMessage = {message: string `No saga found for order ${orderId}`};
            return <http:NotFound>{body: errorMessage};
        }
        return sagaState;
    }
}
