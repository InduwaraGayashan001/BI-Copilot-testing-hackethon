import ballerina/http;
import ballerina/log;
import ballerinax/java.jms;

listener http:Listener marketDataControlListener = new (servicePort);

// A named service value (rather than an inline service-on-listener) so it can be dynamically
// detached/attached to marketDataPricesListener for the pause/resume/unsubscribe endpoints.
jms:Service marketDataPricesConsumerService = service object {

    // Processes an inbound price tick and acknowledges it only after processing succeeds, so a
    // failure leaves the message unacknowledged and eligible for redelivery. In-flight count is
    // tracked so the unsubscribe endpoint can refuse while ticks are still being processed.
    remote function onMessage(jms:Message message, jms:Caller caller) returns error? {
        inFlightTracker.increment();
        error? processResult = handlePriceTickMessage(message, caller);
        inFlightTracker.decrement();
        return processResult;
    }
};

function handlePriceTickMessage(jms:Message message, jms:Caller caller) returns error? {
    if message !is jms:TextMessage {
        log:printWarn("Received non-text message on MARKET.DATA.PRICES, skipping");
        return;
    }

    PriceTick|error priceTick = message.content.fromJsonStringWithType(PriceTick);
    if priceTick is error {
        log:printError("Failed to parse price tick payload", 'error = priceTick);
        return;
    }

    processPriceTick(priceTick);
    check publishNormalisedTick(priceTick);
    check publishSpreadAlertIfNeeded(priceTick);

    check caller->acknowledge(message);
}

// Handles a single price tick. Replace with the actual downstream processing logic.
function processPriceTick(PriceTick priceTick) {
    log:printInfo("Price tick processed",
            instrumentId = priceTick.instrumentId,
            instrumentClass = priceTick.instrumentClass,
            lastTradedPrice = priceTick.lastTradedPrice);
}

// Explicitly attaches the consumer service to the durable-subscription listener at startup,
// instead of the declarative `service on listener` syntax, so it can later be dynamically
// detached/re-attached by the pause/resume/unsubscribe endpoints below.
function init() returns error? {
    check marketDataPricesListener.attach(marketDataPricesConsumerService, "market-data-prices-consumer");
    check marketDataPricesListener.'start();
}

service /marketData on marketDataControlListener {

    # Pauses consumption from MARKET.DATA.PRICES by detaching the consumer service. Ticks
    # published while paused are retained by the broker under the durable subscription.
    #
    # + return - Confirmation that consumption has been paused, or an error response
    resource function post pause() returns ControlResponse|http:InternalServerError {
        if !consumerState.isAttached() {
            return {state: "PAUSED", message: "Consumption is already paused"};
        }

        error? detachResult = marketDataPricesListener.detach(marketDataPricesConsumerService);
        if detachResult is error {
            return {
                body: {
                    message: "Failed to pause consumption: " + detachResult.message()
                }
            };
        }
        consumerState.markDetached();
        return {state: "PAUSED", message: "Consumption from MARKET.DATA.PRICES has been paused"};
    }

    # Resumes consumption from MARKET.DATA.PRICES by re-attaching the consumer service.
    #
    # + return - Confirmation that consumption has resumed, or an error response
    resource function post resume() returns ControlResponse|http:InternalServerError {
        if consumerState.isAttached() {
            return {state: "RUNNING", message: "Consumption is already running"};
        }

        error? attachResult = marketDataPricesListener.attach(marketDataPricesConsumerService,
                "market-data-prices-consumer");
        if attachResult is error {
            return {
                body: {
                    message: "Failed to resume consumption: " + attachResult.message()
                }
            };
        }
        consumerState.markAttached();
        return {state: "RUNNING", message: "Consumption from MARKET.DATA.PRICES has resumed"};
    }

    # Unsubscribes the durable subscription on MARKET.DATA.PRICES. Refuses with a conflict while
    # ticks are still in flight, since deleting a durable subscription with an active consumer or
    # unacknowledged messages is erroneous.
    #
    # + return - Confirmation that the subscription has been removed, a conflict if messages are
    # still in flight, or an error response
    resource function post unsubscribe() returns ControlResponse|http:Conflict|http:InternalServerError {
        int inFlightCount = inFlightTracker.get();
        if inFlightCount > 0 {
            http:Conflict conflictResponse = {
                body: {
                    message: string `Cannot unsubscribe while ${inFlightCount} message(s) are still in flight`
                }
            };
            return conflictResponse;
        }

        if consumerState.isAttached() {
            error? detachResult = marketDataPricesListener.detach(marketDataPricesConsumerService);
            if detachResult is error {
                http:InternalServerError errorResponse = {
                    body: {
                        message: "Failed to detach consumer before unsubscribing: " + detachResult.message()
                    }
                };
                return errorResponse;
            }
            consumerState.markDetached();
        }

        error? unsubscribeResult = marketDataPricesListener.gracefulStop();
        if unsubscribeResult is error {
            http:InternalServerError errorResponse = {
                body: {
                    message: "Failed to stop the listener before unsubscribing: " + unsubscribeResult.message()
                }
            };
            return errorResponse;
        }

        consumerState.markUnsubscribed();
        return {state: "UNSUBSCRIBED", message: "Durable subscription on MARKET.DATA.PRICES has been removed"};
    }
}
