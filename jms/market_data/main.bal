import ballerina/log;
import ballerinax/java.jms;

service "market-data-prices-consumer" on marketDataPricesListener {

    // Processes an inbound price tick and acknowledges it only after processing succeeds, so a
    // failure leaves the message unacknowledged and eligible for redelivery.
    remote function onMessage(jms:Message message, jms:Caller caller) returns error? {
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

        check caller->acknowledge(message);
    }
}

// Handles a single price tick. Replace with the actual downstream processing logic.
function processPriceTick(PriceTick priceTick) {
    log:printInfo("Price tick processed",
            instrumentId = priceTick.instrumentId,
            instrumentClass = priceTick.instrumentClass,
            lastTradedPrice = priceTick.lastTradedPrice);
}
