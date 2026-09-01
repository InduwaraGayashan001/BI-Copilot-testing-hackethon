import ballerina/log;

// Processes a received market data price tick. Any downstream processing
// failure is returned as an error so the caller can decide how to handle
// redelivery.
function processPriceTick(PriceTick priceTick) returns error? {
    log:printInfo("Processed price tick", instrumentId = priceTick.instrumentId,
            instrumentClass = priceTick.instrumentClass, price = priceTick.price,
            currency = priceTick.currency, timestamp = priceTick.timestamp);
}

