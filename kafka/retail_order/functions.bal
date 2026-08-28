import ballerina/log;

// Processes a single order event. Any error returned here causes the whole
// batch to be treated as failed, so the caller will not commit the offsets.
function processOrderEvent(OrderEvent orderEvent) returns error? {
    log:printInfo("Processing order event", orderId = orderEvent.orderId,
            customerId = orderEvent.customerId, orderAmount = orderEvent.orderAmount,
            currency = orderEvent.currency, itemCount = orderEvent.itemCount,
            channel = orderEvent.channel);
}
