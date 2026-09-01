import ballerina/log;
import ballerinax/ibm.ibmmq;

@ibmmq:ServiceConfig {
    topicName: marketDataTopicName,
    consumerType: ibmmq:DURABLE,
    subscriberName: subscriberName,
    sessionAckMode: ibmmq:CLIENT_ACKNOWLEDGE,
    messageSelector: instrumentClassSelector,
    pollingInterval: pollingInterval,
    receiveTimeout: receiveTimeout
}
service ibmmq:Service on marketDataListener {

    # Handles an incoming market data price tick delivered through the
    # durable subscription on MARKET.DATA.PRICES. The message is
    # acknowledged only after it has been successfully bound to a
    # PriceTick record and processed; if either step fails, the message is
    # left unacknowledged so it is redelivered.
    #
    # + message - the received IBM MQ message
    # + caller - the caller used to acknowledge the message
    # + return - an error if the acknowledgement itself fails
    remote function onMessage(ibmmq:Message message, ibmmq:Caller caller) returns error? {
        PriceTick|error priceTick = mapToPriceTick(message);
        if priceTick is error {
            log:printError("Failed to bind the price tick payload", priceTick);
            return;
        }

        error? processResult = processPriceTick(priceTick);
        if processResult is error {
            log:printError("Failed to process the price tick", processResult,
                    instrumentId = priceTick.instrumentId);
            return;
        }

        ibmmq:Error? acknowledgeResult = caller->acknowledge(message);
        if acknowledgeResult is ibmmq:Error {
            log:printError("Failed to acknowledge the price tick", acknowledgeResult,
                    instrumentId = priceTick.instrumentId);
            return acknowledgeResult;
        }

        log:printInfo("Price tick acknowledged", instrumentId = priceTick.instrumentId);
    }

    # Handles runtime errors that occur while receiving or dispatching
    # messages from the MARKET.DATA.PRICES subscription.
    #
    # + mqError - the error encountered by the listener
    remote function onError(ibmmq:Error mqError) returns error? {
        log:printError("Error while receiving market data from IBM MQ", mqError);
    }
}

