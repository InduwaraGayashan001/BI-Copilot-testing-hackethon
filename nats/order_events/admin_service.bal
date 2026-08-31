import ballerina/http;

// Administrative HTTP endpoints for replaying order events and purging the ORDERS stream.
service /orders on new http:Listener(adminServicePort) {

    // Pulls the next message from orders.created synchronously and acknowledges it.
    resource function post replay() returns ReplayResult|http:InternalServerError {
        ReplayResult|error result = replayNextOrderEvent();
        if result is error {
            http:InternalServerError internalError = {
                body: {
                    message: "Failed to replay the next order event",
                    detail: result.message()
                }
            };
            return internalError;
        }
        return result;
    }

    // Empties the ORDERS stream. Only permitted when the purgeEnabled configuration is turned on.
    resource function post purge() returns PurgeResult|http:Forbidden|http:InternalServerError {
        PurgeResult|error result = purgeOrdersStream();
        if result is PurgeResult {
            return result;
        }
        if !purgeEnabled {
            http:Forbidden forbidden = {
                body: {
                    message: "Purging the orders stream is disabled",
                    detail: result.message()
                }
            };
            return forbidden;
        }
        http:InternalServerError internalError = {
            body: {
                message: "Failed to purge the orders stream",
                detail: result.message()
            }
        };
        return internalError;
    }
}
