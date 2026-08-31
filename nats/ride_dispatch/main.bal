import ballerina/log;
import ballerinax/nats;

// Subscribes to rides.request.* (one wildcard token per city, e.g. rides.request.colombo)
// using a queue group so that multiple instances of this service share the load -
// only one member of the queue group receives each message. The pendingLimits cap how many
// messages/bytes this subscription will buffer in flight while waiting to be processed -
// once either limit is hit, the NATS client marks the subscription as a slow consumer, any
// further incoming messages are dropped (not queued) until buffered messages are processed
// and drained below the limits, and onError is invoked with the dropped-message notification.
@nats:ServiceConfig {
    subject: "rides.request.*",
    queueName: queueGroupName,
    pendingLimits: dispatchPendingLimits
}
service nats:Service on new nats:Listener(natsUrl, connectionName = connectionName, retryConfig = natsRetryConfig) {

    remote function onMessage(nats:AnydataMessage message) returns error? {
        RideRequest rideRequest = check message.content.cloneWithType(RideRequest);
        log:printInfo(string `Dispatching ride ${rideRequest.rideId} for rider ${rideRequest.riderId} in ${rideRequest.city}`);
    }

    // Handles failures separately based on their cause: malformed/unbindable payloads,
    // constraint validation failures, and any other processing errors.
    remote function onError(nats:AnydataMessage message, nats:Error err) {
        if err is nats:PayloadBindingError {
            log:printError(string `Payload binding failed for subject ${message.subject}`, 'error = err);
        } else if err is nats:PayloadValidationError {
            log:printError(string `Payload constraint validation failed for subject ${message.subject}`, 'error = err);
        } else {
            log:printError(string `Error while processing message on subject ${message.subject}`, 'error = err);
        }
    }
}
