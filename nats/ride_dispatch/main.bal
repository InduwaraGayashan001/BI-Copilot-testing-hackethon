import ballerina/log;
import ballerinax/nats;

// Subscribes to rides.request.* (one wildcard token per city, e.g. rides.request.colombo)
// using a queue group so that multiple instances of this service share the load -
// only one member of the queue group receives each message.
@nats:ServiceConfig {
    subject: "rides.request.*",
    queueName: queueGroupName
}
service nats:Service on new nats:Listener(natsUrl, connectionName = connectionName, retryConfig = natsRetryConfig) {

    remote function onMessage(nats:AnydataMessage message) returns error? {
        RideRequest rideRequest = check message.content.cloneWithType(RideRequest);
        log:printInfo(string `Dispatching ride ${rideRequest.rideId} for rider ${rideRequest.riderId} in ${rideRequest.city}`);
    }
}
