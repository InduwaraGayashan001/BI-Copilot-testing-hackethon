import ballerinax/nats;

// Publishes a ride request to the subject rides.request.{city}.
function publishRideRequest(RideRequest rideRequest) returns nats:Error? {
    string subject = string `rides.request.${rideRequest.city}`;
    nats:AnydataMessage message = {
        content: rideRequest,
        subject: subject
    };
    check natsClient->publishMessage(message);
}

// Publishes a driver ETA lookup request to drivers.eta.{city} and waits for the reply
// using the request-reply API, bounded by the configurable driverEtaTimeout.
function lookupDriverEta(DriverEtaRequest etaRequest) returns DriverEtaResponse|nats:Error {
    string subject = string `drivers.eta.${etaRequest.city}`;
    nats:AnydataMessage requestMessage = {
        content: etaRequest,
        subject: subject
    };
    nats:AnydataMessage reply = check natsClient->requestMessage(requestMessage, driverEtaTimeout);
    DriverEtaResponse|error etaResponse = reply.content.cloneWithType(DriverEtaResponse);
    if etaResponse is error {
        return error nats:Error("Failed to bind driver ETA response", etaResponse);
    }
    return etaResponse;
}
