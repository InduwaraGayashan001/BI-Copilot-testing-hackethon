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
