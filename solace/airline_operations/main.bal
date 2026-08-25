import ballerina/http;
import ballerinax/solace;

service /ops on new http:Listener(servicePort) {

    # Accepts a flight operational event and publishes it onto the Solace PubSub+ topic hierarchy.
    #
    # + flightEvent - The flight operational event to publish
    # + return - The acknowledgement on success, or an error response on failure
    resource function post flight\-events(@http:Payload FlightEvent flightEvent)
            returns FlightEventAccepted|FlightEventBadRequest|FlightEventPublishError {

        int? delayMinutes = flightEvent?.delayMinutes;
        if flightEvent.eventType == DELAY && delayMinutes is () {
            return <FlightEventBadRequest>{
                body: {
                    message: "delayMinutes is required when eventType is DELAY"
                }
            };
        }

        string|solace:Error result = publishFlightEvent(flightEvent);
        if result is solace:Error {
            return <FlightEventPublishError>{
                body: {
                    message: string `Failed to publish flight event: ${result.message()}`
                }
            };
        }

        return <FlightEventAccepted>{
            body: {
                eventId: flightEvent.eventId,
                topic: result
            }
        };
    }
}
