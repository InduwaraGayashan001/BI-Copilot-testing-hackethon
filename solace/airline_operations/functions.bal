import ballerinax/solace;

# Builds the hierarchical Solace topic name for a flight event of the form
# `airline/ops/{carrierCode}/{departureAirport}/{eventType}`.
#
# + flightEvent - The flight event to derive the topic for
# + return - The hierarchical topic name
function buildFlightEventTopic(FlightEvent flightEvent) returns string {
    return string `airline/ops/${flightEvent.carrierCode}/${flightEvent.departureAirport}/${flightEvent.eventType}`;
}

# Derives the message priority based on the event type, giving cancellations the highest priority.
#
# + eventType - The category of the flight operational event
# + return - A priority value between 0 (lowest) and 9 (highest)
function derivePriority(EventType eventType) returns int {
    match eventType {
        CANCELLATION => {
            return 9;
        }
        DELAY => {
            return 7;
        }
        GATE_CHANGE => {
            return 6;
        }
        DEPARTED|ARRIVED => {
            return 5;
        }
        BOARDING => {
            return 4;
        }
        _ => {
            return 0;
        }
    }
}

# Publishes a flight operational event onto the Solace PubSub+ topic hierarchy.
#
# + flightEvent - The flight event to publish
# + return - The topic the event was published to, or a `solace:Error` if publishing fails
function publishFlightEvent(FlightEvent flightEvent) returns string|solace:Error {
    string topicName = buildFlightEventTopic(flightEvent);
    int priority = derivePriority(flightEvent.eventType);

    map<solace:Property> properties = {
        flightNumber: flightEvent.flightNumber,
        departureAirport: flightEvent.departureAirport,
        arrivalAirport: flightEvent.arrivalAirport
    };

    solace:Message message = {
        payload: flightEvent,
        deliveryMode: solace:PERSISTENT,
        priority,
        correlationId: flightEvent.eventId,
        properties
    };

    check solaceProducer->send(message, {topicName});
    return topicName;
}
