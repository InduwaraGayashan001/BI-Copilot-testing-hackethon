import ballerina/http;

# Represents the category of a flight operational event.
public enum EventType {
    GATE_CHANGE,
    DELAY,
    CANCELLATION,
    BOARDING,
    DEPARTED,
    ARRIVED
}

# Represents a flight operational event submitted by a client.
#
# + eventId - Unique identifier for the event
# + flightNumber - Flight number associated with the event
# + carrierCode - Carrier (airline) code operating the flight
# + departureAirport - IATA/ICAO code of the departure airport
# + arrivalAirport - IATA/ICAO code of the arrival airport
# + eventType - Category of the operational event
# + scheduledTime - Originally scheduled time for the flight event
# + actualTime - Actual time at which the event occurred
# + delayMinutes - Delay duration in minutes, applicable for delay related events
public type FlightEvent record {|
    string eventId;
    string flightNumber;
    string carrierCode;
    string departureAirport;
    string arrivalAirport;
    EventType eventType;
    string scheduledTime;
    string actualTime;
    int delayMinutes?;
|};

# Represents a successful acknowledgement after publishing a flight event.
#
# + eventId - Unique identifier of the published event
# + topic - Hierarchical Solace topic the event was published to
public type FlightEventAck record {|
    string eventId;
    string topic;
|};

# Represents the response returned when a flight event is accepted for publishing.
public type FlightEventAccepted record {|
    *http:Created;
    FlightEventAck body;
|};

# Represents an error detail payload.
#
# + message - Human readable error description
public type ErrorDetail record {|
    string message;
|};

# Represents the response returned when the request payload is invalid.
public type FlightEventBadRequest record {|
    *http:BadRequest;
    ErrorDetail body;
|};

# Represents the response returned when publishing the event to Solace fails.
public type FlightEventPublishError record {|
    *http:InternalServerError;
    ErrorDetail body;
|};
