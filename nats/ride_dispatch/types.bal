// Represents a ride dispatch request published to the NATS subject rides.request.{city}
public type RideRequest record {|
    string rideId;
    string riderId;
    string city;
    decimal pickupLat;
    decimal pickupLng;
    string requestedAt;
|};
