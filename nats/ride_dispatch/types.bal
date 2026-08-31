// Represents a ride dispatch request published to the NATS subject rides.request.{city}
public type RideRequest record {|
    string rideId;
    string riderId;
    string city;
    decimal pickupLat;
    decimal pickupLng;
    string requestedAt;
|};

// Represents a driver ETA lookup request published to the NATS subject drivers.eta.{city}
public type DriverEtaRequest record {|
    string rideId;
    string city;
    decimal pickupLat;
    decimal pickupLng;
|};

// Represents the driver ETA reply received for a DriverEtaRequest
public type DriverEtaResponse record {|
    string rideId;
    string driverId;
    decimal etaMinutes;
|};
