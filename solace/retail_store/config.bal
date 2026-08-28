// Solace PubSub+ broker connection configuration.
configurable string solaceBrokerUrl = ?;
configurable string solaceVpnName = ?;

// Basic authentication credentials for the broker connection.
configurable string solaceUsername = ?;
configurable string solacePassword = ?;

// Durable topic endpoint configuration for the store telemetry subscription.
// The topic uses the `*` single-level wildcard for the region, storeId and deviceType
// segments so that telemetry from every store and device is matched:
//   retail/telemetry/*/*/*
//   - segment 1 `*` matches any region (e.g. "us-east", "eu-west")
//   - segment 2 `*` matches any storeId (e.g. "store-042")
//   - segment 3 `*` matches any deviceType (e.g. "fridge", "pos", "hvac")
configurable string telemetryTopicName = "retail/telemetry/*/*/*";
configurable string telemetryEndpointName = "RETAIL.TELEMETRY.DTE";

