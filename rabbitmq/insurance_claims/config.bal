configurable string rabbitmqHost = "localhost";
configurable int rabbitmqPort = 5672;
configurable string rabbitmqVhost = "/";
configurable string rabbitmqUsername = ?;
configurable string rabbitmqPassword = ?;

configurable int httpListenerPort = 8080;

const string CLAIMS_EXCHANGE = "claims.exchange";

const string CLAIMS_AUTO_QUEUE = "claims.auto";
const string CLAIMS_HEALTH_QUEUE = "claims.health";
const string CLAIMS_PROPERTY_QUEUE = "claims.property";

const string CLAIMS_AUTO_BINDING_KEY = "claim.auto.*";
const string CLAIMS_HEALTH_BINDING_KEY = "claim.health.*";
const string CLAIMS_PROPERTY_BINDING_KEY = "claim.property.*";
