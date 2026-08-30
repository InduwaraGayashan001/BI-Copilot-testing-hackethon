configurable string rabbitmqHost = "localhost";
configurable int rabbitmqPort = 5672;
configurable string rabbitmqVhost = "/";
configurable string rabbitmqUsername = ?;
configurable string rabbitmqPassword = ?;

configurable int httpListenerPort = 8080;

# Number of unacknowledged messages the shared consumer listener will prefetch per consumer.
configurable int consumerPrefetchCount = 10;

# Maximum number of retry attempts before a claim message is dead-lettered permanently.
configurable int maxRetryCount = 3;

# Delay (in milliseconds) a failed claim waits in the retry queue before being redelivered.
configurable int retryTtlMillis = 30000;

# Guards the destructive purge operation on the dead-letter queue.
configurable boolean allowDeadLetterPurge = false;

const string CLAIMS_EXCHANGE = "claims.exchange";

const string CLAIMS_AUTO_QUEUE = "claims.auto";
const string CLAIMS_HEALTH_QUEUE = "claims.health";
const string CLAIMS_PROPERTY_QUEUE = "claims.property";

const string CLAIMS_AUTO_BINDING_KEY = "claim.auto.*";
const string CLAIMS_HEALTH_BINDING_KEY = "claim.health.*";
const string CLAIMS_PROPERTY_BINDING_KEY = "claim.property.*";

# Dead-letter exchange that all claim queues route rejected messages to.
const string CLAIMS_DLX_EXCHANGE = "claims.dlx";

# Terminal queue holding messages that exhausted all retry attempts.
const string CLAIMS_DEAD_LETTER_QUEUE = "claims.dead-letter";
const string CLAIMS_DEAD_LETTER_BINDING_KEY = "#";

# Holding queue that delays a failed message before it is routed back to the main exchange for reprocessing.
const string CLAIMS_RETRY_QUEUE = "claims.retry";

# RabbitMQ queue argument names (verified against the AMQP 0-9-1 spec).
const string ARG_DEAD_LETTER_EXCHANGE = "x-dead-letter-exchange";
const string ARG_DEAD_LETTER_ROUTING_KEY = "x-dead-letter-routing-key";
const string ARG_MESSAGE_TTL = "x-message-ttl";

# Custom application header used to cap the number of redelivery attempts for a claim message.
const string RETRY_COUNT_HEADER = "x-retry-count";
