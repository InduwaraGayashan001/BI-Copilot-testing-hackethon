// Kafka broker connection details.
configurable string kafkaBootstrapServers = ?;

// MySQL database connection details used for customer enrichment.
configurable string customerDbHost = ?;
configurable int customerDbPort = 3306;
configurable string customerDbUser = ?;
configurable string customerDbPassword = ?;
configurable string customerDbName = ?;

// Retry configuration for enrichment/publish failures.
configurable int maxRetryAttempts = 3;
configurable decimal initialRetryDelaySeconds = 0.5;
configurable decimal maxRetryDelaySeconds = 8.0;
