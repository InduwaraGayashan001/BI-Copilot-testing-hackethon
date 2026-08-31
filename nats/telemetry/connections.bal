import ballerinax/nats;

// TLS configuration securing the connection to the NATS server using a truststore.
final nats:SecureSocket telemetrySecureSocket = {
    cert: {
        path: tlsTrustStorePath,
        password: tlsTrustStorePassword
    }
};

// Token-based authentication credentials for the NATS server.
final nats:Tokens telemetryAuthTokens = {
    token: natsAuthToken
};

// Shared NATS client used to publish alerts directly and to back the JetStream client
// used for the TELEMETRY_HOT stream.
final nats:Client natsClient = check new (natsUrl, connectionName = connectionName, auth = telemetryAuthTokens,
    secureSocket = telemetrySecureSocket);

final nats:JetStreamClient jetStreamClient = check new (natsClient);

