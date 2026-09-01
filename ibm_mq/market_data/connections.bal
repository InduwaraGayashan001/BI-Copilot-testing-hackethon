import ballerina/crypto;
import ballerinax/ibm.ibmmq;

// Truststore used to verify the IBM MQ server's certificate during the TLS
// handshake.
final crypto:TrustStore mqTruststore = {
    path: truststorePath,
    password: truststorePassword
};

// The SSL/TLS cipher suite pinned for the connection, validated against the
// connector's supported cipher suite values.
// Chosen: TLS_AES_256_GCM_SHA384 - a TLS 1.3 AEAD cipher suite (AES-256-GCM
// with SHA-384), offering forward secrecy and strong modern security with
// good performance.
// Alternatives considered:
//   - TLS_AES_128_GCM_SHA256 - TLS 1.3, lighter-weight, still modern and secure.
//   - TLS_CHACHA20_POLY1305_SHA256 - TLS 1.3, preferred on hardware without AES-NI.
//   - SSL_ECDHE_RSA_WITH_AES_256_GCM_SHA384 / SSL_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 -
//     TLS 1.2 ECDHE suites, use only if the IBM MQ server does not support TLS 1.3.
final ibmmq:SslCipherSuite mqSslCipherSuite = check sslCipherSuite.ensureType();

final ibmmq:SecureSocket mqSecureSocket = {
    cert: mqTruststore
};

final ibmmq:QueueManager marketDataQueueManager = check new (
    name = queueManagerName,
    host = host,
    port = port,
    channel = channel,
    userID = userID,
    password = password,
    secureSocket = mqSecureSocket,
    sslCipherSuite = mqSslCipherSuite
);

// Message selector limiting delivery to the configured instrument classes.
final string instrumentClassSelector = buildInstrumentClassSelector(instrumentClasses);

listener ibmmq:Listener marketDataListener = check new (
    name = queueManagerName,
    host = host,
    port = port,
    channel = channel,
    userID = userID,
    password = password,
    secureSocket = mqSecureSocket,
    sslCipherSuite = mqSslCipherSuite
);

