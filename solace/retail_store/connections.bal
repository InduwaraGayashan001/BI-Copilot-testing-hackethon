import ballerinax/solace;

// Listener bound to a durable topic endpoint (DTE) subscribed to the store telemetry topic
// hierarchy. Receive timestamps and expiration calculation are enabled so that every consumed
// message carries a populated `receiveTimestamp` and `expiration`, letting the service drop
// readings whose expiration has already passed.
//
// Connection compression is enabled via `solaceCompressionLevel` (see config.bal) - each message
// is compressed/decompressed in software before/after it goes over the wire, trading additional
// CPU time on both the broker and this client for reduced bytes transferred. This is worthwhile
// on slow or metered links; on a fast local/datacenter link to the broker it can end up costing
// more in CPU than it saves in transfer time, so it should be tuned per deployment.
listener solace:Listener telemetryListener = check new (solaceBrokerUrl,
    messageVpn = solaceVpnName,
    auth = {
        username: solaceUsername,
        password: solacePassword
    },
    generateReceiveTimestamps = true,
    calculateMessageExpiration = true,
    compressionLevel = solaceCompressionLevel
);

// Producer used to publish device telemetry alerts onto `retail/alerts/{region}/{storeId}`.
final solace:MessageProducer alertProducer = check new (solaceBrokerUrl,
    messageVpn = solaceVpnName,
    auth = {
        username: solaceUsername,
        password: solacePassword
    },
    compressionLevel = solaceCompressionLevel
);

# Creates a new `solace:MessageConsumer` bound to the nightly batch queue
# `RETAIL.TELEMETRY.BATCH` with client acknowledgement, used to drain the queue on demand from
# `POST /telemetry/drain`. A fresh consumer is created per drain request and closed once the
# drain completes (see `drainBatchQueue` in functions.bal).
#
# + return - The connected batch queue consumer, or a `solace:Error` if the connection fails
function createBatchQueueConsumer() returns solace:MessageConsumer|solace:Error {
    return new (solaceBrokerUrl,
        messageVpn = solaceVpnName,
        auth = {
            username: solaceUsername,
            password: solacePassword
        },
        calculateMessageExpiration = true,
        compressionLevel = solaceCompressionLevel,
        subscriptionConfig = {
            queueName: batchQueueName,
            ackMode: solace:CLIENT_ACK
        }
    );
}

