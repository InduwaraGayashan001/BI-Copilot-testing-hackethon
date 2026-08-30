import ballerina/http;

listener http:Listener reconciliationListener = new (reconciliationServicePort);

service /settlement on reconciliationListener {

    // Reports whether the given paymentId has been settled within the
    // configured duplicate-suppression TTL window.
    resource function get status/[string paymentId]() returns SettlementStatus {
        int? expiry = getProcessedEntry(paymentId);
        return {
            paymentId: paymentId,
            settled: expiry is int,
            expiryEpochSeconds: expiry
        };
    }

    // Seeks the reconciliation consumer to the offsets corresponding to
    // `fromTimestamp` (falling back to the earliest offset per-partition when
    // no offset exists at or after that timestamp) and re-drives every record
    // found from there through the same settlement path used by the listener.
    resource function post replay(@http:Payload ReplayRequest replayRequest) returns ReplayResult|http:InternalServerError {
        ReplayResult|error replayResult = replayFromTimestamp(replayRequest.fromTimestamp);
        if replayResult is error {
            return {
                body: {message: "Failed to replay payment authorized events: " + replayResult.message()}
            };
        }
        return replayResult;
    }

    // Reports the reconciliation consumer's assigned partitions, the last
    // committed offset per partition, and the current lag, all derived
    // directly from the Kafka consumer's offset APIs.
    resource function get health() returns SettlementHealth|http:InternalServerError {
        SettlementHealth|error health = getSettlementHealth();
        if health is error {
            return {
                body: {message: "Failed to compute settlement health: " + health.message()}
            };
        }
        return health;
    }
}
