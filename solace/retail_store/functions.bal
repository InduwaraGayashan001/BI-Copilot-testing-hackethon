import ballerina/time;
import ballerinax/solace;

# Mutable telemetry processing state (the bounded, shed-oldest buffer plus the processing
# counters surfaced on `GET /telemetry/health`), grouped into a single record so it can be
# accessed atomically within one `lock` statement. Written to from the topic endpoint
# subscription (main.bal) and the batch drain (drainBatchQueue below), and read from the health
# check resource (main.bal).
isolated TelemetryState telemetryState = {
    telemetryBuffer: [],
    shedCount: 0,
    processedCount: 0,
    drainedCount: 0,
    skippedExpiredCount: 0
};

isolated function incrementProcessedCount() {
    lock {
        telemetryState.processedCount += 1;
    }
}

isolated function incrementSkippedExpiredCount() {
    lock {
        telemetryState.skippedExpiredCount += 1;
    }
}

isolated function addDrainedCount(int count) {
    lock {
        telemetryState.drainedCount += count;
    }
}

# Appends a device telemetry reading onto the bounded buffer. When the buffer is already at
# `telemetryBufferCapacity`, the oldest buffered reading is shed (discarded) to make room for the
# newest one instead of blocking or rejecting the newest reading.
#
# + deviceTelemetry - The device telemetry reading to buffer
isolated function bufferTelemetryReading(DeviceTelemetry deviceTelemetry) {
    lock {
        if telemetryState.telemetryBuffer.length() >= telemetryBufferCapacity {
            _ = telemetryState.telemetryBuffer.shift();
            telemetryState.shedCount += 1;
        }
        telemetryState.telemetryBuffer.push(deviceTelemetry.clone());
    }
}

# Builds the current telemetry health snapshot.
#
# + return - The current buffer state and processing counters
isolated function buildTelemetryHealth() returns TelemetryHealth {
    lock {
        return {
            bufferedCount: telemetryState.telemetryBuffer.length(),
            bufferCapacity: telemetryBufferCapacity,
            shedCount: telemetryState.shedCount,
            processedCount: telemetryState.processedCount,
            drainedCount: telemetryState.drainedCount,
            skippedExpiredCount: telemetryState.skippedExpiredCount
        };
    }
}

# Checks whether a device telemetry message has already expired, based on the broker-calculated
# `expiration` timestamp (populated because the listener/consumer has `calculateMessageExpiration`
# enabled). An `expiration` of zero means the message never expires.
#
# + expiration - The broker-calculated expiration timestamp of the message, if present
# + return - `true` if a non-zero expiration timestamp has already passed relative to the
# current time
function isExpired(int? expiration) returns boolean {
    if expiration is () || expiration == 0 {
        return false;
    }

    time:Utc currentTime = time:utcNow();
    int currentTimeMillis = currentTime[0] * 1000;
    return expiration < currentTimeMillis;
}

# Drains the nightly batch queue `RETAIL.TELEMETRY.BATCH` using a blocking receive per message
# (waiting up to `batchReceiveTimeout` for each one). Once a blocking receive times out without
# producing a message, a non-blocking `receiveNoWait` is issued to confirm the queue is actually
# empty (rather than merely quiet) before the drain stops. Every message is client-acknowledged;
# readings whose expiration has already passed are still acknowledged (so they are removed from
# the queue) but are counted separately instead of being buffered for downstream processing. The
# consumer is closed on every path, including on error.
#
# + return - The drained and skipped-expired counts, or a `solace:Error` if consuming/closing
# fails partway through
function drainBatchQueue() returns DrainResult|solace:Error {
    solace:MessageConsumer batchConsumer = check createBatchQueueConsumer();

    int drainedCountLocal = 0;
    int skippedExpiredCountLocal = 0;
    solace:Error? failure = ();

    while true {
        DeviceTelemetryMessage|solace:Error? receiveResult = batchConsumer->receive(batchReceiveTimeout);

        if receiveResult is solace:Error {
            failure = receiveResult;
            break;
        }

        if receiveResult is () {
            // The blocking receive timed out; confirm the queue is actually empty with a
            // non-blocking receive before stopping the drain.
            DeviceTelemetryMessage|solace:Error? noWaitResult = batchConsumer->receiveNoWait();

            if noWaitResult is solace:Error {
                failure = noWaitResult;
                break;
            }

            if noWaitResult is () {
                break;
            }

            solace:Error? outcome = handleDrainedMessage(noWaitResult, batchConsumer);
            if outcome is solace:Error {
                failure = outcome;
                break;
            }
            if isExpired(noWaitResult?.expiration) {
                skippedExpiredCountLocal += 1;
            } else {
                drainedCountLocal += 1;
            }
            continue;
        }

        solace:Error? outcome = handleDrainedMessage(receiveResult, batchConsumer);
        if outcome is solace:Error {
            failure = outcome;
            break;
        }
        if isExpired(receiveResult?.expiration) {
            skippedExpiredCountLocal += 1;
        } else {
            drainedCountLocal += 1;
        }
    }

    check batchConsumer->close();

    if failure is solace:Error {
        return failure;
    }

    addDrainedCount(drainedCountLocal);
    return {
        drainedCount: drainedCountLocal,
        skippedExpiredCount: skippedExpiredCountLocal
    };
}

# Acknowledges a single message drained from the batch queue.
#
# + message - The device telemetry message drained from the batch queue
# + batchConsumer - The batch queue consumer the message was received from
# + return - A `solace:Error` if acknowledgement fails
function handleDrainedMessage(DeviceTelemetryMessage message, solace:MessageConsumer batchConsumer)
        returns solace:Error? {
    check batchConsumer->ack(message);
}

# Determines whether a device telemetry reading's metric value has crossed the threshold
# configured for its device type.
#
# + deviceTelemetry - The device telemetry reading to check
# + return - The configured threshold if the reading's value has crossed it, or `()` if the
# device type has no configured threshold or the value has not crossed it
function checkThresholdCrossed(DeviceTelemetry deviceTelemetry) returns decimal? {
    decimal? threshold = deviceTypeThresholds[deviceTelemetry.deviceType];
    if threshold is () {
        return ();
    }
    if deviceTelemetry.value > threshold {
        return threshold;
    }
    return ();
}

# Builds the hierarchical Solace topic name for a device telemetry alert of the form
# `retail/alerts/{region}/{storeId}`.
#
# + deviceTelemetry - The device telemetry reading the alert is raised for
# + return - The hierarchical topic name
function buildAlertTopic(DeviceTelemetry deviceTelemetry) returns string {
    return string `retail/alerts/${deviceTelemetry.region}/${deviceTelemetry.storeId}`;
}

# Packs a compact correlation payload for a device telemetry alert into a fixed-width byte array
# to be carried in the message's `userData` field. Solace caps `userData` at 36 bytes; this
# encoding uses 16 bytes for the (UTF-8, truncated/padded) storeId, 8 bytes for the epoch-second
# timestamp and 1 byte for the severity flag - 25 bytes in total, comfortably within the limit.
#
# + alertCorrelation - The correlation details to pack
# + return - The packed 25-byte correlation payload
function packAlertCorrelation(AlertCorrelation alertCorrelation) returns byte[] {
    byte[] storeIdBytes = alertCorrelation.storeId.toBytes();
    byte[] correlationBytes = [];

    int storeIdFieldLength = 16;
    foreach int i in 0 ..< storeIdFieldLength {
        if i < storeIdBytes.length() {
            correlationBytes.push(storeIdBytes[i]);
        } else {
            correlationBytes.push(0);
        }
    }

    int triggeredAt = alertCorrelation.triggeredAt;
    foreach int i in 0 ..< 8 {
        int shiftAmount = (7 - i) * 8;
        correlationBytes.push(<byte>((triggeredAt >> shiftAmount) & 255));
    }

    correlationBytes.push(<byte>alertCorrelation.severity);

    return correlationBytes;
}

# Publishes a device telemetry alert onto `retail/alerts/{region}/{storeId}` with direct
# (at-most-once) delivery, a short time-to-live and top priority so the alert is not queued
# behind routine telemetry traffic, carrying a compact correlation payload in the `userData`
# field.
#
# + deviceTelemetry - The device telemetry reading that crossed its threshold
# + threshold - The configured threshold that was crossed
# + return - A `solace:Error` if publishing fails
function publishDeviceTelemetryAlert(DeviceTelemetry deviceTelemetry, decimal threshold) returns solace:Error? {
    DeviceTelemetryAlert deviceTelemetryAlert = {
        storeId: deviceTelemetry.storeId,
        region: deviceTelemetry.region,
        deviceType: deviceTelemetry.deviceType,
        deviceId: deviceTelemetry.deviceId,
        metric: deviceTelemetry.metric,
        value: deviceTelemetry.value,
        threshold,
        unit: deviceTelemetry.unit
    };

    time:Utc currentTime = time:utcNow();
    AlertCorrelation alertCorrelation = {
        storeId: deviceTelemetry.storeId,
        triggeredAt: currentTime[0],
        severity: 1
    };
    byte[] userData = packAlertCorrelation(alertCorrelation);

    string topicName = buildAlertTopic(deviceTelemetry);

    solace:Message alertMessage = {
        payload: deviceTelemetryAlert,
        deliveryMode: solace:DIRECT,
        priority: alertPriority,
        timeToLive: alertTimeToLive,
        userData
    };

    check alertProducer->send(alertMessage, {topicName});
}

