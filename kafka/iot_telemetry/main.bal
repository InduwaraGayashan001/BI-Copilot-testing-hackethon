import ballerina/log;
import ballerinax/kafka;

service kafka:Service on telemetryIngestionListener {

    remote function onConsumerRecord(kafka:Caller caller, TelemetryReadingConsumerRecord[] records) returns error? {
        foreach TelemetryReadingConsumerRecord telemetryRecord in records {
            TelemetryReading telemetryReading = telemetryRecord.value;
            log:printInfo("Ingested telemetry reading",
                    deviceId = telemetryReading.deviceId,
                    siteId = telemetryReading.siteId,
                    metric = telemetryReading.metric,
                    value = telemetryReading.value,
                    unit = telemetryReading.unit,
                    readingAt = telemetryReading.readingAt);
        }

        kafka:Error? commitResult = caller->commit();
        if commitResult is kafka:Error {
            log:printError("Failed to commit offsets for the processed batch", 'error = commitResult);
            return commitResult;
        }
        log:printInfo("Successfully processed telemetry batch", batchSize = records.length());
    }

    remote function onError(kafka:Error err) returns error? {
        log:printError("Error while consuming device telemetry events", 'error = err);
    }
}
