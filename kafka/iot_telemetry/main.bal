import ballerina/log;
import ballerina/task;
import ballerinax/kafka;

// Recurring job that closes the current tumbling window on every tick,
// publishing aggregates and any threshold alerts they trigger.
class WindowFlushJob {
    *task:Job;

    public function execute() {
        flushWindowAndPublish();
    }
}

public function main() returns error? {
    task:JobId _ = check task:scheduleJobRecurByFrequency(new WindowFlushJob(), windowSizeSeconds);
    log:printInfo("Started telemetry aggregation window flush job", intervalSeconds = windowSizeSeconds);

    // Manual poll loop rather than a `kafka:Listener`, so the exact consumer
    // instance being polled is also the one `pause`/`resume` are applied to
    // for backpressure.
    while true {
        TelemetryReadingConsumerRecord[]|kafka:Error polledRecords = telemetryConsumer->poll(1);
        if polledRecords is kafka:Error {
            log:printError("Error while polling device telemetry events", 'error = polledRecords);
            continue;
        }
        if polledRecords.length() == 0 {
            continue;
        }

        foreach TelemetryReadingConsumerRecord telemetryRecord in polledRecords {
            windowAggregator.addReading(telemetryRecord.value);
        }

        kafka:Error? commitResult = telemetryConsumer->'commit();
        if commitResult is kafka:Error {
            log:printError("Failed to commit offsets for the processed batch", 'error = commitResult);
            continue;
        }
        log:printInfo("Successfully ingested telemetry batch", batchSize = polledRecords.length());
    }
}
