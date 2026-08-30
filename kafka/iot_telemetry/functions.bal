import ballerina/log;
import ballerinax/kafka;

// Parses the "metric=threshold,metric=threshold" configuration string into a
// lookup map. Malformed entries are logged and skipped rather than failing
// startup.
function parseMetricAlertThresholds(string metricAlertThresholdsConfig) returns map<decimal> {
    map<decimal> thresholdsByMetric = {};
    if metricAlertThresholdsConfig.trim().length() == 0 {
        return thresholdsByMetric;
    }
    string[] thresholdEntries = re `,`.split(metricAlertThresholdsConfig);
    foreach string thresholdEntry in thresholdEntries {
        string trimmedEntry = thresholdEntry.trim();
        if trimmedEntry.length() == 0 {
            continue;
        }
        string[] metricAndThreshold = re `=`.split(trimmedEntry);
        if metricAndThreshold.length() != 2 {
            log:printWarn("Skipping malformed metric alert threshold entry", entry = trimmedEntry);
            continue;
        }
        string metricName = metricAndThreshold[0].trim();
        decimal|error thresholdValue = decimal:fromString(metricAndThreshold[1].trim());
        if thresholdValue is error {
            log:printWarn("Skipping metric alert threshold entry with an invalid decimal value",
                    entry = trimmedEntry, 'error = thresholdValue);
            continue;
        }
        thresholdsByMetric[metricName] = thresholdValue;
    }
    return thresholdsByMetric;
}

// Publishes a single closed-window aggregate to `iot.telemetry.aggregated`,
// keyed by deviceId so all windows for a device land on the same partition.
function publishWindowAggregate(WindowAggregate windowAggregate) returns error? {
    check telemetryProducer->send({
        topic: TELEMETRY_AGGREGATED_TOPIC,
        key: windowAggregate.deviceId.toBytes(),
        value: windowAggregate.toJson().toJsonString().toBytes()
    });
}

// Publishes a threshold-crossing alert to `iot.alerts`, keyed by deviceId.
function publishTelemetryAlert(TelemetryAlert telemetryAlert) returns error? {
    check telemetryProducer->send({
        topic: TELEMETRY_ALERTS_TOPIC,
        key: telemetryAlert.deviceId.toBytes(),
        value: telemetryAlert.toJson().toJsonString().toBytes()
    });
}

// Builds the alert for a window aggregate if, and only if, its mean crosses
// the given threshold for that metric. Metrics without a configured
// threshold never alert. Takes the threshold map as a parameter so the
// threshold-crossing logic can be unit tested independently of configuration.
function buildAlertIfThresholdCrossed(WindowAggregate windowAggregate, map<decimal> thresholdsByMetric)
        returns TelemetryAlert? {
    decimal? threshold = thresholdsByMetric[windowAggregate.metric];
    if threshold is () {
        return ();
    }
    if windowAggregate.mean <= threshold {
        return ();
    }
    return {
        deviceId: windowAggregate.deviceId,
        siteId: windowAggregate.siteId,
        metric: windowAggregate.metric,
        unit: windowAggregate.unit,
        mean: windowAggregate.mean,
        threshold: threshold,
        windowStart: windowAggregate.windowStart,
        windowEnd: windowAggregate.windowEnd
    };
}

// Flushes the current tumbling window, publishes every resulting aggregate,
// and publishes an alert for any aggregate whose mean crosses its metric's
// threshold. Alert publish failures drive the backpressure pause/resume of
// the telemetry consumer's partitions; aggregate publish failures are logged
// since they do not feed the alert pipeline.
function flushWindowAndPublish() {
    map<decimal> thresholdsByMetric = parseMetricAlertThresholds(metricAlertThresholds);
    WindowAggregate[] windowAggregates = windowAggregator.'flush();
    foreach WindowAggregate windowAggregate in windowAggregates {
        error? aggregatePublishResult = publishWindowAggregate(windowAggregate);
        if aggregatePublishResult is error {
            log:printError("Failed to publish window aggregate", 'error = aggregatePublishResult,
                    deviceId = windowAggregate.deviceId, metric = windowAggregate.metric);
        }

        TelemetryAlert? telemetryAlert = buildAlertIfThresholdCrossed(windowAggregate, thresholdsByMetric);
        if telemetryAlert is TelemetryAlert {
            handleAlertPublish(telemetryAlert);
        }
    }
    log:printInfo("Flushed telemetry aggregation window", windowCount = windowAggregates.length());
}

// Publishes a single alert and applies the backpressure state transition
// based on the outcome: pause the telemetry consumer's partitions on the
// first failure, and resume them on the first subsequent success.
function handleAlertPublish(TelemetryAlert telemetryAlert) {
    error? alertPublishResult = publishTelemetryAlert(telemetryAlert);
    if alertPublishResult is error {
        log:printError("Failed to publish telemetry alert", 'error = alertPublishResult,
                deviceId = telemetryAlert.deviceId, metric = telemetryAlert.metric);
        boolean transitionedToFailing = alertPublishHealth.markFailure();
        if transitionedToFailing {
            pauseTelemetryConsumerPartitions();
        }
        return;
    }
    boolean transitionedToHealthy = alertPublishHealth.markSuccess();
    if transitionedToHealthy {
        resumeTelemetryConsumerPartitions();
    }
}

// Pauses every partition currently assigned to the telemetry consumer,
// applying backpressure so no further readings are pulled in while alert
// publishing is failing.
function pauseTelemetryConsumerPartitions() {
    kafka:TopicPartition[]|kafka:Error assignedPartitions = telemetryConsumer->getAssignment();
    if assignedPartitions is kafka:Error {
        log:printError("Failed to read the telemetry consumer's assigned partitions for pausing",
                'error = assignedPartitions);
        return;
    }
    kafka:Error? pauseResult = telemetryConsumer->pause(assignedPartitions);
    if pauseResult is kafka:Error {
        log:printError("Failed to pause telemetry consumer partitions", 'error = pauseResult);
        return;
    }
    log:printWarn("Paused telemetry consumer partitions due to failing alert publishing",
            partitionCount = assignedPartitions.length());
}

// Resumes every partition currently assigned to the telemetry consumer, once
// alert publishing has recovered.
function resumeTelemetryConsumerPartitions() {
    kafka:TopicPartition[]|kafka:Error assignedPartitions = telemetryConsumer->getAssignment();
    if assignedPartitions is kafka:Error {
        log:printError("Failed to read the telemetry consumer's assigned partitions for resuming",
                'error = assignedPartitions);
        return;
    }
    kafka:Error? resumeResult = telemetryConsumer->resume(assignedPartitions);
    if resumeResult is kafka:Error {
        log:printError("Failed to resume telemetry consumer partitions", 'error = resumeResult);
        return;
    }
    log:printInfo("Resumed telemetry consumer partitions after alert publishing recovered",
            partitionCount = assignedPartitions.length());
}
