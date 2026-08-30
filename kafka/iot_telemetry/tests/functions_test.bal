import ballerina/test;

// Builds a valid window aggregate used as the baseline for threshold tests.
function buildWindowAggregate(string metric, decimal mean) returns WindowAggregate => {
    deviceId: "DEV-1",
    siteId: "SITE-1",
    metric: metric,
    unit: "C",
    windowStart: "2026-08-30T05:00:00Z",
    windowEnd: "2026-08-30T05:01:00Z",
    count: 5,
    min: mean - 5.0d,
    max: mean + 5.0d,
    mean: mean
};

@test:Config {}
function testBuildAlertIfThresholdCrossedReturnsAlertWhenMeanExceedsThreshold() {
    map<decimal> thresholdsByMetric = {"temperature": 80.0d};
    WindowAggregate windowAggregate = buildWindowAggregate("temperature", 85.0d);
    TelemetryAlert? telemetryAlert = buildAlertIfThresholdCrossed(windowAggregate, thresholdsByMetric);
    test:assertTrue(telemetryAlert is TelemetryAlert,
            msg = "An alert should be raised when the mean exceeds the configured threshold");
    if telemetryAlert is TelemetryAlert {
        test:assertEquals(telemetryAlert.deviceId, windowAggregate.deviceId, msg = "deviceId should be carried over");
        test:assertEquals(telemetryAlert.metric, windowAggregate.metric, msg = "metric should be carried over");
        test:assertEquals(telemetryAlert.mean, windowAggregate.mean, msg = "mean should be carried over");
        test:assertEquals(telemetryAlert.threshold, 80.0d, msg = "threshold should be the configured metric threshold");
    }
}

@test:Config {}
function testBuildAlertIfThresholdCrossedReturnsNilWhenMeanAtOrBelowThreshold() {
    map<decimal> thresholdsByMetric = {"temperature": 80.0d};
    WindowAggregate windowAggregate = buildWindowAggregate("temperature", 80.0d);
    TelemetryAlert? telemetryAlert = buildAlertIfThresholdCrossed(windowAggregate, thresholdsByMetric);
    test:assertTrue(telemetryAlert is (), msg = "No alert should be raised when the mean does not exceed the threshold");
}

@test:Config {}
function testBuildAlertIfThresholdCrossedReturnsNilForMetricWithoutConfiguredThreshold() {
    map<decimal> thresholdsByMetric = {"temperature": 80.0d};
    WindowAggregate windowAggregate = buildWindowAggregate("vibration", 1000.0d);
    TelemetryAlert? telemetryAlert = buildAlertIfThresholdCrossed(windowAggregate, thresholdsByMetric);
    test:assertTrue(telemetryAlert is (),
            msg = "No alert should be raised for a metric with no configured threshold");
}

@test:Config {}
function testAlertPublishHealthTransitionsOnlyOnceOnRepeatedFailures() {
    AlertPublishHealth alertPublishHealth = new ();
    boolean firstTransition = alertPublishHealth.markFailure();
    boolean secondTransition = alertPublishHealth.markFailure();
    test:assertTrue(firstTransition, msg = "The first failure should transition from healthy to failing");
    test:assertFalse(secondTransition, msg = "A repeated failure should not report another transition");
    test:assertTrue(alertPublishHealth.isFailing(), msg = "Health should report failing after a failure");
}

@test:Config {}
function testAlertPublishHealthTransitionsOnlyOnceOnRepeatedSuccesses() {
    AlertPublishHealth alertPublishHealth = new ();
    _ = alertPublishHealth.markFailure();
    boolean firstRecovery = alertPublishHealth.markSuccess();
    boolean secondRecovery = alertPublishHealth.markSuccess();
    test:assertTrue(firstRecovery, msg = "The first success after a failure should transition to healthy");
    test:assertFalse(secondRecovery, msg = "A repeated success should not report another transition");
    test:assertFalse(alertPublishHealth.isFailing(), msg = "Health should report healthy after recovery");
}

@test:Config {}
function testAlertPublishHealthStartsHealthy() {
    AlertPublishHealth alertPublishHealth = new ();
    test:assertFalse(alertPublishHealth.isFailing(), msg = "A freshly created health tracker should start healthy");
}

