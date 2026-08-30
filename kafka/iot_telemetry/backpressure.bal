// Tracks whether publishing to `iot.alerts` is currently failing. Used to
// decide when the telemetry consumer's partitions should be paused (alert
// publishing is failing, so we stop pulling in more data that would need to
// raise more alerts) and when they should be resumed (alert publishing has
// recovered). All state is guarded by this class's own lock.
public isolated class AlertPublishHealth {
    private boolean failing = false;

    // Records an alert publish failure. Returns `true` only on the transition
    // from healthy to failing, so the caller pauses partitions exactly once.
    public isolated function markFailure() returns boolean {
        lock {
            boolean wasFailing = self.failing;
            self.failing = true;
            return !wasFailing;
        }
    }

    // Records an alert publish success. Returns `true` only on the transition
    // from failing to healthy, so the caller resumes partitions exactly once.
    public isolated function markSuccess() returns boolean {
        lock {
            boolean wasFailing = self.failing;
            self.failing = false;
            return wasFailing;
        }
    }

    // Reports whether alert publishing is currently considered to be failing.
    public isolated function isFailing() returns boolean {
        lock {
            return self.failing;
        }
    }
}
