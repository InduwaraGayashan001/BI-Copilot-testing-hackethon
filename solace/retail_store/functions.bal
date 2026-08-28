import ballerina/time;

# Checks whether a device telemetry message has already expired, based on the broker-calculated
# `expiration` timestamp (populated because the listener has `calculateMessageExpiration`
# enabled). An `expiration` of zero means the message never expires.
#
# + message - The device telemetry message received from the durable topic endpoint
# + return - `true` if the message carries a non-zero expiration timestamp that has already
# passed relative to the current time
function isExpired(DeviceTelemetryMessage message) returns boolean {
    int? expiration = message?.expiration;
    if expiration is () || expiration == 0 {
        return false;
    }

    time:Utc currentTime = time:utcNow();
    int currentTimeMillis = currentTime[0] * 1000;
    return expiration < currentTimeMillis;
}

