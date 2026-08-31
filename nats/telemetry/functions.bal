import ballerina/log;
import ballerina/time;

// Running count of readings dropped for being older than stalenessWindowSeconds.
int staleReadingCount = 0;

// Returns true if the reading's readingAt timestamp is older than stalenessWindowSeconds
// relative to now. A readingAt value that cannot be parsed is treated as stale so it does
// not get processed as if it were fresh.
function isStaleReading(DeviceReading deviceReading) returns boolean {
    time:Utc|time:Error readingTime = time:utcFromString(deviceReading.readingAt);
    if readingTime is time:Error {
        log:printWarn(string `Unable to parse readingAt '${deviceReading.readingAt}', treating as stale`,
                'error = readingTime);
        return true;
    }
    time:Utc now = time:utcNow();
    decimal ageSeconds = time:utcDiffSeconds(now, readingTime);
    return ageSeconds > stalenessWindowSeconds;
}

// Drops a stale reading: counts it and logs the drop for observability.
function dropStaleReading(DeviceReading deviceReading) {
    lock {
        staleReadingCount += 1;
    }
    log:printWarn(string `Dropping stale reading for ${deviceReading.region}.${deviceReading.siteId}.${deviceReading.deviceType} `
            + string `metric=${deviceReading.metric} readingAt=${deviceReading.readingAt} (staleCount=${staleReadingCount})`);
}

// Processes a fresh (non-stale) reading. Replace with the actual downstream handling logic.
function processReading(DeviceReading deviceReading) {
    log:printInfo(string `Reading ${deviceReading.metric}=${deviceReading.value} from `
            + string `${deviceReading.region}/${deviceReading.siteId}/${deviceReading.deviceType} at ${deviceReading.readingAt}`);
}

