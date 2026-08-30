import ballerina/time;
import ballerinax/java.jms;

// Tracks the number of ticks currently being processed (received but not yet acknowledged), so
// the unsubscribe endpoint can refuse with a conflict while messages are still in flight.
isolated class InFlightTracker {
    private int count = 0;

    isolated function increment() {
        lock {
            self.count += 1;
        }
    }

    isolated function decrement() {
        lock {
            self.count -= 1;
        }
    }

    isolated function get() returns int {
        lock {
            return self.count;
        }
    }
}

final InFlightTracker inFlightTracker = new;

// Tracks whether the market-data consumer service is currently attached to the listener, so
// pause/resume requests are idempotent and unsubscribe can tell whether it needs to detach first.
isolated class ConsumerState {
    private boolean attached = true;
    private boolean unsubscribed = false;

    isolated function markDetached() {
        lock {
            self.attached = false;
        }
    }

    isolated function markAttached() {
        lock {
            self.attached = true;
        }
    }

    isolated function markUnsubscribed() {
        lock {
            self.unsubscribed = true;
        }
    }

    isolated function isAttached() returns boolean {
        lock {
            return self.attached;
        }
    }

    isolated function isUnsubscribed() returns boolean {
        lock {
            return self.unsubscribed;
        }
    }
}

final ConsumerState consumerState = new;

// Resolves the bid-ask spread threshold configured for the given instrument class, falling back
// to defaultSpreadThreshold when the class has no specific entry.
function resolveSpreadThreshold(string instrumentClass) returns decimal {
    if spreadThresholdsByClass.hasKey(instrumentClass) {
        return spreadThresholdsByClass.get(instrumentClass);
    }
    return defaultSpreadThreshold;
}

// Republishes a price tick to MARKET.DATA.NORMALISED as a map message.
function publishNormalisedTick(PriceTick priceTick) returns error? {
    jms:MapMessage mapMessage = {
        content: toNormalisedTickContent(priceTick),
        priority: normalisedTickPriority
    };
    check normalisedTickProducer->send(mapMessage);
}

// When the tick's bid-ask spread crosses the configured threshold for its instrument class,
// republishes an alert to MARKET.DATA.ALERTS as a non-persistent, short-lived, high-priority map
// message so it overtakes normalised ticks in delivery.
function publishSpreadAlertIfNeeded(PriceTick priceTick) returns error? {
    decimal spread = priceTick.ask - priceTick.bid;
    decimal threshold = resolveSpreadThreshold(priceTick.instrumentClass);
    if spread <= threshold {
        return;
    }

    SpreadAlert spreadAlert = {
        instrumentId: priceTick.instrumentId,
        instrumentClass: priceTick.instrumentClass,
        bid: priceTick.bid,
        ask: priceTick.ask,
        spread,
        threshold,
        tickTime: priceTick.tickTime
    };

    jms:MapMessage mapMessage = {
        content: toSpreadAlertContent(spreadAlert),
        deliveryMode: 1,
        expiration: currentTimeMillis() + alertTimeToLiveMillis,
        priority: alertPriority
    };
    check spreadAlertProducer->send(mapMessage);
}

function currentTimeMillis() returns int {
    [int, decimal] [epochSeconds, fractionOfSecond] = time:utcNow();
    decimal totalMillis = (<decimal>epochSeconds + fractionOfSecond) * 1000;
    return <int>totalMillis;
}
