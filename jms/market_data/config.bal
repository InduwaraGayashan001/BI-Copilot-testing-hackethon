// ActiveMQ Artemis broker connection URL.
configurable string providerUrl = "tcp://localhost:61616";

// JMS client identifier used to establish the durable topic subscription. Must remain the same
// across restarts so the broker can identify and resume this subscription.
configurable string clientId = ?;

// Durable subscriber name used together with the client id to uniquely identify the durable
// subscription on the broker.
configurable string subscriberName = ?;

// Instrument classes this subscriber is interested in. Used to build a JMS message selector so
// only ticks for these instrument classes are delivered to this durable subscription.
configurable string[] instrumentClasses = ["EQUITY", "FX", "FIXED_INCOME"];

// HTTP listener port for the pause/resume/unsubscribe control endpoints.
configurable int servicePort = 8080;

// Per-instrument-class bid-ask spread thresholds. When a tick's spread (ask - bid) crosses the
// configured threshold for its instrument class, an alert is republished to MARKET.DATA.ALERTS.
// Instrument classes not present here fall back to defaultSpreadThreshold.
configurable map<decimal> spreadThresholdsByClass = {
    "EQUITY": 0.05,
    "FX": 0.0010,
    "FIXED_INCOME": 0.02
};

// Fallback spread threshold for instrument classes not listed in spreadThresholdsByClass.
configurable decimal defaultSpreadThreshold = 0.05;

// Time-to-live, in milliseconds, applied to alert messages published to MARKET.DATA.ALERTS.
// Kept short since an alert is only useful while it is fresh.
configurable int alertTimeToLiveMillis = 5000;

// JMS priority (0-9, higher is more urgent) used for alert messages. Must be greater than
// normalisedTickPriority so alerts are delivered ahead of normalised ticks.
configurable int alertPriority = 7;

// JMS priority (0-9) used for normalised tick messages published to MARKET.DATA.NORMALISED.
configurable int normalisedTickPriority = 4;
