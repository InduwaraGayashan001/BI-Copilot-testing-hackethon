// Market data price tick received from the MARKET.DATA.PRICES topic.
public type PriceTick record {|
    string instrumentId;
    string instrumentClass;
    decimal bid;
    decimal ask;
    decimal lastTradedPrice;
    decimal volume;
    string tickTime;
|};

// Alert raised when a tick's bid-ask spread crosses the configured threshold for its
// instrument class, republished to MARKET.DATA.ALERTS.
public type SpreadAlert record {|
    string instrumentId;
    string instrumentClass;
    decimal bid;
    decimal ask;
    decimal spread;
    decimal threshold;
    string tickTime;
|};

// Response confirming a pause/resume/unsubscribe control operation.
public type ControlResponse record {|
    string state;
    string message;
|};
