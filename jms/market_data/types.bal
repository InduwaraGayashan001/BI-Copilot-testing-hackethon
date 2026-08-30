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
