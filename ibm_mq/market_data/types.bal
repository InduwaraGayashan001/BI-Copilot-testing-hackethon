// Represents a market data price tick received on the MARKET.DATA.PRICES topic.
public type PriceTick record {|
    string instrumentId;
    string instrumentClass;
    decimal price;
    string currency;
    string timestamp;
|};

