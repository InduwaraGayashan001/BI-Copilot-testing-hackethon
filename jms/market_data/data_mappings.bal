// Converts a price tick into the map content used for the MARKET.DATA.NORMALISED map message.
function toNormalisedTickContent(PriceTick priceTick) returns map<anydata> => {
    instrumentId: priceTick.instrumentId,
    instrumentClass: priceTick.instrumentClass,
    bid: priceTick.bid,
    ask: priceTick.ask,
    lastTradedPrice: priceTick.lastTradedPrice,
    volume: priceTick.volume,
    tickTime: priceTick.tickTime
};

// Converts a spread alert into the map content used for the MARKET.DATA.ALERTS map message.
function toSpreadAlertContent(SpreadAlert spreadAlert) returns map<anydata> => {
    instrumentId: spreadAlert.instrumentId,
    instrumentClass: spreadAlert.instrumentClass,
    bid: spreadAlert.bid,
    ask: spreadAlert.ask,
    spread: spreadAlert.spread,
    threshold: spreadAlert.threshold,
    tickTime: spreadAlert.tickTime
};
