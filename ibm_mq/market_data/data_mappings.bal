import ballerinax/ibm.ibmmq;

// Builds a JMS-style message selector that restricts delivery to messages
// whose instrumentClass property is one of the configured instrument
// classes.
function buildInstrumentClassSelector(string[] configuredInstrumentClasses) returns string {
    string[] quotedInstrumentClasses = from string instrumentClass in configuredInstrumentClasses
        select string `'${instrumentClass}'`;
    string instrumentClassList = string:'join(", ", ...quotedInstrumentClasses);
    return string `instrumentClass IN (${instrumentClassList})`;
}

// Binds a raw IBM MQ message received on MARKET.DATA.PRICES to a typed
// PriceTick record.
function mapToPriceTick(ibmmq:Message priceTickMessage) returns PriceTick|error {
    string payloadText = check string:fromBytes(priceTickMessage.payload);
    return payloadText.fromJsonStringWithType(PriceTick);
}

