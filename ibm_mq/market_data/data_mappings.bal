import ballerinax/ibm.ibmmq;

// JVM charset name used to decode the mainframe's EBCDIC payload bytes.
// Chosen: Cp037 (IBM037) - US/Canada EBCDIC, the most common mainframe code
// page and the one this upstream feed uses.
// Alternatives: Cp1047 (IBM1047, EBCDIC open-systems variant used by z/OS
// USS) and Cp500 (IBM500, EBCDIC for Belgium/Switzerland) - use one of these
// instead if the mainframe is configured for a different regional code page.
const string EBCDIC_CHARSET_NAME = "Cp037";

// Expected IBM MQ coded character set identifier on inbound price tick
// messages, matching the EBCDIC_CHARSET_NAME code page above.
// Chosen: ibmmq:MQCCSI_EBCDIC (CCSID 37) - the standard CCSID for IBM037.
// Alternatives: 500 (IBM500) and 1047 (IBM1047), matching the Cp500/Cp1047
// alternatives noted above.
final ibmmq:MessageCharset expectedCharacterSet = ibmmq:MQCCSI_EBCDIC;

// Expected numeric encoding on inbound price tick messages: normal
// (big-endian) integer, packed-decimal and IEEE float representation, which
// is the mainframe's native (big-endian) numeric encoding.
// Alternatives: swap in the *_REVERSED constants (ibmmq:MQENC_INTEGER_REVERSED,
// ibmmq:MQENC_DECIMAL_REVERSED, ibmmq:MQENC_FLOAT_IEEE_REVERSED) if the
// upstream system is little-endian, or ibmmq:MQENC_FLOAT_S390 for the
// System/390 floating-point format instead of IEEE.
final int expectedEncoding = ibmmq:MQENC_INTEGER_NORMAL | ibmmq:MQENC_DECIMAL_NORMAL | ibmmq:MQENC_FLOAT_IEEE_NORMAL;

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
// PriceTick record. The upstream mainframe feed puts the payload in EBCDIC,
// so the message descriptor's characterSet and encoding are checked against
// the expected mainframe values before the payload bytes are decoded using
// that code page rather than the default UTF-8 decoding.
function mapToPriceTick(ibmmq:Message priceTickMessage) returns PriceTick|error {
    if priceTickMessage.characterSet != expectedCharacterSet || priceTickMessage.encoding != expectedEncoding {
        return error("Unexpected character set or encoding on the price tick message", 
                characterSet = priceTickMessage.characterSet, encoding = priceTickMessage.encoding);
    }
    string payloadText = check decodeEbcdic(priceTickMessage.payload);
    return payloadText.fromJsonStringWithType(PriceTick);
}

