import ballerina/jballerina.java;
import ballerina/log;

// Processes a received market data price tick. Any downstream processing
// failure is returned as an error so the caller can decide how to handle
// redelivery.
function processPriceTick(PriceTick priceTick) returns error? {
    log:printInfo("Processed price tick", instrumentId = priceTick.instrumentId,
            instrumentClass = priceTick.instrumentClass, price = priceTick.price,
            currency = priceTick.currency, timestamp = priceTick.timestamp);
}

// Decodes EBCDIC (IBM037/Cp037) encoded bytes into a Ballerina string.
// Ballerina's built-in string:fromBytes only supports UTF-8, so the
// mainframe's EBCDIC payload is decoded through the JVM's Cp037 charset via
// Java interop instead.
function decodeEbcdic(byte[] ebcdicBytes) returns string|error {
    handle charset = check charsetForName(java:fromString(EBCDIC_CHARSET_NAME));
    handle decodedString = newStringFromBytes(ebcdicBytes, charset);
    return java:toString(decodedString) ?: "";
}

function charsetForName(handle charsetName) returns handle|error = @java:Method {
    name: "forName",
    'class: "java.nio.charset.Charset"
} external;

function newStringFromBytes(byte[] bytes, handle charset) returns handle = @java:Constructor {
    'class: "java.lang.String",
    paramTypes: ["[B", "java.nio.charset.Charset"]
} external;

