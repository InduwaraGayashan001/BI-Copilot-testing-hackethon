import ballerinax/java.jms;

// Forwards a message that failed fixed-width parsing to SHIPMENT.STATUS.INVALID, attaching the
// parse error as a message property so downstream consumers can inspect the failure reason
// without having to re-parse the payload.
function forwardInvalidShipmentStatus(jms:Message originalMessage, error parseError) returns error? {
    jms:TextMessage invalidMessage = {
        content: originalMessage is jms:TextMessage ? originalMessage.content : "",
        properties: {
            "parseError": parseError.message()
        }
    };
    check shipmentStatusInvalidProducer->send(invalidMessage);
}
