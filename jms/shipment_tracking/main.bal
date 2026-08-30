import ballerina/log;
import ballerinax/java.jms;

service "shipment-status-consumer" on shipmentStatusListener {

    // Parses the fixed-width payload into a ShipmentStatus record. Messages that fail to parse
    // are forwarded to SHIPMENT.STATUS.INVALID with the parse error attached as a property, then
    // acknowledged so they do not redeliver forever.
    remote function onMessage(jms:Message message, jms:Caller caller) returns error? {
        if message !is jms:TextMessage {
            log:printWarn("Received non-text shipment status message, routing to invalid queue");
            check forwardInvalidShipmentStatus(message, error("Message is not a text message"));
            check caller->acknowledge(message);
            return;
        }

        string fixedWidthLine = message.content;
        ShipmentStatus|error shipmentStatus = parseFixedWidthShipmentStatus(fixedWidthLine);
        if shipmentStatus is error {
            log:printWarn("Failed to parse fixed-width shipment status, routing to invalid queue",
                    'error = shipmentStatus);
            check forwardInvalidShipmentStatus(message, shipmentStatus);
            check caller->acknowledge(message);
            return;
        }

        processShipmentStatus(shipmentStatus);
        check caller->acknowledge(message);
    }
}

// Handles a successfully parsed shipment status event. Replace with the actual downstream
// processing logic.
function processShipmentStatus(ShipmentStatus shipmentStatus) {
    log:printInfo("Shipment status processed",
            shipmentId = shipmentStatus.shipmentId,
            carrierCode = shipmentStatus.carrierCode,
            status = shipmentStatus.status,
            locationCode = shipmentStatus.locationCode);
}
