import ballerina/http;
import ballerina/log;
import ballerinax/java.jms;

listener http:Listener shipmentTrackingControlListener = new (servicePort);

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
        check publishShipmentStatus(shipmentStatus);
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

service /shipments on shipmentTrackingControlListener {

    # Drains SHIPMENT.STATUS.REPLAY in configurable batches for the nightly reconciliation
    # window, committing the transacted session once per batch. Uses a non-blocking receive to
    # detect an empty queue rather than waiting out a timeout. Replay messages that fail
    # fixed-width parsing are routed to SHIPMENT.STATUS.INVALID; messages that have already been
    # attempted maxProcessingAttempts times are treated as poison messages and routed to
    # SHIPMENT.STATUS.DLQ instead of being retried further.
    #
    # + return - A summary of the reconciliation run, or an error response
    resource function post reconcile() returns ReconcileResult|http:InternalServerError {
        ReconcileResult|error result = drainReplayQueue();
        if result is error {
            return {
                body: {
                    message: "Failed to reconcile SHIPMENT.STATUS.REPLAY: " + result.message()
                }
            };
        }
        return result;
    }
}
