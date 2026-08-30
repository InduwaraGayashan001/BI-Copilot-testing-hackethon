// Column boundaries (0-based, end-exclusive) for the fixed-width shipment status layout sent by
// the legacy system on SHIPMENT.STATUS.IN. See types.bal for the full layout documentation.
const int SHIPMENT_ID_START = 0;
const int SHIPMENT_ID_END = 20;
const int CARRIER_CODE_START = 20;
const int CARRIER_CODE_END = 30;
const int STATUS_START = 30;
const int STATUS_END = 45;
const int LOCATION_CODE_START = 45;
const int LOCATION_CODE_END = 55;
const int STATUS_AT_START = 55;
const int STATUS_AT_END = 75;
const int EXCEPTION_REASON_START = 75;
const int EXCEPTION_REASON_END = 155;

// Parses a single fixed-width line received from the legacy system into a ShipmentStatus
// record. Returns an error describing the failure when the line is too short or a required
// field is blank.
function parseFixedWidthShipmentStatus(string fixedWidthLine) returns ShipmentStatus|error {
    if fixedWidthLine.length() < EXCEPTION_REASON_START {
        return error(string `Fixed-width line too short: expected at least ${EXCEPTION_REASON_START} characters, got ${fixedWidthLine.length()}`);
    }

    string shipmentId = fixedWidthLine.substring(SHIPMENT_ID_START, SHIPMENT_ID_END).trim();
    string carrierCode = fixedWidthLine.substring(CARRIER_CODE_START, CARRIER_CODE_END).trim();
    string status = fixedWidthLine.substring(STATUS_START, STATUS_END).trim();
    string locationCode = fixedWidthLine.substring(LOCATION_CODE_START, LOCATION_CODE_END).trim();
    string statusAt = fixedWidthLine.substring(STATUS_AT_START, STATUS_AT_END).trim();

    if shipmentId.length() == 0 {
        return error("Missing required field: shipmentId");
    }
    if carrierCode.length() == 0 {
        return error("Missing required field: carrierCode");
    }
    if status.length() == 0 {
        return error("Missing required field: status");
    }
    if locationCode.length() == 0 {
        return error("Missing required field: locationCode");
    }
    if statusAt.length() == 0 {
        return error("Missing required field: statusAt");
    }

    string exceptionReason = fixedWidthLine.length() > EXCEPTION_REASON_START
        ? fixedWidthLine.substring(EXCEPTION_REASON_START, int:min(EXCEPTION_REASON_END, fixedWidthLine.length())).trim()
        : "";

    if exceptionReason.length() == 0 {
        return {shipmentId, carrierCode, status, locationCode, statusAt};
    }
    return {shipmentId, carrierCode, status, locationCode, statusAt, exceptionReason};
}

// Converts a shipment status into the map content used for the SHIPMENT.STATUS.OUT /
// SHIPMENT.EXCEPTIONS map messages.
function toShipmentStatusContent(ShipmentStatus shipmentStatus) returns map<anydata> => {
    shipmentId: shipmentStatus.shipmentId,
    carrierCode: shipmentStatus.carrierCode,
    status: shipmentStatus.status,
    locationCode: shipmentStatus.locationCode,
    statusAt: shipmentStatus.statusAt,
    exceptionReason: shipmentStatus?.exceptionReason
};

// Resolves the destination queue name for a shipment status event based on its carrier code,
// falling back to the given default queue when the carrier is not present in the routing map.
function resolveCarrierQueue(string carrierCode, map<string> carrierQueues, string defaultQueue) returns string {
    return carrierQueues[carrierCode] ?: defaultQueue;
}
