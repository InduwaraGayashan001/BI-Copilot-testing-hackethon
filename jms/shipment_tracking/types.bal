// Shipment status event parsed from the fixed-width text sent by the legacy system on
// SHIPMENT.STATUS.IN.
//
// Fixed-width layout (column positions, 0-based, end-exclusive):
//   shipmentId      [0, 20)   20 chars
//   carrierCode     [20, 30)  10 chars
//   status          [30, 45)  15 chars
//   locationCode    [45, 55)  10 chars
//   statusAt        [55, 75)  20 chars (ISO 8601 timestamp)
//   exceptionReason [75, 155) 80 chars (optional, blank when not applicable)
public type ShipmentStatus record {|
    string shipmentId;
    string carrierCode;
    string status;
    string locationCode;
    string statusAt;
    string exceptionReason?;
|};

// Result of draining and reconciling messages from SHIPMENT.STATUS.REPLAY.
public type ReconcileResult record {|
    int batchesProcessed;
    int messagesProcessed;
    int messagesInvalid;
    int messagesPoisoned;
|};
