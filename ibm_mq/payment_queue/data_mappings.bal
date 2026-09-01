import ballerinax/ibm.ibmmq;

// IBM MQ persistence value indicating the message survives queue manager restarts.
const int MQ_PERSISTENCE_PERSISTENT = 1;

// Builds the RFH2 header carrying routing name-value pairs derived from the
// payment instruction so downstream routers can direct the message without
// parsing the payload.
function buildRoutingHeader(PaymentInstruction paymentInstruction) returns ibmmq:MQRFH2 => {
    fieldValues: table [
        {folder: "routing", 'field: "scheme", value: paymentInstruction.scheme},
        {folder: "routing", 'field: "originatingBranch", value: paymentInstruction.originatingBranch}
    ]
};

// Maps a PaymentInstruction into an IBM MQ message, setting the correlation ID,
// priority, persistence, expiry, custom message properties carrying the
// scheme and originating branch, and an RFH2 header carrying routing
// name-value pairs.
function mapToPaymentInstructionMessage(PaymentInstruction paymentInstruction, byte[] correlationId) returns ibmmq:Message => {
    payload: paymentInstruction.toJsonString().toBytes(),
    correlationId: correlationId,
    priority: 5,
    persistence: MQ_PERSISTENCE_PERSISTENT,
    expiry: 6000,
    properties: {
        "scheme": {value: paymentInstruction.scheme},
        "originatingBranch": {value: paymentInstruction.originatingBranch}
    },
    headers: [buildRoutingHeader(paymentInstruction)]
};

// Maps a raw IBM MQ message read from PAYMENT.RESPONSES into a PaymentResponse.
function mapToPaymentResponse(ibmmq:Message responseMessage, string instructionId) returns PaymentResponse|error {
    byte[]? correlationIdBytes = responseMessage.correlationId;
    string correlationId = correlationIdBytes is byte[] ? check string:fromBytes(correlationIdBytes) : "";
    string status = check string:fromBytes(responseMessage.payload);
    return {
        instructionId,
        correlationId,
        status
    };
}
