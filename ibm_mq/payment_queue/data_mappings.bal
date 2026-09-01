import ballerinax/ibm.ibmmq;

// IBM MQ persistence value indicating the message survives queue manager restarts.
const int MQ_PERSISTENCE_PERSISTENT = 1;

// Maps a PaymentInstruction into an IBM MQ message, setting the correlation ID,
// priority, persistence, expiry and custom message properties carrying the
// scheme and originating branch.
function mapToPaymentInstructionMessage(PaymentInstruction paymentInstruction, byte[] correlationId) returns ibmmq:Message => {
    payload: paymentInstruction.toJsonString().toBytes(),
    correlationId: correlationId,
    priority: 5,
    persistence: MQ_PERSISTENCE_PERSISTENT,
    expiry: 6000,
    properties: {
        "scheme": {value: paymentInstruction.scheme},
        "originatingBranch": {value: paymentInstruction.originatingBranch}
    }
};
