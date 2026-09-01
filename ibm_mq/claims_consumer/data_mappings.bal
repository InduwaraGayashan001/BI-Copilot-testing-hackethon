import ballerinax/ibm.ibmmq;

// Binds a raw IBM MQ message received on CLAIMS.INBOUND to a typed
// ClaimSubmission record.
function mapToClaimSubmission(ibmmq:Message claimMessage) returns ClaimSubmission|error {
    string payloadText = check string:fromBytes(claimMessage.payload);
    return payloadText.fromJsonStringWithType(ClaimSubmission);
}
