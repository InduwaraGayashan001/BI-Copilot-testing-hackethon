import ballerinax/java.jms;

// Sends the transfer request to the core-banking system as a JMS text message.
function sendTransferRequest(TransferRequest transferRequest) returns error? {
    string payload = transferRequest.toJsonString();
    jms:TextMessage textMessage = {
        content: payload,
        correlationId: transferRequest.transferId,
        jmsType: "CORE_TRANSFER"
    };
    check coreTransferRequestProducer->send(textMessage);
}
