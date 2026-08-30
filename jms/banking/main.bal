import ballerina/http;

service /banking on new http:Listener(servicePort) {

    # Accepts a transfer request and forwards it to the core-banking system over JMS.
    #
    # + transferRequest - The transfer details to be processed
    # + return - The accepted transfer confirmation, or an error response
    resource function post transfers(@http:Payload TransferRequest transferRequest)
            returns TransferAccepted|http:InternalServerError {
        error? sendResult = sendTransferRequest(transferRequest);
        if sendResult is error {
            return {
                body: {
                    message: "Failed to submit transfer request to core-banking system: " + sendResult.message()
                }
            };
        }
        return {
            transferId: transferRequest.transferId,
            status: "ACCEPTED"
        };
    }

    # Performs a synchronous balance enquiry against the core-banking system.
    #
    # + accountNumber - The account number to enquire on
    # + return - The account balance, a 504 on timeout, or an error response
    resource function get accounts/[string accountNumber]/balance()
            returns BalanceEnquiryResponse|http:GatewayTimeout|http:InternalServerError {
        BalanceEnquiryResponse|EnquiryTimeoutError|error result = enquireBalance(accountNumber);
        if result is EnquiryTimeoutError {
            http:GatewayTimeout gatewayTimeout = {
                body: {
                    message: result.message()
                }
            };
            return gatewayTimeout;
        }
        if result is error {
            http:InternalServerError internalServerError = {
                body: {
                    message: "Failed to perform balance enquiry: " + result.message()
                }
            };
            return internalServerError;
        }
        return result;
    }
}
