import ballerina/http;
import ballerina/time;
import ballerina/uuid;
import ballerinax/ibm.ibmmq;

service /payments on new http:Listener(servicePort) {

    # Accepts a payment instruction, puts it onto the PAYMENT.INSTRUCTIONS queue, and
    # synchronously waits for the matching response on PAYMENT.RESPONSES.
    #
    # + paymentInstruction - the payment instruction to be queued
    # + return - the payment response on success, or an error response on failure
    resource function post instructions(@http:Payload PaymentInstruction paymentInstruction)
            returns PaymentResponse|http:ServiceUnavailable|http:NotFound|http:InternalServerError {
        byte[] correlationId = uuid:createType1AsString().toBytes();
        ibmmq:Message message = mapToPaymentInstructionMessage(paymentInstruction, correlationId);

        ibmmq:Error? putResult = paymentInstructionsQueue->put(message);
        if putResult is ibmmq:Error {
            return mapToHttpError(putResult);
        }

        ibmmq:GetMessageOptions getMessageOptions = {
            waitInterval: <int>paymentResponseWaitInterval * 1000,
            matchOptions: {correlationId: correlationId}
        };
        ibmmq:Message|ibmmq:Error? getResult = paymentResponsesQueue->get(getMessageOptions = getMessageOptions);
        if getResult is ibmmq:Error {
            return mapToHttpError(getResult);
        }
        if getResult is () {
            ErrorDetails errorDetails = {
                message: "No response received for the payment instruction within the wait interval",
                reasonCode: (),
                timestamp: time:utcToString(time:utcNow())
            };
            return <http:ServiceUnavailable>{body: errorDetails};
        }

        PaymentResponse|error paymentResponse = mapToPaymentResponse(getResult, paymentInstruction.instructionId);
        if paymentResponse is error {
            ErrorDetails errorDetails = {
                message: paymentResponse.message(),
                reasonCode: (),
                timestamp: time:utcToString(time:utcNow())
            };
            return <http:InternalServerError>{body: errorDetails};
        }
        return paymentResponse;
    }
}
