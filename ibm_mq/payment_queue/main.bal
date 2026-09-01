import ballerina/http;
import ballerina/time;
import ballerina/uuid;
import ballerinax/ibm.ibmmq;

service /payments on new http:Listener(servicePort) {

    # Accepts a payment instruction and puts it onto the PAYMENT.INSTRUCTIONS queue.
    #
    # + paymentInstruction - the payment instruction to be queued
    # + return - the acceptance details on success, or an error response on failure
    resource function post instructions(@http:Payload PaymentInstruction paymentInstruction)
            returns PaymentAccepted|http:InternalServerError {
        string correlationId = uuid:createType1AsString();
        ibmmq:Message message = mapToPaymentInstructionMessage(paymentInstruction, correlationId.toBytes());

        ibmmq:Error? putResult = paymentInstructionsQueue->put(message);
        if putResult is ibmmq:Error {
            ErrorDetails errorDetails = {
                message: putResult.message(),
                timestamp: time:utcToString(time:utcNow())
            };
            return <http:InternalServerError>{body: errorDetails};
        }

        return {
            instructionId: paymentInstruction.instructionId,
            correlationId: correlationId,
            status: "ACCEPTED"
        };
    }
}
