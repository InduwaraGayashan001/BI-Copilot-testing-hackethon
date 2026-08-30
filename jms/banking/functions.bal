import ballerina/time;
import ballerina/uuid;
import ballerinax/java.jms;

// In-memory audit trail of transfer requests that have been submitted to the core-banking system.
final table<TransferAuditRecord> key(transferId) auditTrail = table [];

// Registers a pending transfer awaiting correlation, tracks its outcome once the core-banking
// system replies, and matches inbound replies against transfers that are still pending.
isolated class PendingTransferRegistry {
    private final map<PendingTransfer> pendingTransfers = {};

    isolated function register(string transferId) {
        lock {
            self.pendingTransfers[transferId] = {
                transferId,
                status: "PENDING",
                coreStatus: (),
                coreMessage: ()
            };
        }
    }

    // Attempts to correlate an inbound reply to a pending transfer. Returns true only when a
    // matching pending transfer was found and updated, false if the reply is unmatched.
    isolated function correlate(string transferId, string coreStatus, string? coreMessage) returns boolean {
        lock {
            if !self.pendingTransfers.hasKey(transferId) {
                return false;
            }
            PendingTransferStatus resolvedStatus = coreStatus == "SUCCESS" ? "COMPLETED" : "FAILED";
            self.pendingTransfers[transferId] = {
                transferId,
                status: resolvedStatus,
                coreStatus,
                coreMessage
            };
            return true;
        }
    }

    isolated function get(string transferId) returns PendingTransfer? {
        lock {
            return self.pendingTransfers[transferId].clone();
        }
    }
}

final PendingTransferRegistry pendingTransferRegistry = new;

// Sends the transfer request to the core-banking system as a JMS text message and writes the
// corresponding audit record, committing both together, and rolling back if either step fails.
function sendTransferRequest(TransferRequest transferRequest) returns error? {
    pendingTransferRegistry.register(transferRequest.transferId);

    string payload = transferRequest.toJsonString();
    jms:TextMessage textMessage = {
        content: payload,
        correlationId: transferRequest.transferId,
        jmsType: "CORE_TRANSFER"
    };

    error? sendError = coreTransferRequestProducer->send(textMessage);
    if sendError is error {
        error? rollbackError = jmsTransferSession->'rollback();
        if rollbackError is error {
            return error(string `Failed to send transfer request and rollback failed: ${sendError.message()}, ${rollbackError.message()}`);
        }
        return sendError;
    }

    TransferAuditRecord auditRecord = {
        transferId: transferRequest.transferId,
        debitAccount: transferRequest.debitAccount,
        creditAccount: transferRequest.creditAccount,
        amount: transferRequest.amount,
        currency: transferRequest.currency,
        valueDate: transferRequest.valueDate,
        submittedAt: time:utcToString(time:utcNow())
    };

    error? auditError = writeAuditRecord(auditRecord);
    if auditError is error {
        error? rollbackError = jmsTransferSession->'rollback();
        if rollbackError is error {
            return error(string `Failed to write audit record and rollback failed: ${auditError.message()}, ${rollbackError.message()}`);
        }
        return auditError;
    }

    error? commitError = jmsTransferSession->'commit();
    if commitError is error {
        return commitError;
    }
}

// Writes the audit record for a submitted transfer. Modelled as a function that can fail so the
// caller can roll back the transacted JMS session if persistence fails.
function writeAuditRecord(TransferAuditRecord auditRecord) returns error? {
    auditTrail.add(auditRecord);
}

// Error used to signal that no reply was received from the core-banking system before the
// configured enquiry timeout elapsed.
type EnquiryTimeoutError distinct error;

// Performs a synchronous request-reply balance enquiry against the core-banking system over
// CORE.ENQUIRY.REQUEST, using a temporary reply queue and a selector on the correlation id.
// The temporary queue consumer is always closed, whether the enquiry succeeds, times out, or fails.
function enquireBalance(string accountNumber) returns BalanceEnquiryResponse|EnquiryTimeoutError|error {
    string correlationId = uuid:createRandomUuid();
    jms:Destination replyQueue = {
        'type: jms:TEMPORARY_QUEUE,
        name: string `ENQUIRY.REPLY.${correlationId}`
    };

    jms:MessageConsumer|jms:Error replyConsumer = jmsEnquirySession.createConsumer(
        destination = replyQueue,
        messageSelector = string `JMSCorrelationID = '${correlationId}'`
    );
    if replyConsumer is jms:Error {
        return replyConsumer;
    }

    CoreEnquiryRequest enquiryRequest = {accountNumber};
    jms:TextMessage requestMessage = {
        content: enquiryRequest.toJsonString(),
        correlationId,
        replyTo: replyQueue,
        jmsType: "CORE_ENQUIRY"
    };

    error? sendError = coreEnquiryRequestProducer->send(requestMessage);
    if sendError is error {
        error? closeError = replyConsumer->close();
        if closeError is error {
            return error(string `Failed to send enquiry request and close reply consumer failed: ${sendError.message()}, ${closeError.message()}`);
        }
        return sendError;
    }

    jms:Message|jms:Error? reply = replyConsumer->receive(enquiryTimeoutMillis);
    error? closeError = replyConsumer->close();
    if closeError is error {
        return closeError;
    }

    if reply is jms:Error {
        return reply;
    }
    if reply is () {
        return error EnquiryTimeoutError(string `Timed out waiting for balance enquiry reply for account ${accountNumber}`);
    }
    if reply !is jms:TextMessage {
        return error(string `Unexpected non-text balance enquiry reply for account ${accountNumber}`);
    }

    CoreEnquiryResponse coreResponse = check reply.content.fromJsonStringWithType(CoreEnquiryResponse);
    return {
        accountNumber: coreResponse.accountNumber,
        availableBalance: coreResponse.availableBalance,
        currency: coreResponse.currency
    };
}
