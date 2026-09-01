// Represents a payment instruction submitted by a client to be placed on the
// PAYMENT.INSTRUCTIONS queue.
public type PaymentInstruction record {|
    string instructionId;
    string debtorAccount;
    string creditorAccount;
    decimal amount;
    string currency;
    string scheme;
    string originatingBranch;
    string? remittanceInformation;
|};

// Response returned when a payment instruction has been successfully queued.
public type PaymentAccepted record {|
    string instructionId;
    string correlationId;
    string status;
|};

// Generic error response payload.
public type ErrorDetails record {|
    string message;
    string timestamp;
|};
