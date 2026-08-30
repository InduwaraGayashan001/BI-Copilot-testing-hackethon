// Request payload for initiating a core-banking transfer.
public type TransferRequest record {|
    string transferId;
    string debitAccount;
    string creditAccount;
    decimal amount;
    string currency;
    string valueDate;
|};

// Response returned once the transfer request has been accepted for processing.
public type TransferAccepted record {|
    string transferId;
    string status;
|};

// Audit trail record persisted alongside every transfer request sent to the core-banking system.
public type TransferAuditRecord record {|
    readonly string transferId;
    string debitAccount;
    string creditAccount;
    decimal amount;
    string currency;
    string valueDate;
    string submittedAt;
|};

// Tracks the lifecycle of a transfer request awaiting a core-banking response.
public type PendingTransferStatus "PENDING"|"COMPLETED"|"FAILED";

public type PendingTransfer record {|
    string transferId;
    PendingTransferStatus status;
    string? coreStatus;
    string? coreMessage;
|};

// Response payload for the account balance enquiry.
public type BalanceEnquiryResponse record {|
    string accountNumber;
    decimal availableBalance;
    string currency;
|};

// Request sent to the core-banking system over CORE.ENQUIRY.REQUEST.
type CoreEnquiryRequest record {|
    string accountNumber;
|};

// Reply expected from the core-banking system on the temporary reply queue.
type CoreEnquiryResponse record {|
    string accountNumber;
    decimal availableBalance;
    string currency;
|};
