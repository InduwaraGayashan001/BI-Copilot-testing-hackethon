# Represents an insurance claim submission.
public type ClaimSubmission record {|
    string claimId;
    string policyNumber;
    string claimType;
    decimal claimAmount;
    string incidentDate;
    string priority;
|};

# Represents the response returned after a claim is successfully published.
public type ClaimAccepted record {|
    string claimId;
    string routingKey;
|};
