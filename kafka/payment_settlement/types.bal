import ballerinax/kafka;

// Represents an authorized payment event consumed from the `payments.authorized` Kafka topic.
public type PaymentAuthorized record {|
    string paymentId;
    string orderId;
    string merchantId;
    decimal amount;
    string currency;
|};

// Represents a Kafka consumer record whose value is bound to the `PaymentAuthorized` type.
public type PaymentAuthorizedConsumerRecord record {|
    *kafka:AnydataConsumerRecord;
    PaymentAuthorized value;
|};

// Represents a settlement event ready for publishing to the `payments.settlement` topic.
public type PaymentSettlement record {|
    string paymentId;
    string orderId;
    string merchantId;
    decimal amount;
    string currency;
|};
