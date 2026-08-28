import ballerinax/kafka;

// Represents an order event consumed from the `orders.created` Kafka topic.
public type OrderEvent record {|
    string orderId;
    string customerId;
    decimal orderAmount;
    string currency;
    int itemCount;
    string channel;
|};

// Represents a Kafka consumer record whose value is bound to the `OrderEvent` type.
public type OrderEventConsumerRecord record {|
    *kafka:AnydataConsumerRecord;
    OrderEvent value;
|};

// Represents customer attributes fetched from the MySQL `customers` table.
public type CustomerInfo record {|
    string tier;
    string email;
    string country;
|};

// Represents an order event enriched with customer information, ready for publishing.
public type EnrichedOrder record {|
    string orderId;
    string customerId;
    decimal orderAmount;
    string currency;
    int itemCount;
    string channel;
    string customerTier;
    string customerEmail;
    string customerCountry;
|};

// Error raised when an order event payload is structurally invalid and must be
// routed straight to the DLQ without any retry attempts.
public type InvalidOrderEventError distinct error;

// Error raised when enrichment or publishing fails due to a transient issue
// and should be retried with exponential backoff before falling back to the DLQ.
public type RetryableProcessingError distinct error;
