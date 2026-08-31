// Represents an order event published to the NATS JetStream subject orders.created
public type OrderEvent record {|
    string orderId;
    string customerId;
    decimal totalAmount;
    string currency;
    string createdAt;
|};

// Represents a transient failure while persisting an order, meaning the message
// should be redelivered rather than dropped.
public type TransientPersistenceError distinct error;

// Response returned by POST /orders/replay summarizing the replayed order event.
public type ReplayResult record {|
    string subject;
    OrderEvent orderEvent;
|};

// Response returned by POST /orders/purge confirming the stream was purged.
public type PurgeResult record {|
    string streamName;
    string status;
|};
