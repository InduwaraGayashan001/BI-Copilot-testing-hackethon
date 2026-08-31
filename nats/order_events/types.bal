// Represents an order event published to the NATS JetStream subject orders.created
public type OrderEvent record {|
    string orderId;
    string customerId;
    decimal totalAmount;
    string currency;
    string createdAt;
|};
