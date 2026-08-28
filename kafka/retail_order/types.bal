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
