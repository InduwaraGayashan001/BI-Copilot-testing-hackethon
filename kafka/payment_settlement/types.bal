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

// Reports whether a given paymentId has been settled recently, based on the
// in-memory duplicate-suppression cache. Since there is no persistent store,
// this only reflects payments settled within the configured TTL window.
public type SettlementStatus record {|
    string paymentId;
    boolean settled;
    int? expiryEpochSeconds;
|};

// Request body for POST /settlement/replay.
public type ReplayRequest record {|
    int fromTimestamp;
|};

// Summary of a replay operation.
public type ReplayResult record {|
    int fromTimestamp;
    boolean usedTimestampLookup;
    int recordsReplayed;
    PartitionReplayInfo[] partitions;
|};

// Per-partition detail of where the replay actually seeked to.
public type PartitionReplayInfo record {|
    string topic;
    int partition;
    int seekedOffset;
    boolean fellBackToEarliest;
|};

// Lag information for a single assigned partition.
public type PartitionLag record {|
    string topic;
    int partition;
    int? committedOffset;
    int endOffset;
    int lag;
|};

// Health snapshot of the reconciliation consumer.
public type SettlementHealth record {|
    PartitionLag[] partitions;
|};
