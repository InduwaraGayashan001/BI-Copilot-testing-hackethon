import ballerina/log;
import ballerina/time;
import ballerinax/kafka;

// Atomically checks whether the given paymentId was already processed within
// its TTL window and, if not, marks it as processed. Returns `true` when the
// caller should proceed with settlement (i.e., it is not a duplicate), and
// `false` when the record is a duplicate and should be skipped. Expired
// entries are lazily evicted as they are encountered.
isolated function markProcessedIfAbsent(string paymentId) returns boolean {
    lock {
        int nowEpochSeconds = time:utcNow()[0];
        int? existingExpiry = processedPaymentIds[paymentId];
        if existingExpiry is int && existingExpiry > nowEpochSeconds {
            return false;
        }
        processedPaymentIds[paymentId] = nowEpochSeconds + <int>duplicatePaymentTtlSeconds;
        return true;
    }
}

// Looks up the current duplicate-suppression cache entry for a paymentId
// without mutating it, used by the reconciliation status endpoint.
isolated function getProcessedEntry(string paymentId) returns int? {
    lock {
        int? existingExpiry = processedPaymentIds[paymentId];
        if existingExpiry is int {
            int nowEpochSeconds = time:utcNow()[0];
            if existingExpiry > nowEpochSeconds {
                return existingExpiry;
            }
        }
        return ();
    }
}

// Seeks the reconciliation consumer to the offsets corresponding to
// `fromTimestamp` for every partition of `payments.authorized`, using the
// connector's `offsetsForTimes` lookup. `offsetsForTimes` legitimately returns
// no offset for a partition when no message exists at or after that
// timestamp on it; only in that documented null case do we fall back to the
// partition's earliest offset, rather than inventing a substitute API.
function replayFromTimestamp(int fromTimestamp) returns ReplayResult|error {
    kafka:TopicPartition[] topicPartitions =
        check reconciliationConsumer->getTopicPartitions(PAYMENTS_AUTHORIZED_TOPIC);

    kafka:TopicPartitionTimestamp[] lookupRequests = from kafka:TopicPartition topicPartition in topicPartitions
        select [topicPartition, fromTimestamp];
    kafka:TopicPartitionOffset[] offsetsForTimes = check reconciliationConsumer->offsetsForTimes(lookupRequests);

    check reconciliationConsumer->assign(topicPartitions);

    PartitionReplayInfo[] partitionReplayInfos = [];
    foreach kafka:TopicPartitionOffset topicPartitionOffset in offsetsForTimes {
        kafka:TopicPartition topicPartition = topicPartitionOffset[0];
        kafka:OffsetAndTimestamp? offsetAndTimestamp = topicPartitionOffset[1];

        int seekedOffset;
        boolean fellBackToEarliest;
        if offsetAndTimestamp is kafka:OffsetAndTimestamp {
            seekedOffset = offsetAndTimestamp.offset;
            fellBackToEarliest = false;
        } else {
            kafka:PartitionOffset[] beginningOffsets =
                check reconciliationConsumer->getBeginningOffsets([topicPartition]);
            seekedOffset = beginningOffsets[0].offset;
            fellBackToEarliest = true;
            log:printWarn("No offset found for timestamp, falling back to earliest",
                    topic = topicPartition.topic, partition = topicPartition.partition,
                    fromTimestamp = fromTimestamp);
        }

        check reconciliationConsumer->seek({partition: topicPartition, offset: seekedOffset});
        partitionReplayInfos.push({
            topic: topicPartition.topic,
            partition: topicPartition.partition,
            seekedOffset: seekedOffset,
            fellBackToEarliest: fellBackToEarliest
        });
    }

    PaymentAuthorizedConsumerRecord[] polledRecords = check reconciliationConsumer->poll(5);
    foreach PaymentAuthorizedConsumerRecord polledRecord in polledRecords {
        error? redriveResult = redrivePaymentAuthorizedRecord(polledRecord);
        if redriveResult is error {
            log:printError("Failed to redrive payment authorized event during replay",
                    'error = redriveResult, paymentId = polledRecord.value.paymentId);
        }
    }

    boolean usedTimestampLookup = partitionReplayInfos.some(info => !info.fellBackToEarliest);
    return {
        fromTimestamp: fromTimestamp,
        usedTimestampLookup: usedTimestampLookup,
        recordsReplayed: polledRecords.length(),
        partitions: partitionReplayInfos
    };
}

// Re-drives a single record found during replay: applies the same duplicate
// suppression and publishes the settlement transactionally, but does not
// commit any offset since the reconciliation consumer is not part of the
// service's consumer group.
function redrivePaymentAuthorizedRecord(PaymentAuthorizedConsumerRecord polledRecord) returns error? {
    PaymentAuthorized paymentAuthorized = polledRecord.value;

    boolean shouldSettle = markProcessedIfAbsent(paymentAuthorized.paymentId);
    if !shouldSettle {
        log:printInfo("Skipping duplicate payment authorized event during replay",
                paymentId = paymentAuthorized.paymentId);
        return;
    }

    PaymentSettlement paymentSettlement = toPaymentSettlement(paymentAuthorized);
    transaction {
        check paymentSettlementProducer->send({
            topic: PAYMENTS_SETTLEMENT_TOPIC,
            key: paymentSettlement.paymentId.toBytes(),
            value: paymentSettlement.toJson().toJsonString().toBytes()
        });
        check commit;
    }
    return;
}

// Builds the health snapshot for the reconciliation consumer's currently
// assigned partitions: the last committed offset and the current lag, with
// the lag derived exactly as (end offset - committed offset) using the
// connector's own offset APIs rather than an estimate.
function getSettlementHealth() returns SettlementHealth|error {
    kafka:TopicPartition[] assignedPartitions =
        check reconciliationConsumer->getTopicPartitions(PAYMENTS_AUTHORIZED_TOPIC);
    kafka:PartitionOffset[] endOffsets = check reconciliationConsumer->getEndOffsets(assignedPartitions);

    map<int> endOffsetByPartition = {};
    foreach kafka:PartitionOffset endOffset in endOffsets {
        string partitionKey = endOffset.partition.topic + ":" + endOffset.partition.partition.toString();
        endOffsetByPartition[partitionKey] = endOffset.offset;
    }

    PartitionLag[] partitionLags = [];
    foreach kafka:TopicPartition topicPartition in assignedPartitions {
        kafka:PartitionOffset|error? committedOffsetResult = reconciliationConsumer->getCommittedOffset(topicPartition);

        int? committedOffset = ();
        if committedOffsetResult is kafka:PartitionOffset {
            committedOffset = committedOffsetResult.offset;
        }

        string partitionKey = topicPartition.topic + ":" + topicPartition.partition.toString();
        int endOffset = endOffsetByPartition.get(partitionKey);
        int lag = committedOffset is int ? endOffset - committedOffset : endOffset;

        partitionLags.push({
            topic: topicPartition.topic,
            partition: topicPartition.partition,
            committedOffset: committedOffset,
            endOffset: endOffset,
            lag: lag
        });
    }

    return {partitions: partitionLags};
}
