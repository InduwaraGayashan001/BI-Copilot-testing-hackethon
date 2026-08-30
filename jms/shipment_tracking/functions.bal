import ballerina/log;
import ballerinax/java.jms;

// Forwards a message that failed fixed-width parsing to SHIPMENT.STATUS.INVALID, attaching the
// parse error as a message property so downstream consumers can inspect the failure reason
// without having to re-parse the payload.
function forwardInvalidShipmentStatus(jms:Message originalMessage, error parseError) returns error? {
    jms:TextMessage invalidMessage = {
        content: originalMessage is jms:TextMessage ? originalMessage.content : "",
        properties: {
            "parseError": parseError.message()
        }
    };
    check shipmentStatusInvalidProducer->send(invalidMessage);
}

// Publishes a successfully parsed shipment status event as a map message, routing it by carrier
// to either an exception queue or a regular status-out queue. Both carrier and status are set as
// message properties so downstream consumers can filter using JMS message selectors.
function publishShipmentStatus(ShipmentStatus shipmentStatus) returns error? {
    map<anydata> content = toShipmentStatusContent(shipmentStatus);
    string? exceptionReason = shipmentStatus?.exceptionReason;
    string carrierCode = shipmentStatus.carrierCode;

    if exceptionReason is string {
        string destinationQueue = resolveCarrierQueue(carrierCode, carrierExceptionQueues, defaultShipmentExceptionQueue);
        jms:MapMessage exceptionMessage = {
            content,
            priority: exceptionPriority,
            expiration: exceptionTtlMillis,
            properties: {
                carrier: carrierCode,
                status: shipmentStatus.status
            }
        };
        check shipmentStatusRoutedProducer->sendTo({'type: jms:QUEUE, name: destinationQueue}, exceptionMessage);
        return;
    }

    string destinationQueue = resolveCarrierQueue(carrierCode, carrierStatusOutQueues, defaultShipmentStatusOutQueue);
    jms:MapMessage acceptedMessage = {
        content,
        properties: {
            carrier: carrierCode,
            status: shipmentStatus.status
        }
    };
    check shipmentStatusRoutedProducer->sendTo({'type: jms:QUEUE, name: destinationQueue}, acceptedMessage);
}

// Routes a replay message that has exceeded maxProcessingAttempts to the dead-letter queue,
// preserving the original text content.
function routeToReplayDlq(jms:Message message) returns error? {
    jms:TextMessage dlqMessage = {
        content: message is jms:TextMessage ? message.content : ""
    };
    check shipmentStatusDlqProducer->sendTo({'type: jms:QUEUE, name: shipmentStatusDlqQueue}, dlqMessage);
}

// Tracks processing attempts per replay message id across reconciliation runs, since the
// connector only exposes a boolean `redelivered` flag and not a numeric delivery count. Once a
// message's attempts exceed maxProcessingAttempts it is treated as a poison message.
isolated class ReplayAttemptTracker {
    private final map<int> attemptsByMessageId = {};

    isolated function recordAttempt(string messageId) returns int {
        lock {
            int attempts = (self.attemptsByMessageId[messageId] ?: 0) + 1;
            self.attemptsByMessageId[messageId] = attempts;
            return attempts;
        }
    }

    isolated function clear(string messageId) {
        lock {
            _ = self.attemptsByMessageId.removeIfHasKey(messageId);
        }
    }
}

final ReplayAttemptTracker replayAttemptTracker = new;

// Drains SHIPMENT.STATUS.REPLAY in batches of reconcileBatchSize, committing the transacted
// session once per batch. Uses receiveNoWait() rather than a timed receive so an empty queue is
// detected immediately instead of waiting out a timeout. Stops once a batch comes back empty.
function drainReplayQueue() returns ReconcileResult|error {
    int batchesProcessed = 0;
    int messagesProcessed = 0;
    int messagesInvalid = 0;
    int messagesPoisoned = 0;

    boolean queueDrained = false;
    while !queueDrained {
        int messagesInBatch = 0;
        foreach int i in 0 ..< reconcileBatchSize {
            jms:Message|error? received = shipmentStatusReplayConsumer->receiveNoWait();
            if received is error {
                return received;
            }
            if received is () {
                queueDrained = true;
                break;
            }

            jms:Message replayMessage = received;
            messagesInBatch += 1;

            string? messageId = replayMessage?.messageId;
            if messageId is string {
                int attempts = replayAttemptTracker.recordAttempt(messageId);
                if attempts > maxProcessingAttempts {
                    log:printWarn("Replay message exceeded max processing attempts, routing to DLQ",
                            messageId = messageId, attempts = attempts);
                    check routeToReplayDlq(replayMessage);
                    replayAttemptTracker.clear(messageId);
                    messagesPoisoned += 1;
                    continue;
                }
            }

            error? processResult = processReplayMessage(replayMessage);
            if processResult is error {
                messagesInvalid += 1;
            } else {
                messagesProcessed += 1;
            }
            if messageId is string {
                replayAttemptTracker.clear(messageId);
            }
        }

        if messagesInBatch > 0 {
            check shipmentStatusReplaySession->'commit();
            batchesProcessed += 1;
        }
    }

    return {batchesProcessed, messagesProcessed, messagesInvalid, messagesPoisoned};
}

// Parses and publishes a single replay message using the same logic as the live consumer.
// Messages that fail parsing are routed to SHIPMENT.STATUS.INVALID and treated as processed
// (not poisoned) since retrying a permanently malformed message will never succeed.
function processReplayMessage(jms:Message replayMessage) returns error? {
    if replayMessage !is jms:TextMessage {
        log:printWarn("Received non-text replay message, routing to invalid queue");
        check forwardInvalidShipmentStatus(replayMessage, error("Message is not a text message"));
        return error("Non-text replay message");
    }

    string fixedWidthLine = replayMessage.content;
    ShipmentStatus|error shipmentStatus = parseFixedWidthShipmentStatus(fixedWidthLine);
    if shipmentStatus is error {
        log:printWarn("Failed to parse fixed-width replay message, routing to invalid queue",
                'error = shipmentStatus);
        check forwardInvalidShipmentStatus(replayMessage, shipmentStatus);
        return error("Failed to parse replay message", shipmentStatus);
    }

    processShipmentStatus(shipmentStatus);
    check publishShipmentStatus(shipmentStatus);
}
