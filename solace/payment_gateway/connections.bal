import ballerinax/solace;

final solace:MessageProducer paymentInstructionProducer = check new (solaceBrokerUrl,
    messageVpn = solaceVpnName,
    auth = {
        username: solaceUsername,
        password: solacePassword
    },
    transacted = true
);

// Transacted listener for the settlement consumer, consuming from PAYMENTS.INSTRUCTIONS.IN.
// NOTE: a solace:Listener's transacted session and a solace:MessageProducer's transacted session
// are always two independent underlying transacted sessions in this connector - a producer cannot
// be attached to a listener's session, and there is no single commit that spans both. See
// settlementProducer below and processSettlement in functions.bal for how the two transactions
// are coordinated to approximate "settle together".
listener solace:Listener settlementListener = check new (solaceBrokerUrl,
    messageVpn = solaceVpnName,
    auth = {
        username: solaceUsername,
        password: solacePassword
    },
    transacted = true
);

// Transacted producer used by the settlement consumer to publish onto PAYMENTS.SETTLEMENT.OUT and
// PAYMENTS.INSTRUCTIONS.DLQ. Its transaction is committed first (making the settlement/DLQ publish
// durable) and only then is the settlementListener's consume committed via caller->commit, so the
// source message is removed from PAYMENTS.INSTRUCTIONS.IN only after the outbound publish is
// confirmed durable. If the producer commit fails, both transactions are rolled back so the
// instruction is redelivered and reprocessed rather than lost; sequenceNumber-based duplicate
// suppression (see functions.bal) guards against the resulting at-least-once redelivery.
final solace:MessageProducer settlementProducer = check new (solaceBrokerUrl,
    messageVpn = solaceVpnName,
    auth = {
        username: solaceUsername,
        password: solacePassword
    },
    transacted = true
);
