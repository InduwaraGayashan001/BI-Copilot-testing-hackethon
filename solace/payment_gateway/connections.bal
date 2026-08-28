import ballerinax/solace;

final solace:MessageProducer paymentInstructionProducer = check new (solaceBrokerUrl,
    messageVpn = solaceVpnName,
    auth = {
        username: solaceUsername,
        password: solacePassword
    },
    transacted = true
);
