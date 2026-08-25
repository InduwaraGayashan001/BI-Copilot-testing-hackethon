import ballerinax/solace;

final solace:MessageProducer solaceProducer = check new (solaceBrokerUrl,
    messageVpn = solaceVpnName,
    auth = {
        username: solaceUsername,
        password: solacePassword
    }
);
