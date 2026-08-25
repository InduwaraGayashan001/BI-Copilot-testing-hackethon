// Solace PubSub+ broker connection configuration.
configurable string solaceBrokerUrl = ?;
configurable string solaceVpnName = ?;
configurable string solaceUsername = ?;
configurable string solacePassword = ?;

// HTTP listener configuration.
configurable int servicePort = 8090;

// Disruption queue consumption configuration.
configurable string disruptionsQueueName = "AIRLINE.OPS.DISRUPTIONS";
configurable int disruptionsTransportWindowSize = 10;
