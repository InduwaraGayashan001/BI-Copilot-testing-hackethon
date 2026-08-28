// Solace PubSub+ broker connection configuration.
configurable string solaceBrokerUrl = ?;
configurable string solaceVpnName = ?;

// Basic authentication credentials for the broker connection.
configurable string solaceUsername = ?;
configurable string solacePassword = ?;

// HTTP listener configuration.
configurable int servicePort = 8091;

// Payment instructions queue configuration.
configurable string paymentInstructionsQueueName = "PAYMENTS.INSTRUCTIONS.IN";
