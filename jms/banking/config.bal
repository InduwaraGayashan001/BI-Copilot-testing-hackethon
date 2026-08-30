// ActiveMQ Artemis broker connection URL.
configurable string providerUrl = "tcp://localhost:61616";

// HTTP listener port for the REST API front door.
configurable int servicePort = 8080;

// Timeout, in milliseconds, to wait for a core-banking balance enquiry reply before failing with a 504.
configurable int enquiryTimeoutMillis = 5000;
