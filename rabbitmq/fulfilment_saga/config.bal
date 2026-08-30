configurable string rabbitmqHost = "localhost";
configurable int rabbitmqPort = 5672;
configurable string rabbitmqVhost = "/";
configurable string rabbitmqUsername = ?;
configurable string rabbitmqPassword = ?;

# Additional broker nodes the client can fail over to if the primary (`rabbitmqHost`:`rabbitmqPort`)
# is unreachable. Each entry is a "host:port" pair, e.g. ["broker2.example.com:5672"].
configurable string[] rabbitmqFailoverAddresses = [];

configurable int httpListenerPort = 8080;

# Maximum time (in milliseconds) to wait for the inventory reservation reply before
# responding to the caller with a 504 Gateway Timeout.
configurable decimal reservationReplyTimeoutMillis = 5000;

# Routing key / queue name that inventory reservation requests are published to.
const string INVENTORY_RESERVE_QUEUE = "inventory.reserve";
