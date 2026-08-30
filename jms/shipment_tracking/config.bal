// ActiveMQ Artemis broker connection URL.
configurable string providerUrl = "tcp://localhost:61616";

// Queue that the legacy system publishes fixed-width shipment status events to.
configurable string shipmentStatusInQueue = "SHIPMENT.STATUS.IN";

// Queue that messages which fail fixed-width parsing are routed to.
configurable string shipmentStatusInvalidQueue = "SHIPMENT.STATUS.INVALID";
