import ballerinax/ibm.ibmmq;

final ibmmq:QueueManager marketDataQueueManager = check new (
    name = queueManagerName,
    host = host,
    port = port,
    channel = channel,
    userID = userID,
    password = password
);

// Message selector limiting delivery to the configured instrument classes.
final string instrumentClassSelector = buildInstrumentClassSelector(instrumentClasses);

listener ibmmq:Listener marketDataListener = check new (
    name = queueManagerName,
    host = host,
    port = port,
    channel = channel,
    userID = userID,
    password = password
);

