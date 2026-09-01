import ballerinax/ibm.ibmmq;

final ibmmq:QueueManager paymentQueueManager = check new (
    name = queueManagerName,
    host = host,
    port = port,
    channel = channel,
    userID = userID,
    password = password
);

final ibmmq:Queue paymentInstructionsQueue = check paymentQueueManager.accessQueue(
    paymentInstructionsQueueName,
    ibmmq:MQOO_OUTPUT
);
