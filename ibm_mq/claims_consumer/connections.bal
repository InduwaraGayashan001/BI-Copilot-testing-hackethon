import ballerinax/ibm.ibmmq;

listener ibmmq:Listener claimsInboundListener = check new (
    name = queueManagerName,
    host = host,
    port = port,
    channel = channel,
    userID = userID,
    password = password
);
