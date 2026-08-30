import ballerinax/rabbitmq;

final rabbitmq:Client rabbitmqClient = check new (rabbitmqHost, rabbitmqPort, connectionData = {
    username: rabbitmqUsername,
    password: rabbitmqPassword,
    virtualHost: rabbitmqVhost
});

function initClaimsTopology() returns error? {
    check rabbitmqClient->exchangeDeclare(CLAIMS_EXCHANGE, rabbitmq:TOPIC_EXCHANGE, {durable: true});

    check rabbitmqClient->queueDeclare(CLAIMS_AUTO_QUEUE, {durable: true});
    check rabbitmqClient->queueDeclare(CLAIMS_HEALTH_QUEUE, {durable: true});
    check rabbitmqClient->queueDeclare(CLAIMS_PROPERTY_QUEUE, {durable: true});

    check rabbitmqClient->queueBind(CLAIMS_AUTO_QUEUE, CLAIMS_EXCHANGE, CLAIMS_AUTO_BINDING_KEY);
    check rabbitmqClient->queueBind(CLAIMS_HEALTH_QUEUE, CLAIMS_EXCHANGE, CLAIMS_HEALTH_BINDING_KEY);
    check rabbitmqClient->queueBind(CLAIMS_PROPERTY_QUEUE, CLAIMS_EXCHANGE, CLAIMS_PROPERTY_BINDING_KEY);
}
