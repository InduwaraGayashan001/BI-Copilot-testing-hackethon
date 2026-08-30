import ballerinax/rabbitmq;

final rabbitmq:Client rabbitmqClient = check new (rabbitmqHost, rabbitmqPort, connectionData = {
    username: rabbitmqUsername,
    password: rabbitmqPassword,
    virtualHost: rabbitmqVhost
});
