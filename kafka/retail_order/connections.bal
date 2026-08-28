import ballerinax/kafka;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

final mysql:Client customerDbClient = check new (host = customerDbHost, port = customerDbPort,
    user = customerDbUser, password = customerDbPassword, database = customerDbName);

final kafka:Producer orderEventProducer = check new (kafkaBootstrapServers, {
    acks: "all",
    enableIdempotence: true,
    clientId: "order-enrichment-producer"
});
