import ballerina/http;
import ballerinax/rabbitmq;

function init() returns error? {
    check initClaimsTopology();
}

service /claims on new http:Listener(httpListenerPort) {

    # Accepts a claim submission and publishes it to the claims topic exchange.
    #
    # + claimSubmission - the claim submission payload
    # + return - 202 Accepted with the routing key used, or an error response
    resource function post .(ClaimSubmission claimSubmission) returns http:Accepted|http:InternalServerError {
        string routingKey = buildRoutingKey(claimSubmission.claimType, claimSubmission.priority);

        rabbitmq:BasicProperties properties = {
            correlationId: claimSubmission.claimId,
            contentType: "application/json"
        };

        rabbitmq:AnydataMessage claimMessage = {
            content: claimSubmission,
            routingKey: routingKey,
            exchange: CLAIMS_EXCHANGE,
            properties: properties
        };

        rabbitmq:Error? publishResult = rabbitmqClient->publishMessage(claimMessage);
        if publishResult is rabbitmq:Error {
            return <http:InternalServerError>{
                body: {message: "Failed to publish claim submission: " + publishResult.message()}
            };
        }

        ClaimAccepted claimAccepted = {
            claimId: claimSubmission.claimId,
            routingKey: routingKey
        };
        return <http:Accepted>{body: claimAccepted};
    }
}
