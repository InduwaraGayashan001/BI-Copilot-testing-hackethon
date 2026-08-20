import ballerina/http;

service /support on new http:Listener(9090) {

    # Accepts a support ticket, triages it using the supportTicketAgent, and returns a
    # strongly typed triage response.
    #
    # + ticket - the strongly typed support ticket to triage
    # + return - the triage response, or an error if triage fails
    resource function post tickets(@http:Payload SupportTicket ticket) returns TicketTriageResponse|http:InternalServerError {
        TicketTriageResult|error triageResult = triageSupportTicket(ticket);

        if triageResult is error {
            return <http:InternalServerError>{
                body: {message: "Failed to triage support ticket: " + triageResult.message()}
            };
        }

        return {
            ticketId: ticket.id,
            category: triageResult.category,
            urgency: triageResult.urgency,
            summary: triageResult.summary,
            suggestedReply: triageResult.suggestedReply,
            confidence: triageResult.confidence,
            referencedArticleId: triageResult?.referencedArticleId,
            referencedArticleTitle: triageResult?.referencedArticleTitle
        };
    }
}
