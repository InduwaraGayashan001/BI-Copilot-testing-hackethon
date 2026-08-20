import ballerina/ai;

// A small in-memory knowledge base used by the support-article search tool.
final readonly & SupportArticle[] knowledgeBaseArticles = [
    {
        articleId: "KB-1001",
        title: "How to update your billing information",
        category: "billing",
        content: "To update your billing information, go to Account Settings > Billing, " +
            "select 'Update Payment Method', and enter your new card details. Changes take " +
            "effect immediately and apply to the next billing cycle."
    },
    {
        articleId: "KB-1002",
        title: "Understanding duplicate charges and refunds",
        category: "billing",
        content: "Duplicate charges are usually caused by a delayed payment confirmation. " +
            "If you see two charges for the same invoice, they are automatically reconciled " +
            "within 3-5 business days. If not, contact support with the invoice number to " +
            "request a manual refund."
    },
    {
        articleId: "KB-2001",
        title: "Troubleshooting login and connectivity issues",
        category: "technical",
        content: "If you are unable to log in or the application is unresponsive, clear your " +
            "browser cache, verify your internet connection, and confirm the service status " +
            "page shows no ongoing incidents. Persistent issues should be escalated with logs."
    },
    {
        articleId: "KB-2002",
        title: "Resolving API integration errors",
        category: "technical",
        content: "API errors such as 401 or 403 usually indicate an expired or invalid access " +
            "token. Regenerate your API key from the Developer Console and ensure the " +
            "Authorization header uses the 'Bearer' scheme."
    },
    {
        articleId: "KB-3001",
        title: "Resetting your account password",
        category: "account",
        content: "To reset your password, click 'Forgot Password' on the login page and follow " +
            "the link sent to your registered email. The link expires after 30 minutes."
    },
    {
        articleId: "KB-3002",
        title: "Managing account access and permissions",
        category: "account",
        content: "Account administrators can manage user roles and permissions from the " +
            "'Team Management' section. Only administrators can invite or remove members."
    },
    {
        articleId: "KB-9001",
        title: "General contact and escalation guidelines",
        category: "other",
        content: "For requests that do not fall under billing, technical, or account issues, " +
            "please provide as much detail as possible so the request can be routed to the " +
            "correct department."
    }
];

# Searches the support knowledge base using the ticket category and description to find the
# most relevant article.
#
# + category - the ticket category (billing, technical, account, or other)
# + description - the ticket description used to match relevant content
# + return - the best matching support article, or an error if none is found
@ai:AgentTool
isolated function searchSupportArticles(string category, string description) returns SupportArticleSearchResult|error {
    string normalizedDescription = description.toLowerAscii();

    SupportArticle[] categoryMatches = from SupportArticle article in knowledgeBaseArticles
        where article.category == category.toLowerAscii()
        select article;

    SupportArticle[] candidates = categoryMatches.length() > 0 ? categoryMatches : knowledgeBaseArticles;

    SupportArticle? bestMatch = ();
    int bestScore = -1;
    foreach SupportArticle article in candidates {
        string[] keywords = re `\s+`.split(article.title.toLowerAscii());
        int score = 0;
        foreach string keyword in keywords {
            boolean containsKeyword = normalizedDescription.includes(keyword);
            if containsKeyword {
                score += 1;
            }
        }
        if score > bestScore {
            bestScore = score;
            bestMatch = article;
        }
    }

    if bestMatch is () {
        return error("No matching support article was found");
    }

    SupportArticle matchedArticle = bestMatch;
    return {
        articleId: matchedArticle.articleId,
        title: matchedArticle.title,
        content: matchedArticle.content
    };
}

# Invokes the supportTicketAgent to triage a support ticket and returns a strongly typed result.
#
# + ticket - the support ticket to triage
# + return - the structured triage result, or an error if triage or parsing fails
function triageSupportTicket(SupportTicket ticket) returns TicketTriageResult|error {
    string triageQuery = "Triage the following support ticket and respond with ONLY a " +
        "single valid JSON object (no markdown, no extra text) that matches exactly this shape: " +
        "{\"category\": \"billing\"|\"technical\"|\"account\"|\"other\", \"urgency\": <integer 1-5>, " +
        "\"summary\": \"<short summary>\", \"suggestedReply\": \"<suggested customer reply>\", " +
        "\"confidence\": <number 0.0-1.0>, \"referencedArticleId\": \"<article id if used, otherwise omit>\", " +
        "\"referencedArticleTitle\": \"<article title if used, otherwise omit>\"}." + "\n\n" +
        "Ticket ID: " + ticket.id + "\n" +
        "Subject: " + ticket.subject + "\n" +
        "Description: " + ticket.description + "\n" +
        "Priority: " + ticket.priority + "\n" +
        "Language: " + ticket.language;

    string agentAnswer = check supportTicketAgent.run(triageQuery, sessionId = ticket.id);
    string jsonAnswer = extractJsonObject(agentAnswer);
    TicketTriageResult triageResult = check jsonAnswer.fromJsonStringWithType(TicketTriageResult);
    return triageResult;
}

# Extracts the JSON object substring from a raw text response, stripping any surrounding
# markdown code fences or extraneous text.
#
# + rawText - the raw text possibly containing a JSON object
# + return - the extracted JSON object string
function extractJsonObject(string rawText) returns string {
    int? startIndex = rawText.indexOf("{");
    int? endIndex = rawText.lastIndexOf("}");
    if startIndex is int && endIndex is int && endIndex >= startIndex {
        return rawText.substring(startIndex, endIndex + 1);
    }
    return rawText;
}
