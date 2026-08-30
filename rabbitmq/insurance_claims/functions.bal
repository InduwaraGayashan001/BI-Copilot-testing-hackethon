# Derives the routing key for a claim submission from its claim type and priority.
# The claim type is normalized to lower case and any whitespace is replaced with a dot
# to build a `claim.<type>.<priority>` style routing key, e.g. `claim.auto.high`.
#
# + claimType - the type of the claim (e.g. "auto", "health", "property")
# + priority - the priority of the claim (e.g. "high", "low")
# + return - the derived routing key
function buildRoutingKey(string claimType, string priority) returns string {
    string normalizedType = claimType.trim().toLowerAscii();
    string normalizedPriority = priority.trim().toLowerAscii();
    string:RegExp whitespacePattern = re `\s+`;
    normalizedType = whitespacePattern.replaceAll(normalizedType, "-");
    normalizedPriority = whitespacePattern.replaceAll(normalizedPriority, "-");
    return string `claim.${normalizedType}.${normalizedPriority}`;
}
