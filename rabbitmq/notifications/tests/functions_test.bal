import ballerina/test;
import ballerinax/rabbitmq;

@test:Config {}
function testMapUrgencyToPriorityLow() {
    int priority = mapUrgencyToPriority(URGENCY_LOW);
    test:assertEquals(priority, 0, msg = "Low urgency should map to priority 0");
}

@test:Config {}
function testMapUrgencyToPriorityNormal() {
    int priority = mapUrgencyToPriority(URGENCY_NORMAL);
    test:assertEquals(priority, 8, msg = "Normal urgency should map to priority 8");
}

@test:Config {}
function testMapUrgencyToPriorityHigh() {
    int priority = mapUrgencyToPriority(URGENCY_HIGH);
    test:assertEquals(priority, 16, msg = "High urgency should map to priority 16");
}

@test:Config {}
function testMapUrgencyToPriorityUrgent() {
    int priority = mapUrgencyToPriority(URGENCY_URGENT);
    test:assertEquals(priority, 31, msg = "Urgent urgency should map to priority 31 (max of the 0-31 range)");
}

@test:Config {}
function testExtractNotificationHeadersSucceeds() returns error? {
    rabbitmq:BasicProperties properties = {
        correlationId: "NOTIF-1",
        headers: {
            "x-tenant-id": "tenant-a",
            "x-notification-id": "NOTIF-1"
        }
    };
    [string, string] headerResult = check extractNotificationHeaders(properties);
    test:assertEquals(headerResult[0], "tenant-a", msg = "tenantId should be extracted from headers");
    test:assertEquals(headerResult[1], "NOTIF-1", msg = "notificationId should be extracted from headers");
}

@test:Config {}
function testExtractNotificationHeadersFailsWhenPropertiesAbsent() {
    [string, string]|error headerResult = extractNotificationHeaders(());
    test:assertTrue(headerResult is error, msg = "Extraction should fail when properties are absent");
}

@test:Config {}
function testExtractNotificationHeadersFailsWhenHeadersAbsent() {
    rabbitmq:BasicProperties properties = {correlationId: "NOTIF-2"};
    [string, string]|error headerResult = extractNotificationHeaders(properties);
    test:assertTrue(headerResult is error, msg = "Extraction should fail when headers are absent");
}

@test:Config {}
function testExtractNotificationHeadersFailsWhenTenantIdMissing() {
    rabbitmq:BasicProperties properties = {
        correlationId: "NOTIF-3",
        headers: {"x-notification-id": "NOTIF-3"}
    };
    [string, string]|error headerResult = extractNotificationHeaders(properties);
    test:assertTrue(headerResult is error, msg = "Extraction should fail when tenantId header is missing");
}

@test:Config {}
function testExtractNotificationHeadersFailsWhenNotificationIdMissing() {
    rabbitmq:BasicProperties properties = {
        correlationId: "NOTIF-4",
        headers: {"x-tenant-id": "tenant-a"}
    };
    [string, string]|error headerResult = extractNotificationHeaders(properties);
    test:assertTrue(headerResult is error, msg = "Extraction should fail when notificationId header is missing");
}
