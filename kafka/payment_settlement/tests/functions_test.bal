import ballerina/test;

// Builds a valid authorized payment event used as the baseline for tests.
function buildValidPaymentAuthorized(string paymentId) returns PaymentAuthorized => {
    paymentId: paymentId,
    orderId: "ORD-2001",
    merchantId: "MERCH-77",
    amount: 249.50d,
    currency: "USD"
};

@test:Config {}
function testMarkProcessedIfAbsentAllowsFirstOccurrence() {
    boolean shouldSettle = markProcessedIfAbsent("PAY-DEDUP-1");
    test:assertTrue(shouldSettle, msg = "The first occurrence of a paymentId should be allowed to settle");
}

@test:Config {dependsOn: [testMarkProcessedIfAbsentAllowsFirstOccurrence]}
function testMarkProcessedIfAbsentRejectsDuplicateWithinTtl() {
    boolean firstAttempt = markProcessedIfAbsent("PAY-DEDUP-2");
    boolean secondAttempt = markProcessedIfAbsent("PAY-DEDUP-2");
    test:assertTrue(firstAttempt, msg = "The first occurrence should be allowed to settle");
    test:assertFalse(secondAttempt, msg = "A duplicate paymentId within the TTL window should be rejected");
}

@test:Config {dependsOn: [testMarkProcessedIfAbsentRejectsDuplicateWithinTtl]}
function testGetProcessedEntryReturnsExpiryForSettledPayment() {
    string paymentId = "PAY-DEDUP-3";
    boolean shouldSettle = markProcessedIfAbsent(paymentId);
    int? expiry = getProcessedEntry(paymentId);
    test:assertTrue(shouldSettle, msg = "The first occurrence should be allowed to settle");
    test:assertTrue(expiry is int, msg = "A settled paymentId should have a recorded expiry");
}

@test:Config {}
function testGetProcessedEntryReturnsNilForUnknownPayment() {
    int? expiry = getProcessedEntry("PAY-NEVER-SEEN");
    test:assertTrue(expiry is (), msg = "An unknown paymentId should not have a cache entry");
}

@test:Config {}
function testToPaymentSettlementMapsAllFieldsFromEvent() {
    PaymentAuthorized paymentAuthorized = buildValidPaymentAuthorized("PAY-3001");
    PaymentSettlement paymentSettlement = toPaymentSettlement(paymentAuthorized);
    test:assertEquals(paymentSettlement.paymentId, paymentAuthorized.paymentId,
            msg = "paymentId should be carried over");
    test:assertEquals(paymentSettlement.orderId, paymentAuthorized.orderId, msg = "orderId should be carried over");
    test:assertEquals(paymentSettlement.merchantId, paymentAuthorized.merchantId,
            msg = "merchantId should be carried over");
    test:assertEquals(paymentSettlement.amount, paymentAuthorized.amount, msg = "amount should be carried over");
    test:assertEquals(paymentSettlement.currency, paymentAuthorized.currency,
            msg = "currency should be carried over");
}
