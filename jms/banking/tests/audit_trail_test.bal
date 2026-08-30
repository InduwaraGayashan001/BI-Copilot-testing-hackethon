import ballerina/test;

@test:Config {}
function testWriteAuditRecordAddsEntryToAuditTrail() returns error? {
    TransferAuditRecord auditRecord = {
        transferId: "audit-txn-001",
        debitAccount: "ACC1001",
        creditAccount: "ACC2002",
        amount: 100.50,
        currency: "USD",
        valueDate: "2026-08-30",
        submittedAt: "2026-08-30T17:00:00Z"
    };

    check writeAuditRecord(auditRecord);

    TransferAuditRecord storedRecord = auditTrail.get("audit-txn-001");
    test:assertEquals(storedRecord.debitAccount, "ACC1001");
    test:assertEquals(storedRecord.creditAccount, "ACC2002");
    test:assertEquals(storedRecord.amount, <decimal>100.50);
    test:assertEquals(storedRecord.currency, "USD");
}
