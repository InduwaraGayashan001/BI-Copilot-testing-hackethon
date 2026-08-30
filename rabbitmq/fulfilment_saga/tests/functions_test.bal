import ballerina/test;

function sampleReservationRequest(string orderId, string sku = "SKU-1", int quantity = 2) returns ReservationRequest => {
    orderId,
    warehouseId: "WH-1",
    items: [{sku, quantity}]
};

function sampleFulfilmentRequest(string orderId, string warehouseId = "WH-1") returns FulfilmentRequest => {
    orderId,
    warehouseId,
    items: [{sku: "SKU-1", quantity: 2}],
    shippingMethod: "STANDARD"
};

@test:Config {}
function testCheckStockSucceedsForInStockItems() returns error? {
    ReservationRequest reservationRequest = sampleReservationRequest("ORD-1");
    check checkStock(reservationRequest);
}

@test:Config {}
function testCheckStockFailsForOutOfStockSku() {
    ReservationRequest reservationRequest = sampleReservationRequest("ORD-2", sku = "OUT-OF-STOCK");
    error? stockCheckResult = checkStock(reservationRequest);
    test:assertTrue(stockCheckResult is error, msg = "Stock check should fail for the OUT-OF-STOCK sentinel SKU");
    if stockCheckResult is error {
        test:assertEquals(stockCheckResult.message(), "Insufficient stock for SKU OUT-OF-STOCK (order ORD-2)",
                msg = "Error message should describe the shortfall");
    }
}

@test:Config {}
function testCheckStockFailsForNonPositiveQuantity() {
    ReservationRequest reservationRequest = sampleReservationRequest("ORD-3", quantity = 0);
    error? stockCheckResult = checkStock(reservationRequest);
    test:assertTrue(stockCheckResult is error, msg = "Stock check should fail for a non-positive quantity");
}

@test:Config {}
function testChargePaymentSucceedsForNormalOrder() returns error? {
    FulfilmentRequest fulfilmentRequest = sampleFulfilmentRequest("ORD-4");
    check chargePayment(fulfilmentRequest);
}

@test:Config {}
function testChargePaymentFailsForPaymentFailSentinel() {
    FulfilmentRequest fulfilmentRequest = sampleFulfilmentRequest("ORD-5", warehouseId = "PAYMENT-FAIL");
    error? paymentResult = chargePayment(fulfilmentRequest);
    test:assertTrue(paymentResult is error, msg = "Payment should fail for the PAYMENT-FAIL sentinel warehouse ID");
    if paymentResult is error {
        test:assertEquals(paymentResult.message(), "Payment charge failed for order ORD-5",
                msg = "Error message should describe the payment failure");
    }
}

@test:Config {}
function testDispatchShippingSucceedsForNormalOrder() returns error? {
    FulfilmentRequest fulfilmentRequest = sampleFulfilmentRequest("ORD-6");
    check dispatchShipping(fulfilmentRequest);
}

@test:Config {}
function testDispatchShippingFailsForShippingFailSentinel() {
    FulfilmentRequest fulfilmentRequest = sampleFulfilmentRequest("ORD-7", warehouseId = "SHIPPING-FAIL");
    error? shippingResult = dispatchShipping(fulfilmentRequest);
    test:assertTrue(shippingResult is error, msg = "Shipping should fail for the SHIPPING-FAIL sentinel warehouse ID");
    if shippingResult is error {
        test:assertEquals(shippingResult.message(), "Shipping dispatch failed for order ORD-7",
                msg = "Error message should describe the shipping failure");
    }
}
