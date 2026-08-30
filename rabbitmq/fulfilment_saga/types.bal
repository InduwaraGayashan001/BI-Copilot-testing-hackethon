# Represents a single line item within a fulfilment request.
public type OrderItem record {|
    string sku;
    int quantity;
|};

# Represents an incoming order fulfilment request.
public type FulfilmentRequest record {|
    string orderId;
    string warehouseId;
    OrderItem[] items;
    string shippingMethod;
|};

# Represents the reservation outcome returned by the inventory service.
public type ReservationResult record {|
    string orderId;
    boolean reserved;
    string message?;
|};

# Generic error message body used by error responses.
public type ErrorMessage record {|
    string message;
|};
