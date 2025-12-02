CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactSales (

    # Savienojumi ar dimensijām
    SalesDateKey Date,
    CustomerID Int32,
    ProductID Int32,
    StoreID Int32,
    EmployeeID Int32,

    # "Grain"
    SalesOrderID Int32,
    SalesOrderDetailID Int32,

    # Mērījumi
    QuantitySold Int32,
    SalesRevenue Decimal(18, 2),
    DiscountAmount Decimal(18, 2),
    NumberOfTransactions UInt8,

    # Noderīgas vērtības
    UnitPrice Decimal(18, 2),
    UnitPriceDiscount Decimal(18, 4),
    LineTotal Decimal(18, 2),
    InsertedAt DateTime DEFAULT now()
) ENGINE = MergeTree PARTITION BY toYYYYMM(SalesDateKey)
ORDER BY
    (
        SalesDateKey,
        ProductID,
        CustomerID,
        StoreID,
        SalesOrderID,
        SalesOrderDetailID
    );