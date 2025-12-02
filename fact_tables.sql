-- Fact tables
-- Sales Facts
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactSales (

    -- Savienojumi ar dimensijām
    SalesDateKey Date,
    CustomerID Int32,
    ProductID Int32,
    StoreID Int32,
    EmployeeID Int32,

    -- "Grain"
    SalesOrderID Int32,
    SalesOrderDetailID Int32,

    -- Mērījumi
    QuantitySold Int32,
    SalesRevenue Decimal(18, 2),
    DiscountAmount Decimal(18, 2),
    NumberOfTransactions UInt8,

    -- Noderīgas vērtības
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

-- Purchase Facts
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactPurchases (

    -- Savienojumi ar dimensijām
    PurchaseDateKey Date,
    CustomerID Int32,
    ProductID Int32,
    StoreID Int32,
    EmployeeID Int32,

    -- "Grain"
    PurchaseOrderID Int32,
    PurchaseOrderDetailID Int32,

    -- Mērījumi
    QuantityBought Int32,
    PurchaseAmount Decimal(18, 2),
    DiscountAmount Decimal(18, 2),
    NumberOfTransactions UInt8,

    -- Noderīgas vērtības
    UnitPrice Decimal(18, 2),
    UnitPriceDiscount Decimal(18, 4),
    LineTotal Decimal(18, 2),
    InsertedAt DateTime DEFAULT now()
) ENGINE = MergeTree PARTITION BY toYYYYMM(PurchaseDateKey)
ORDER BY
    (
        PurchaseDateKey,
        ProductID,
        CustomerID,
        StoreID,
        PurchaseOrderID,
        PurchaseOrderDetailID
    );

-- Inventory Facts
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactInvertory (

    -- Savienojumi ar dimensijām
    InventoryDateKey Date,
    ProductKey UInt32,
    StoreKey UInt32,
    WarehouseKey UInt32,

    -- Mērījumi
    quantityOnHand UInt32,
    StockAging UInt32,
    ReorderLevel UInt32,
    SafetyStockLevels UInt32,

    -- "Grain"
    SnapshotCreatedDateTime DateTime,
    ETLBatchID String
) ENGINE = MergeTree PARTITION BY toYYYYMM(InventoryDateKey)
ORDER BY
    (InventoryDateKey, ProductKey, WarehouseKey, StoreKey);

-- Production Facts
CREATE TABLE IF NOT EXISTS FactProduction (

    -- Surogātatslēga
    ProductionRunID UInt64,

    -- Savienojumi ar Dimensijām
    ProductionDateKey Date,
    ProductKey UInt32,
    SupervisorKey UInt32,

    -- Mērījumi
    UnitsProduced UInt32,
    ProductionTimeHours Decimal(10, 2),
    ScrapRatePercent Decimal(5, 2),
    DefectCount UInt32,

    -- "Grain"
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree PARTITION BY toYYYYMM(ProductionDateKey)
ORDER BY
    (ProductionDateKey, ProductKey, ProductionRunID);

-- Employee Sale Facts
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactEmployeeSales (

    -- Savienojumi ar dimensijām
    SalesDateKey Date,
    EmployeeKey UInt32,
    StoreKey UInt32,
    SalesTerritoryKey UInt32,

    -- Mērījumi
    SalesAmount Decimal(18, 2),
    SalesTarget Decimal(18, 2),
    TargetAttainment Decimal(10, 4),
    CustomerContactsCount UInt32,

    -- "Grain"
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree PARTITION BY toYYYYMM(SalesDateKey)
ORDER BY
    (
        SalesDateKey,
        EmployeeKey,
        StoreKey,
        SalesTerritoryKey
    );

-- Customer Feedback Facts
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactCustomerFeedback (

    -- Savienojumi ar Dimensijām
    FeedbackDateKey Date,
    CustomerKey UInt32,
    EmployeeKey UInt32,
    FeedbackCategoryKey UInt32,

    -- Mērījumi
    FeedbackScore UInt8,
    ComplaintCount UInt8,
    ResolutionTimeHours Decimal(10, 2),
    CSATScore Decimal(5, 2),
    Comments String,
    Channel LowCardinality(String),

    -- "Grain"
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree PARTITION BY toYYYYMM(FeedbackDateKey)
ORDER BY
    (
        FeedbackDateKey,
        CustomerKey,
        EmployeeKey,
        FeedbackCategoryKey
    );

-- Promotion Response Facts
CREATE TABLE FactPromotionResponse (

    -- Savienojumi ar Dimensijām
    PromotionDateKey Date,
    ProductKey UInt32,
    StoreKey UInt32,
    PromotionKey UInt32,

    -- Atribūti
    SalesDuringCampaign Decimal(18, 2),
    DiscountUsageCount UInt32,
    CustomerUptakeRate Decimal(10, 4),
    PromotionROI Decimal(10, 4),

    -- "Grain"
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree PARTITION BY toYYYYMM(PromotionDateKey)
ORDER BY
    (
        PromotionDateKey,
        ProductKey,
        PromotionKey,
        StoreKey
    );

CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactFinance (
    -- Savienojumi ar Dimensijām
    InvoiceDateKey Date,
    CustomerKey UInt32,
    StoreKey UInt32,
    FinanceCategoryKey UInt32,

    -- Atribūti
    InvoiceAmount Decimal(18, 2),
    PaymentDelayDays Int32,
    CreditUsagePct Decimal(10, 4),
    InterestCharges Decimal(18, 2),
    InvoiceNumber String,
    PaymentStatus LowCardinality(String),
    CurrencyCode LowCardinality(String),
    -- "Grain"
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree PARTITION BY toYYYYMM(InvoiceDateKey)
ORDER BY
    (
        InvoiceDateKey,
        CustomerKey,
        StoreKey,
        FinanceCategoryKey
    );

-- Return Facts
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactReturns (
    -- Savienojumi ar Dimensijām
    ReturnDateKey Date,
    ProductKey UInt32,
    CustomerKey UInt32,
    StoreKey UInt32,
    ReturnReasonKey UInt32,

    -- Atribūti
    ReturnedQuantity Int32,
    RefundAmount Decimal(18, 2),
    RestockingFee Decimal(18, 2),
    ReturnID String,
    OriginalSalesID String,
    ReturnMethod LowCardinality(String),
    ConditionOnReturn LowCardinality(String),

    -- "Grain"
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree PARTITION BY toYYYYMM(ReturnDateKey)
ORDER BY
    (
        ReturnDateKey,
        ProductKey,
        CustomerKey,
        StoreKey,
        ReturnReasonKey
    );