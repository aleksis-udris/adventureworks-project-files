
-- Create Database ADVENTUREWORKS_DWS
CREATE DATABASE IF NOT EXISTS ADVENTUREWORKS_DWS;

-- Fact tables
-- Sales Facts
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.FactSales;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactSales (

    -- Savienojumi ar dimensijām
    SalesDateKey Date,
    CustomerKey UInt64,
    ProductKey UInt64,
    StoreKey UInt64,
    EmployeeKey UInt64,

    -- "Grain"
    SalesOrderID UInt32,
    SalesOrderDetailID UInt32,

    -- Mērījumi
    QuantitySold UInt32,
    SalesRevenue Decimal(18, 2),
    DiscountAmount Decimal(18, 2),
    NumberOfTransactions UInt8,

    -- Noderīgas vērtības
    UnitPrice Decimal(18, 2),
    UnitPriceDiscount Decimal(18, 4),
    LineTotal Decimal(18, 2),
    InsertedAt DateTime DEFAULT now()
) ENGINE = MergeTree PARTITION BY SalesDateKey
ORDER BY
    (
        SalesDateKey,
        ProductKey,
        CustomerKey,
        StoreKey,
        SalesOrderID,
        SalesOrderDetailID
    );

-- Purchase Facts
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.FactPurchases;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactPurchases (

    -- Savienojumi ar dimensijām
    PurchaseDateKey Date,
    VendorKey UInt64,
    ProductKey UInt64,

    -- "Grain"
    PurchaseOrderID UInt32,
    PurchaseOrderDetailID UInt32,

    -- Mērījumi
    QuantityBought UInt32,
    PurchaseAmount Decimal(18, 2),
    DiscountAmount Decimal(18, 2),
    NumberOfTransactions UInt8,

    -- Noderīgas vērtības
    UnitPrice Decimal(18, 2),
    UnitPriceDiscount Decimal(18, 4),
    LineTotal Decimal(18, 2),
    InsertedAt DateTime DEFAULT now()
) ENGINE = MergeTree PARTITION BY PurchaseDateKey
ORDER BY
    (
        PurchaseDateKey,
        ProductKey,
        VendorKey,
        PurchaseOrderID,
        PurchaseOrderDetailID
    );

-- Inventory Facts
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.FactInventory;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactInventory (

    -- Savienojumi ar dimensijām
    InventoryDateKey Date,
    ProductKey UInt64,
    StoreKey UInt64,
    WarehouseKey UInt64,

    -- Atribūti
    QuantityOnHand UInt32,
    StockAging UInt32,
    ReorderLevel UInt32,
    SafetyStockLevels UInt32,

    -- "Grain"
    SnapshotCreatedDateTime DateTime,
    ETLBatchID String
) ENGINE = MergeTree PARTITION BY InventoryDateKey
ORDER BY
    (
        InventoryDateKey,
        ProductKey,
        WarehouseKey,
        StoreKey
    );

-- Production Facts
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.FactProduction;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactProduction (

    -- Surogātatslēga
    ProductionRunID UInt64,

    -- Savienojumi ar Dimensijām
    ProductionDateKey Date,
    ProductKey UInt64,
    SupervisorKey UInt64,

    -- Atribūti
    UnitsProduced UInt32,
    ProductionTimeHours Decimal(10, 2),
    ScrapRatePercent Decimal(5, 2),
    DefectCount UInt32,

    -- "Grain"
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree PARTITION BY ProductionDateKey
ORDER BY
    (
        ProductionDateKey,
        ProductKey,
        ProductionRunID
    );

-- Employee Sale Facts
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.FactEmployeeSales;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactEmployeeSales (

    -- Savienojumi ar dimensijām
    SalesDateKey Date,
    EmployeeKey UInt64,
    StoreKey UInt64,
    SalesTerritoryKey UInt64,

    -- Atribūti
    SalesAmount Decimal(18, 2),
    SalesTarget Decimal(18, 2),
    TargetAttainment Decimal(10, 4),
    CustomerContactsCount UInt32,

    -- "Grain"
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree PARTITION BY SalesDateKey
ORDER BY
    (
        SalesDateKey,
        EmployeeKey,
        StoreKey,
        SalesTerritoryKey
    );

-- Customer Feedback Facts
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.FactCustomerFeedback;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactCustomerFeedback (

    -- Savienojumi ar Dimensijām
    FeedbackDateKey Date,
    CustomerKey UInt64,
    EmployeeKey UInt64,
    FeedbackCategoryKey UInt64,

    -- Atribūti
    FeedbackScore UInt8,
    ComplaintCount UInt8,
    ResolutionTimeHours Decimal(10, 2),
    CSATScore Decimal(5, 2),
    Comments String,
    Channel LowCardinality(String),

    -- "Grain"
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree PARTITION BY FeedbackDateKey
ORDER BY
    (
        FeedbackDateKey,
        CustomerKey,
        EmployeeKey,
        FeedbackCategoryKey
    );

-- Promotion Response Facts
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.FactPromotionResponse;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactPromotionResponse (

    -- Savienojumi ar Dimensijām
    PromotionDateKey Date,
    ProductKey UInt64,
    StoreKey UInt64,
    PromotionKey UInt64,

    -- Atribūti
    SalesDuringCampaign Decimal(18, 2),
    DiscountUsageCount UInt32,
    CustomerUptakeRate Decimal(10, 4),
    PromotionROI Decimal(10, 4),

    -- "Grain"
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree PARTITION BY PromotionDateKey
ORDER BY
    (
        PromotionDateKey,
        ProductKey,
        PromotionKey,
        StoreKey
    );

-- Create Finance Facts
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.FactFinance;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactFinance (
    -- Savienojumi ar Dimensijām
    InvoiceDateKey Date,
    CustomerKey UInt64,
    StoreKey UInt64,
    FinanceCategoryKey UInt64,

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
) ENGINE = MergeTree PARTITION BY InvoiceDateKey
ORDER BY
    (
        InvoiceDateKey,
        CustomerKey,
        StoreKey,
        FinanceCategoryKey
    );

-- Return Facts
DROP TABLE IF EXISTS ADVENTUREWORKS_DWS.FactReturns;
CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.FactReturns (
    -- Savienojumi ar Dimensijām
    ReturnDateKey Date,
    ProductKey UInt64,
    CustomerKey UInt64,
    StoreKey UInt64,
    ReturnReasonKey UInt64,

    -- Atribūti
    ReturnedQuantity UInt32,
    RefundAmount Decimal(18, 2),
    RestockingFee Decimal(18, 2),
    ReturnID String,
    OriginalSalesID String,
    ReturnMethod LowCardinality(String),
    ConditionOnReturn LowCardinality(String),

    -- "Grain"
    ETLBatchID String,
    LoadTimestamp DateTime
) ENGINE = MergeTree() PARTITION BY ReturnDateKey
ORDER BY
    (
        ReturnDateKey,
        ProductKey,
        CustomerKey,
        StoreKey,
        ReturnReasonKey
    );