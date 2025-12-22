# ClickHouse Physical Design Specification

## Table of Contents
1. [ClickHouse Architecture Overview](#architecture-overview)
2. [SCD Type 1 Tables (Static)](#scd-type-1-tables)
3. [SCD Type 2 Tables (Versioned)](#scd-type-2-tables)
4. [Fact Tables](#fact-tables)
5. [Aggregated Tables](#aggregated-tables)
6. [Error Records Table](#error-records-table)
7. [Data Type Mappings](#data-type-mappings)
8. [Performance Tuning](#performance-tuning)

---

## Architecture Overview

### Why ClickHouse?

ClickHouse is a **column-oriented database** optimized for analytical queries (OLAP), making it ideal for data warehousing:

- **Column Storage**: Only reads columns needed for queries
- **Compression**: Typically Great compression: 10 GiB → 1 GiB 
- **Parallel Processing**: Automatic process distribution across CPU cores
- **Partitioning**: Skip unnecessary data at query time
- **OLAP**: Literally made for Analytics, itš in the name

### MergeTree Family

ClickHouse uses the **MergeTree** engine family for all warehouse tables:

| Engine | Use Case | Key Feature |
|--------|----------|-------------|
| `MergeTree` | Fact tables, SCD Type 1 | Append-only, no updates |
| `ReplacingMergeTree(Version)` | SCD Type 2 dimensions | Keeps latest version per key |
| `SummingMergeTree(columns)` | Pre-aggregated tables | Auto-sums measures |

---

## SCD Type 1 Tables

### Example: DimDate

**Purpose**: Calendar dimension with no version history.

```sql
CREATE TABLE ADVENTUREWORKS_DWS.DimDate (
    -- Surrogate Key
    DateKey            Date,

    -- Date Attributes
    Year               UInt16,
    Quarter            UInt8,
    Month              UInt8,
    MonthName          LowCardinality(String),
    Week               UInt8,
    DayOfWeek          UInt8,
    DayName            LowCardinality(String),
    DayOfMonth         UInt8,
    DayOfYear          UInt16,
    WeekOfYear         UInt8,

    -- Boolean Flags
    IsWeekend          UInt8,
    IsHoliday          UInt8,

    -- Descriptors
    HolidayName        LowCardinality(String),
    FiscalYear         UInt16,
    FiscalQuarter      UInt8,
    FiscalMonth        UInt8,
    Season             LowCardinality(String)
)
ENGINE = MergeTree()
PRIMARY KEY DateKey
ORDER BY DateKey;
```

**Design Rationale**:
- **MergeTree (not ReplacingMergeTree)**: Dates never change, no need for versioning
- **PRIMARY KEY = DateKey**: Direct lookup by date (e.g., `WHERE DateKey = '2025-01-15'`)
- **ORDER BY DateKey**: Sorts data on disk by date for range scans
- **No PARTITION BY**: Small table (~10 years = 3,650 rows), partitioning overhead not worth it
- **LowCardinality**: Strings with few unique values (e.g., 12 month names) use dictionary encoding

**Example Query**:
```sql
-- Fast lookup (uses primary key)
SELECT * FROM DimDate WHERE DateKey = '2025-12-22';

-- Fast range scan (uses ORDER BY)
SELECT * FROM DimDate WHERE DateKey BETWEEN '2025-01-01' AND '2025-12-31';
```

---

## SCD Type 2 Tables

### Example: DimCustomer

**Purpose**: Track customer attribute changes over time.

```sql
CREATE TABLE ADVENTUREWORKS_DWS.DimCustomer (
    -- Surrogate Key & Natural Key
    CustomerKey UInt64,
    CustomerID UInt32,

    -- Customer Attributes
    CustomerName String,
    Email String,
    Phone String,
    City LowCardinality(String),
    StateProvince LowCardinality(String),
    Country LowCardinality(String),
    PostalCode String,

    -- Descriptors
    CustomerSegment LowCardinality(String),
    CustomerType LowCardinality(String),
    AccountStatus LowCardinality(String),
    CreditLimit Decimal(18, 2),
    AnnualIncome Decimal(18, 2),
    YearsSinceFirstPurchase Int32,

    -- Version Control
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date,
    EffectiveStartDate Date,
    EffectiveEndDate Nullable(Date),
    Version UInt64
)
ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY (CustomerID, ValidFromDate);
```

**Design Rationale**:

1. **ReplacingMergeTree(Version)**: Automatically keeps newest version per `(CustomerID, ValidFromDate)` during merges
2. **ORDER BY (CustomerID, ValidFromDate)**: Groups all versions of same customer together
3. **PARTITION BY toYYYYMM(ValidFromDate)**: Monthly partitions for fast historical queries
4. **Nullable(Date) for ValidToDate**: Current version has `NULL` (meaning "forever")

**Version Control Fields Explained**:

| Field | Purpose | Example |
|-------|---------|---------|
| CustomerKey | Surrogate key (unique per version) | 1001, 1002 |
| CustomerID | Natural key (same across versions) | 500, 500 |
| ValidFromDate | When this version became active | 2020-01-15, 2025-11-11 |
| ValidToDate | When this version expired (NULL = current) | 2025-11-10, NULL |
| IsCurrent | Quick lookup for current version | 0, 1 |
| Version | Auto-increment for deduplication | 1, 2 |

**Example: Customer Email Change**

-- Before: Customer 500 has old email

| CustomerKey | CustomerID | Email | ValidFromDate | ValidToDate | IsCurrent |
|---|---|---|---|---|---|
| 1001 | 500 | old@example.com | 2020-01-15 | NULL | 1 |

-- After: Customer 500 changes email

| CustomerKey | CustomerID | Email | ValidFromDate | ValidToDate | IsCurrent |
|---|---|---|---|---|---|
| 1001 | 500 | old@example.com | 2020-01-15 | 2025-11-10 | 0 |
| 1002 | 500 | new@example.com | 2025-11-11 | NULL | 1 |


**Lookup Pattern**:
```sql
-- Get current customer data
SELECT * FROM DimCustomer 
WHERE CustomerID = 500 AND IsCurrent = 1;

-- Get customer data as of specific date
SELECT * FROM DimCustomer
WHERE CustomerID = 500 
  AND ValidFromDate <= '2025-06-01'
  AND (ValidToDate IS NULL OR ValidToDate > '2025-06-01');
```

---

## Fact Tables

### Example: FactSales

**Purpose**: Store sales transactions at line item grain.

```sql
CREATE TABLE ADVENTUREWORKS_DWS.FactSales (
    -- Foreign Keys to Dimensions
    SalesDateKey Date,
    CustomerKey UInt64,
    ProductKey UInt64,
    StoreKey UInt64,
    EmployeeKey UInt64,

    -- Grain (unique identifier for each row)
    SalesOrderID UInt32,
    SalesOrderDetailID UInt32,

    -- Measures
    QuantitySold UInt32,
    SalesRevenue Decimal(18, 2),
    DiscountAmount Decimal(18, 2),
    NumberOfTransactions UInt8,

    -- Degenerate Dimensions
    UnitPrice Decimal(18, 2),
    UnitPriceDiscount Decimal(18, 4),
    LineTotal Decimal(18, 2),

    -- Metadata
    InsertedAt DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(SalesDateKey)
ORDER BY (SalesDateKey, ProductKey, CustomerKey, StoreKey, SalesOrderID, SalesOrderDetailID);
```

**Design Rationale**:

1. **MergeTree (not ReplacingMergeTree)**: Facts are append-only, never updated
2. **PARTITION BY toYYYYMM(SalesDateKey)**: Monthly partitions enable partition pruning
3. **ORDER BY**: Optimized for common query patterns (date first, then dimensions)
4. **Grain**: One row per order line item (SalesOrderID + SalesOrderDetailID)

**Partition Pruning Example**:
```sql
-- Good: Scans only December 2025 partition (50x faster)
SELECT * FROM FactSales WHERE SalesDateKey >= '2025-12-01';

-- Bad: Scans ALL partitions (no pruning)
SELECT * FROM FactSales WHERE YEAR(SalesDateKey) = 2025;
```

**Example Measures**:
```
OrderID=12345, LineID=3:
  QuantitySold = 2 (2 units)
  SalesRevenue = 450.50 (2 × $225.25)
  DiscountAmount = 45.05 (10% off)
  NumberOfTransactions = 1 (atomic grain)
```

---

## Aggregated Tables

### Example: agg_daily_sales

**Purpose**: Pre-compute daily sales by store and product category for fast reporting.

```sql
CREATE TABLE ADVENTUREWORKS_DS.agg_daily_sales (
    SalesDateKey          Date,
    StoreKey              UInt64,
    ProductCategoryKey    UInt64,

    RevenueSum            Decimal(18,2),
    QuantitySum           UInt64,
    DiscountSum           Decimal(18,2),
    TransactionCount      UInt64
)
ENGINE = MergeTree()
PARTITION BY SalesDateKey
ORDER BY (SalesDateKey, StoreKey, ProductCategoryKey);
```

**Design Rationale**:
- **MergeTree (not SummingMergeTree)**: We control aggregation via materialized views
- **Grain**: One row per date-store-category combination
- **Purpose**: Avoid scanning 100M fact rows for simple daily reports

**Performance Comparison**:
```sql
-- Without aggregate: Scans FactSales (100M rows)
SELECT SalesDateKey, StoreKey, SUM(SalesRevenue)
FROM FactSales
WHERE SalesDateKey >= '2025-12-01'
GROUP BY SalesDateKey, StoreKey;
→ 15 seconds

-- With aggregate: Scans agg_daily_sales (30K rows)
SELECT SalesDateKey, StoreKey, SUM(RevenueSum)
FROM agg_daily_sales
WHERE SalesDateKey >= '2025-12-01'
GROUP BY SalesDateKey, StoreKey;
→ 0.3 seconds (50x faster!)
```

---

## Error Records Table

**Purpose**: Centralized error logging for ETL pipeline.

```sql
CREATE TABLE ADVENTUREWORKS_DWS.error_records (
    ErrorID UInt64,
    ErrorDate DateTime,
    SourceTable String,
    RecordNaturalKey String,
    ErrorType String,
    ErrorSeverity String,
    ErrorMessage String,
    FailedData String,
    ProcessingBatchID String,
    TaskName String,
    IsRecoverable UInt8,
    RetryCount UInt8 DEFAULT 0,
    LastAttemptDate Nullable(DateTime),
    IsResolved UInt8 DEFAULT 0,
    ResolutionComment Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (ErrorDate, SourceTable, ErrorType)
PARTITION BY toYYYYMM(ErrorDate);
```

**Example Error Record**:
```
ErrorID: 1001
ErrorDate: 2025-11-11 02:30:00
SourceTable: FactSales
RecordNaturalKey: order-12345-line-3
ErrorType: FK_MISS
ErrorSeverity: WARNING
ErrorMessage: CustomerKey not found for CustomerID=999
FailedData: {"order_id": 12345, "customer_id": 999, "amount": 450.50}
IsRecoverable: 1 (retry tomorrow when customer loads)
```

---

## Data Type Mappings

| PostgreSQL | ClickHouse | Rationale |
|------------|------------|-----------|
| INTEGER | UInt32 or Int32 | Use UInt if always positive (saves space) |
| BIGINT | UInt64 or Int64 | For large IDs |
| NUMERIC(18,2) | Decimal(18, 2) | Exact money values (no float errors) |
| VARCHAR | String | No length limit in ClickHouse |
| DATE | Date | 2-byte storage (vs 4-byte in PG) |
| TIMESTAMP | DateTime | 4-byte storage, no timezone |
| BOOLEAN | UInt8 | 0 or 1 (ClickHouse has no native bool) |
| NULL values | Nullable(Type) | Use sparingly (performance impact) |

**Nullable Performance Impact**:
```sql
-- Slow: Nullable column (extra null bitmap)
CREATE TABLE t (id UInt32, value Nullable(String));

-- Fast: Non-nullable with default
CREATE TABLE t (id UInt32, value String DEFAULT '');
```

---

## Performance Tuning

### 1. Partition Pruning

**Good Query** (uses partition pruning):
```sql
SELECT * FROM FactSales WHERE SalesDateKey >= '2025-12-01';
→ Scans only December partition (10M rows)
```

**Bad Query** (scans all partitions):
```sql
SELECT * FROM FactSales WHERE YEAR(SalesDateKey) = 2025;
→ Scans ALL partitions (100M rows)
```

### 2. Column Compression

```sql
ALTER TABLE FactSales 
MODIFY COLUMN SalesDateKey Date CODEC(Delta, ZSTD);
-- Delta: Store differences (good for time series)
-- ZSTD: Compress differences (20:1 ratio typical)
```

### 3. Index Granularity

```sql
ENGINE = MergeTree()
ORDER BY (DateKey)
SETTINGS index_granularity = 8192;  -- Default (balance speed vs memory)
```

**Explanation**: ClickHouse creates a sparse primary index every 8,192 rows. Smaller = more memory but faster lookups.

### 4. Materialized Views for Aggregates

```sql
-- Automatically update agg_daily_sales when FactSales changes
CREATE MATERIALIZED VIEW mv_agg_daily_sales
TO agg_daily_sales
AS
SELECT 
    SalesDateKey,
    StoreKey,
    ProductCategoryKey,
    SUM(SalesRevenue) AS RevenueSum,
    SUM(QuantitySold) AS QuantitySum
FROM FactSales
GROUP BY SalesDateKey, StoreKey, ProductCategoryKey;
```

---

## Summary

| Table Type | Engine | Partitioning | Use Case |
|------------|--------|--------------|----------|
| SCD Type 1 | MergeTree | None (small tables) | Static lookups |
| SCD Type 2 | ReplacingMergeTree | Monthly by ValidFromDate | Versioned dimensions |
| Fact Tables | MergeTree | Monthly by date key | Transactional data |
| Aggregates | MergeTree | Monthly by date key | Pre-computed summaries |
| Error Logs | MergeTree | Monthly by ErrorDate | ETL error tracking |

**Key Takeaways**:
1. Use **ReplacingMergeTree** only for SCD Type 2 (versioned data)
2. Always **partition by date** for time-series data
3. **Avoid Nullable** when possible (use defaults instead)
4. Use **LowCardinality** for strings with few unique values
5. **ORDER BY** matters - put most-queried columns first