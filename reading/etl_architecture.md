# ETL Architecture Overview

## Table of Contents
1. [Architecture Diagram](#architecture-diagram)
2. [ETL Principles](#etl-principles)
3. [Incremental vs Full Load](#incremental-vs-full-load)
4. [Late-Arriving Dimensions](#late-arriving-dimensions)
5. [Data Freshness SLA](#data-freshness-sla)
6. [Daily ETL Flow](#daily-etl-flow)

---

## Architecture Diagram

```
┌─────────────────────────────────┐
│  PostgreSQL Source              │
│ (AdventureWorks)                │
│                                 │
│ • Sales tables                  │
│ • Product tables                │
│ • Customer tables               │
└─────────────┬───────────────────┘
              │
              ↓ [STEP 1: EXTRACT]
              │ PostgreSQL query
              │ Incremental load (last 24h)
              │ Row counts logged
              │
┌─────────────────────────────────┐
│ ClickHouse STAGING LAYER        │
│                                 │
│ • stg_orders                    │
│ • stg_customers                 │
│ • stg_products                  │
│ • Raw data (no transform)       │
└─────────────┬───────────────────┘
              │
              ↓ [STEP 2: VALIDATE]
              │ Null checks
              │ Type checks
              │ Range validation
              │ Duplicate detection
              │
              ├─ PASS → Continue
              └─ FAIL → Log error_records
              │
┌─────────────────────────────────┐
│ ClickHouse TRANSFORM            │
│                                 │
│ • Apply SCD Type 2              │
│ • Deduplicate                   │
│ • Calculate surrogate keys      │
│ • Standardize data              │
└─────────────┬───────────────────┘
              │
              ↓ [STEP 3: MERGE]
              │ SCD logic
              │ FK resolution
              │ Grain validation
              │
┌─────────────────────────────────┐
│ ClickHouse DWH (FINAL)          │
│                                 │
│ • DimCustomer                   │
│ • DimProduct                    │
│ • FactSales                     │
│ • Aggregates                    │
│ • error_records (failures)      │
└─────────────┬───────────────────┘
              │
              ↓ [STEP 4: ANALYZE]
              │ Dashboard queries
              │ Reporting
```

---

## ETL Principles

### ELT vs ETL

This project uses **ETL** (Extract-Transform-Load) approach:

| Stage | Location | Purpose |
|-------|----------|---------|
| Extract | PostgreSQL → Memory | Query source data |
| Transform | Python/Airflow | Apply business logic, SCD Type 2 |
| Load | ClickHouse | Insert final data |

**Why ETL (not ELT)?**
- ClickHouse is optimized for reads, not complex transforms
- Transform logic in Python is more maintainable
- Can apply data quality checks before loading

---

## Incremental vs Full Load

### Incremental Strategy (RECOMMENDED)

Load only NEW or CHANGED data since last run.

**Example Timeline**:
```
Day 1: Load all history (full load) – 5 years of data
  → 100M rows loaded in 8 hours

Day 2+: Load only NEW data from yesterday (incremental)
  → 274K rows loaded in 50 minutes (361x faster!)
```

**Implementation**:
```sql
-- Incremental load: Only yesterday's orders
SELECT * FROM sales.orders 
WHERE OrderDate = CURRENT_DATE - 1;

-- Full load: All orders
SELECT * FROM sales.orders;
```

**Cost Comparison**:

| Load Type | Rows Scanned | Time | Data Transfer |
|-----------|--------------|------|---------------|
| Full load | 100M | 8 hours | 50 GB |
| Incremental | 274K | 50 min | 150 MB |
| **Savings** | **361x less** | **9.6x faster** | **333x less** |

### When to Use Full Load

- **Initial Setup**: First time loading warehouse
- **Schema Changes**: Column added/removed in source
- **Data Corruption**: Need to rebuild from scratch
- **Historical Corrections**: Fixing past data errors

---

## Late-Arriving Dimensions

### Problem Scenario

Order placed November 11, but customer profile added November 12.

```
Source (Nov 11): order_id=12345, customer_id=500
Source (Nov 12): customer_id=500, name="John Doe"

Problem: Nov 11 ETL can't find "John Doe" in DimCustomer
```

### Solution Options

#### Option 1: Default Dimension (Recommended)

Use placeholder dimension key when FK missing:

```python
customer_key = lookup_customer(customer_id)
if customer_key is None:
    customer_key = 0  # Default "Unknown Customer"
    log_warning(f"Customer {customer_id} not found, using default")
```

**Pros**: ETL doesn't fail, order still loaded  
**Cons**: Reports show "Unknown Customer" until next run

#### Option 2: Reprocess Daily

Rerun yesterday's facts after dimensions load:

```
02:00 - Load dimensions (customers arrive now)
02:30 - Load facts (pick up new customers)
03:00 - Reprocess yesterday's errors (fix late arrivals)
```

**Pros**: Fixes late arrivals automatically  
**Cons**: Additional processing time

#### Option 3: Store Natural Keys

Store `customer_id` in FactSales, join at query time:

```sql
-- Query-time join
SELECT f.*, c.CustomerName
FROM FactSales f
LEFT JOIN DimCustomer c ON f.customer_id = c.CustomerID AND c.IsCurrent = 1;
```

**Pros**: No missing data  
**Cons**: Slower queries (join every time)

---

## Data Freshness SLA

### Service Level Agreement

**SLA**: Orders must be in DWH within **4 hours** of transaction.

### Timeline Example

```
Day N-1 23:59 - Orders complete in source system
Day N   02:00 - ETL starts (2-hour delay for source stabilization)
Day N   03:00 - ETL ends (50 minutes processing)
Day N   03:00 - Data available in DWH ✅ (3 hours = within SLA!)
```

### Monitoring & Alerts

```yaml
Alert Conditions:
  CRITICAL:
    - IF ETL ends after 06:00 → Email team immediately
    - IF error_records has non-recoverable errors → Page on-call
    - IF row counts differ > 10% from yesterday → Investigate
  
  WARNING:
    - IF ETL ends after 04:00 → Slack notification
    - IF > 100 recoverable errors → Email team (daily digest)
    - IF partition size > 50GB → Consider optimization
```

---

## Daily ETL Flow

### Complete Day-by-Day Example

```

[Any Hour of Day] – ETL Starts

Step 1: EXTRACT (PostgreSQL → ClickHouse Staging)
├─ Query: SELECT * FROM sales.orders WHERE OrderDate = N-1
├─ Result: 274K rows → stg_orders
├─ Time: 15 minutes
└─ Log: "Extracted 274,532 orders"

Step 2: VALIDATE (Data Quality)
├─ Checks: NULL, types, ranges, duplicates
├─ Result: 274K rows valid, 0 errors
├─ Time: 3 minutes
└─ Log: "All validations passed"

Step 3: TRANSFORM (Staging → Transform)
├─ Task 3a: Load SCD Type 2 dimensions (customers changed)
│  └─ INSERT 25 new customer versions (email/address changes)
│  └─ Time: 2 minutes
├─ Task 3b: Load SCD Type 1 dimensions (static lookups)
│  └─ UPSERT 0 product changes (same as before)
│  └─ Time: 1 minute
└─ Result: Ready for fact loads

Step 4: LOAD (Transform → DWH)
├─ Task 4a: Load FactSales
│  ├─ Resolve FK: CustomerKey from DimCustomer (WHERE IsCurrent=1)
│  ├─ Resolve FK: ProductKey from DimProduct (WHERE IsCurrent=1)
│  ├─ Result: 274K fact rows inserted
│  └─ Time: 12 minutes
├─ Task 4b: Load FactReturns
│  ├─ Result: 5.2K return rows inserted
│  └─ Time: 3 minutes
└─ Task 4c: Load aggregates
   ├─ INSERT agg_daily_sales (pre-compute for fast reporting)
   └─ Time: 5 minutes

Step 5: ERROR HANDLING
├─ Check error_records table: Any new errors?
├─ Recoverable errors found: 12 rows (FK miss)
├─ Action: Add to reprocess queue for next run
├─ Non-recoverable errors: 0 (all good)
└─ Log: "Processed with 12 warnings"

[DAY N 02:50 UTC] – ETL Completes (50 minutes total)

RESULT for Business:
 Yesterday's sales data (274K rows) now in ClickHouse
 Dashboards updated for N-1
 Analysts can query at 03:00 UTC
```

### Task Dependencies

```
extract_data
    ↓
validate_data
    ↓
load_dimensions (parallel)
    ├─ load_dim_customer
    ├─ load_dim_product
    └─ load_dim_store
    ↓
load_facts
    ├─ load_fact_sales
    ├─ load_fact_purchases
    └─ load_fact_inventory
    ↓
update_aggregates
    ├─ agg_daily_sales
    └─ agg_weekly_sales
    ↓
reprocess_errors
```

---

## Performance Optimization

### Batch Processing

```python
# Good: Batch inserts (10K rows at once)
for batch in chunks(rows, 10000):
    clickhouse.insert(batch)
→ 50 minutes for 274K rows

# Bad: Row-by-row inserts
for row in rows:
    clickhouse.insert(row)
→ 8 hours for 274K rows (9.6x slower!)
```

### Parallel Loading

```python
# Load multiple dimensions simultaneously
load_dim_customer.set_downstream([load_fact_sales])
load_dim_product.set_downstream([load_fact_sales])
load_dim_store.set_downstream([load_fact_sales])

# load_fact_sales waits for ALL dimensions to complete
```

### Dimension Caching

```python
# Good: Bulk lookup (1 query)
customer_keys = bulk_lookup_dimension_keys(
    ch_client, 
    "DimCustomer", 
    "CustomerID", 
    [100, 101, 102, ...]  # All customer IDs
)
→ 1 query for 10K lookups

# Bad: Individual lookups (10K queries)
for customer_id in customer_ids:
    customer_key = lookup_customer(customer_id)
→ 10K queries
```

---

## Summary

**Key Principles**:
1. **Incremental loads** - 361x faster than full loads
2. **Batch processing** - 9.6x faster than row-by-row
3. **Parallel execution** - Load dimensions simultaneously
4. **Error handling** - Recoverable vs non-recoverable classification
5. **SLA monitoring** - 4-hour freshness guarantee

**Next Steps**:
- See [Airflow DAG Specification](airflow_dag_spec.md) for orchestration details
- See [Transformation Logic](transformation_logic.md) for SCD Type 2 implementation
- See [Error Handling & Monitoring](error_handling.md) for error recovery strategies