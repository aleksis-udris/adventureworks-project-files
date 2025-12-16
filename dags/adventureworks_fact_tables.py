from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from airflow.sdk import dag, task

from extra_functions import (
    bulk_lookup_dimension_keys,
    build_error_record,
    ch,
    clean_values_in_rows,
    insert_error_records,
    pg,
)

PG_FETCH_SIZE = 50_000
CH_INSERT_BATCH_SIZE = 10_000
CLICKHOUSE_INSERT_SETTINGS = {
    "async_insert": 1,
    "wait_for_async_insert": 0,
}


class FKSpec(Tuple[str, str, str, str, bool, int, bool]):
    pass


def _record_nk(table: str, row: Sequence[Any], idx: Dict[str, int]) -> str:
    try:
        if table == "FactSales":
            return f"{row[idx['SalesOrderID']]}-{row[idx['SalesOrderDetailID']]}"
        if table == "FactPurchases":
            return f"{row[idx['PurchaseOrderID']]}-{row[idx['PurchaseOrderDetailID']]}"
        if table == "FactInventory":
            return (
                f"{row[idx['InventoryDateKey']]}-"
                f"{row[idx['ProductID']]}-"
                f"{row[idx['WarehouseID']]}"
            )
        if table == "FactProduction":
            return str(row[idx["ProductionRunID"]])
        if table == "FactEmployeeSales":
            return f"{row[idx['SalesDateKey']]}-{row[idx['EmployeeID']]}"
        if table == "FactCustomerFeedback":
            return f"{row[idx['FeedbackDateKey']]}-{row[idx.get('CustomerID', 0)]}"
        if table == "FactPromotionResponse":
            return (
                f"{row[idx['PromotionDateKey']]}-"
                f"{row[idx['PromotionID']]}-"
                f"{row[idx['ProductID']]}"
            )
        if table == "FactFinance":
            return str(row[idx["InvoiceNumber"]])
        if table == "FactReturns":
            return str(row[idx["ReturnID"]])
    except Exception:
        pass
    return "-".join(str(x) for x in list(row)[:2])


FACTS = {
    "FactSales": {
        "insert_columns": [
            "SalesDateKey",
            "CustomerKey",
            "ProductKey",
            "StoreKey",
            "EmployeeKey",
            "SalesOrderID",
            "SalesOrderDetailID",
            "QuantitySold",
            "SalesRevenue",
            "DiscountAmount",
            "NumberOfTransactions",
            "UnitPrice",
            "UnitPriceDiscount",
            "LineTotal",
        ],
        "extract_columns": [
            "SalesDateKey",
            "CustomerID",
            "ProductID",
            "StoreID",
            "EmployeeID",
            "SalesOrderID",
            "SalesOrderDetailID",
            "QuantitySold",
            "SalesRevenue",
            "DiscountAmount",
            "NumberOfTransactions",
            "UnitPrice",
            "UnitPriceDiscount",
            "LineTotal",
        ],
        "fk_specs": [
            ("CustomerKey", "DimCustomer", "CustomerID", "CustomerID", True, 0, True),
            ("ProductKey", "DimProduct", "ProductID", "ProductID", True, 0, True),
            ("StoreKey", "DimStore", "StoreID", "StoreID", False, 0, True),
            ("EmployeeKey", "DimEmployee", "EmployeeID", "EmployeeID", False, 0, True),
        ],
        "query": """
            SELECT
                soh.OrderDate::DATE AS SalesDateKey,
                c.CustomerID AS CustomerID,
                p.ProductID AS ProductID,
                c.StoreID AS StoreID,
                soh.SalesPersonID AS EmployeeID,
                sod.SalesOrderID,
                sod.SalesOrderDetailID,
                sod.OrderQty AS QuantitySold,
                CAST(sod.UnitPrice * sod.OrderQty * (1 - sod.UnitPriceDiscount) AS DECIMAL(18, 2)) AS SalesRevenue,
                CAST(sod.UnitPrice * sod.OrderQty * sod.UnitPriceDiscount AS DECIMAL(18, 2)) AS DiscountAmount,
                1 AS NumberOfTransactions,
                CAST(sod.UnitPrice AS DECIMAL(18, 2)) AS UnitPrice,
                CAST(sod.UnitPriceDiscount AS DECIMAL(18, 4)) AS UnitPriceDiscount,
                CAST(sod.UnitPrice * sod.OrderQty * (1 - sod.UnitPriceDiscount) AS DECIMAL(18, 2)) AS LineTotal
            FROM Sales.SalesOrderDetail AS sod
            INNER JOIN Sales.SalesOrderHeader AS soh ON sod.SalesOrderID = soh.SalesOrderID
            LEFT JOIN Sales.Customer AS c ON soh.CustomerID = c.CustomerID
            LEFT JOIN Production.Product AS p ON sod.ProductID = p.ProductID
            ORDER BY soh.OrderDate::DATE, sod.SalesOrderID, sod.SalesOrderDetailID;
        """,
    },
    "FactPurchases": {
        "insert_columns": [
            "PurchaseDateKey",
            "VendorKey",
            "ProductKey",
            "PurchaseOrderID",
            "PurchaseOrderDetailID",
            "QuantityBought",
            "PurchaseAmount",
            "DiscountAmount",
            "NumberOfTransactions",
            "UnitPrice",
            "UnitPriceDiscount",
            "LineTotal",
        ],
        "extract_columns": [
            "PurchaseDateKey",
            "VendorID",
            "ProductID",
            "PurchaseOrderID",
            "PurchaseOrderDetailID",
            "QuantityBought",
            "PurchaseAmount",
            "DiscountAmount",
            "NumberOfTransactions",
            "UnitPrice",
            "UnitPriceDiscount",
            "LineTotal",
        ],
        "fk_specs": [
            ("VendorKey", "DimVendor", "VendorID", "VendorID", True, 0, True),
            ("ProductKey", "DimProduct", "ProductID", "ProductID", True, 0, True),
        ],
        "query": """
            SELECT
                poh.OrderDate::DATE AS PurchaseDateKey,
                v.BusinessEntityID AS VendorID,
                p.ProductID AS ProductID,
                pod.PurchaseOrderID,
                pod.PurchaseOrderDetailID,
                pod.OrderQty AS QuantityBought,
                CAST(pod.UnitPrice * pod.OrderQty AS DECIMAL(18, 2)) AS PurchaseAmount,
                CAST(0 AS DECIMAL(18, 2)) AS DiscountAmount,
                1 AS NumberOfTransactions,
                CAST(pod.UnitPrice AS DECIMAL(18, 2)) AS UnitPrice,
                CAST(0 AS DECIMAL(18, 4)) AS UnitPriceDiscount,
                CAST(pod.UnitPrice * pod.OrderQty AS DECIMAL(18, 2)) AS LineTotal
            FROM Purchasing.PurchaseOrderDetail AS pod
            INNER JOIN Purchasing.PurchaseOrderHeader AS poh ON pod.PurchaseOrderID = poh.PurchaseOrderID
            INNER JOIN Production.Product AS p ON pod.ProductID = p.ProductID
            INNER JOIN Purchasing.Vendor AS v ON poh.VendorID = v.BusinessEntityID
            ORDER BY poh.OrderDate::DATE, pod.PurchaseOrderID, pod.PurchaseOrderDetailID;
        """,
    },
    "FactInventory": {
        "insert_columns": [
            "InventoryDateKey",
            "ProductKey",
            "StoreKey",
            "WarehouseKey",
            "QuantityOnHand",
            "StockAging",
            "ReorderLevel",
            "SafetyStockLevels",
            "SnapshotCreatedDateTime",
            "ETLBatchID",
        ],
        "extract_columns": [
            "InventoryDateKey",
            "ProductID",
            "StoreID",
            "WarehouseID",
            "QuantityOnHand",
            "StockAging",
            "ReorderLevel",
            "SafetyStockLevels",
            "SnapshotCreatedDateTime",
            "ETLBatchID",
        ],
        "fk_specs": [
            ("ProductKey", "DimProduct", "ProductID", "ProductID", True, 0, True),
            ("StoreKey", "DimStore", "StoreID", "StoreID", False, 0, True),
            ("WarehouseKey", "DimWarehouse", "WarehouseID", "WarehouseID", True, 0, True),
        ],
        "query": """
            SELECT
                pi.ModifiedDate::DATE AS InventoryDateKey,
                pi.ProductID AS ProductID,
                NULL::INT AS StoreID,
                pi.LocationID AS WarehouseID,
                pi.Quantity AS QuantityOnHand,
                EXTRACT(DAY FROM (CURRENT_DATE - p.SellStartDate))::INT AS StockAging,
                p.ReorderPoint AS ReorderLevel,
                p.SafetyStockLevel AS SafetyStockLevels,
                CURRENT_TIMESTAMP AS SnapshotCreatedDateTime,
                uuid_generate_v4()::TEXT AS ETLBatchID
            FROM Production.ProductInventory AS pi
            INNER JOIN Production.Product AS p ON pi.ProductID = p.ProductID
            INNER JOIN Production.Location AS l ON pi.LocationID = l.LocationID
            ORDER BY pi.ModifiedDate::DATE, pi.ProductID, pi.LocationID;
        """,
    },
    "FactProduction": {
        "insert_columns": [
            "ProductionRunID",
            "ProductionDateKey",
            "ProductKey",
            "SupervisorKey",
            "UnitsProduced",
            "ProductionTimeHours",
            "ScrapRatePercent",
            "DefectCount",
            "ETLBatchID",
            "LoadTimestamp",
        ],
        "extract_columns": [
            "ProductionRunID",
            "ProductionDateKey",
            "ProductID",
            "SupervisorID",
            "UnitsProduced",
            "ProductionTimeHours",
            "ScrapRatePercent",
            "DefectCount",
            "ETLBatchID",
            "LoadTimestamp",
        ],
        "fk_specs": [
            ("ProductKey", "DimProduct", "ProductID", "ProductID", True, 0, True),
            ("SupervisorKey", "DimEmployee", "EmployeeID", "SupervisorID", False, 0, True),
        ],
        "query": """
            SELECT
                ('x' || MD5(wo.WorkOrderID::TEXT || wo.ProductID::TEXT))::bit(32)::BIGINT AS ProductionRunID,
                wo.StartDate::DATE AS ProductionDateKey,
                p.ProductID AS ProductID,
                NULL::INT AS SupervisorID,
                wo.OrderQty AS UnitsProduced,
                CAST(EXTRACT(EPOCH FROM (COALESCE(wo.EndDate, CURRENT_TIMESTAMP) - wo.StartDate)) / 3600 AS DECIMAL(10, 2)) AS ProductionTimeHours,
                CAST(CASE WHEN wo.OrderQty > 0 THEN (wo.ScrappedQty::DECIMAL / wo.OrderQty::DECIMAL) * 100 ELSE 0 END AS DECIMAL(5, 2)) AS ScrapRatePercent,
                wo.ScrappedQty AS DefectCount,
                uuid_generate_v4()::TEXT AS ETLBatchID,
                CURRENT_TIMESTAMP AS LoadTimestamp
            FROM Production.WorkOrder AS wo
            INNER JOIN Production.Product AS p ON wo.ProductID = p.ProductID
            ORDER BY wo.StartDate::DATE, wo.WorkOrderID;
        """,
    },
    "FactEmployeeSales": {
        "insert_columns": [
            "SalesDateKey",
            "EmployeeKey",
            "StoreKey",
            "SalesTerritoryKey",
            "SalesAmount",
            "SalesTarget",
            "TargetAttainment",
            "CustomerContactsCount",
            "ETLBatchID",
            "LoadTimestamp",
        ],
        "extract_columns": [
            "SalesDateKey",
            "EmployeeID",
            "StoreID",
            "TerritoryID",
            "SalesAmount",
            "SalesTarget",
            "TargetAttainment",
            "CustomerContactsCount",
            "ETLBatchID",
            "LoadTimestamp",
        ],
        "fk_specs": [
            ("EmployeeKey", "DimEmployee", "EmployeeID", "EmployeeID", True, 0, True),
            ("StoreKey", "DimStore", "StoreID", "StoreID", False, 0, True),
            ("SalesTerritoryKey", "DimSalesTerritory", "TerritoryID", "TerritoryID", False, 0, False),
        ],
        "query": """
            SELECT
                soh.OrderDate::DATE AS SalesDateKey,
                e.BusinessEntityID AS EmployeeID,
                s.BusinessEntityID AS StoreID,
                st.TerritoryID AS TerritoryID,
                CAST(SUM(soh.SubTotal) AS DECIMAL(18, 2)) AS SalesAmount,
                CAST(COALESCE(sp.SalesQuota, 0) / 12 AS DECIMAL(18, 2)) AS SalesTarget,
                CAST(
                    CASE
                        WHEN sp.SalesQuota > 0 THEN (SUM(soh.SubTotal) / (sp.SalesQuota / 12)) * 100
                        ELSE 0
                    END AS DECIMAL(10, 4)
                ) AS TargetAttainment,
                COUNT(DISTINCT soh.CustomerID) AS CustomerContactsCount,
                uuid_generate_v4()::TEXT AS ETLBatchID,
                CURRENT_TIMESTAMP AS LoadTimestamp
            FROM Sales.SalesPerson AS sp
            INNER JOIN HumanResources.Employee AS e ON sp.BusinessEntityID = e.BusinessEntityID
            INNER JOIN Sales.SalesOrderHeader AS soh ON sp.BusinessEntityID = soh.SalesPersonID
            LEFT JOIN Sales.Customer AS c ON soh.CustomerID = c.CustomerID
            LEFT JOIN Sales.Store AS s ON c.StoreID = s.BusinessEntityID
            LEFT JOIN Sales.SalesTerritory AS st ON sp.TerritoryID = st.TerritoryID
            GROUP BY
                soh.OrderDate::DATE,
                e.BusinessEntityID,
                s.BusinessEntityID,
                st.TerritoryID,
                sp.SalesQuota
            ORDER BY soh.OrderDate::DATE, e.BusinessEntityID;
        """,
    },
    "FactCustomerFeedback": {
        "insert_columns": [
            "FeedbackDateKey",
            "CustomerKey",
            "EmployeeKey",
            "FeedbackCategoryKey",
            "FeedbackScore",
            "ComplaintCount",
            "ResolutionTimeHours",
            "CSATScore",
            "Comments",
            "Channel",
            "ETLBatchID",
            "LoadTimestamp",
        ],
        "extract_columns": [
            "FeedbackDateKey",
            "CustomerID",
            "EmployeeID",
            "FeedbackCategoryID",
            "FeedbackScore",
            "ComplaintCount",
            "ResolutionTimeHours",
            "CSATScore",
            "Comments",
            "Channel",
            "ETLBatchID",
            "LoadTimestamp",
        ],
        "fk_specs": [
            ("CustomerKey", "DimCustomer", "CustomerID", "CustomerID", False, 0, True),
            ("EmployeeKey", "DimEmployee", "EmployeeID", "EmployeeID", False, 0, True),
            ("FeedbackCategoryKey", "DimProductCategory", "ProductCategoryID", "FeedbackCategoryID", False, 0, False),
        ],
        "query": """
            SELECT
                pr.ReviewDate::DATE AS FeedbackDateKey,
                c.CustomerID AS CustomerID,
                e.BusinessEntityID AS EmployeeID,
                pc.ProductCategoryID AS FeedbackCategoryID,
                pr.Rating AS FeedbackScore,
                CASE WHEN pr.Rating < 3 THEN 1 ELSE 0 END AS ComplaintCount,
                CAST(0 AS DECIMAL(10, 2)) AS ResolutionTimeHours,
                CAST(pr.Rating * 20 AS DECIMAL(5, 2)) AS CSATScore,
                COALESCE(pr.Comments, '') AS Comments,
                'Online' AS Channel,
                uuid_generate_v4()::TEXT AS ETLBatchID,
                CURRENT_TIMESTAMP AS LoadTimestamp
            FROM Production.ProductReview AS pr
            INNER JOIN Production.Product AS p ON pr.ProductID = p.ProductID
            LEFT JOIN Production.ProductSubcategory AS psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
            LEFT JOIN Production.ProductCategory AS pc ON psc.ProductCategoryID = pc.ProductCategoryID
            LEFT JOIN Person.EmailAddress AS ea ON LOWER(pr.EmailAddress) = LOWER(ea.EmailAddress)
            LEFT JOIN Sales.Customer AS c ON ea.BusinessEntityID = c.PersonID
            LEFT JOIN HumanResources.Employee AS e ON e.BusinessEntityID = ea.BusinessEntityID
            ORDER BY pr.ReviewDate::DATE, pr.ProductReviewID;
        """,
    },
    "FactPromotionResponse": {
        "insert_columns": [
            "PromotionDateKey",
            "ProductKey",
            "StoreKey",
            "PromotionKey",
            "SalesDuringCampaign",
            "DiscountUsageCount",
            "CustomerUptakeRate",
            "PromotionROI",
            "ETLBatchID",
            "LoadTimestamp",
        ],
        "extract_columns": [
            "PromotionDateKey",
            "ProductID",
            "StoreID",
            "PromotionID",
            "SalesDuringCampaign",
            "DiscountUsageCount",
            "CustomerUptakeRate",
            "PromotionROI",
            "ETLBatchID",
            "LoadTimestamp",
        ],
        "fk_specs": [
            ("ProductKey", "DimProduct", "ProductID", "ProductID", True, 0, True),
            ("StoreKey", "DimStore", "StoreID", "StoreID", False, 0, True),
            ("PromotionKey", "DimPromotion", "PromotionID", "PromotionID", True, 0, False),
        ],
        "query": """
            SELECT
                soh.OrderDate::DATE AS PromotionDateKey,
                p.ProductID AS ProductID,
                c.StoreID AS StoreID,
                so.SpecialOfferID AS PromotionID,
                CAST(SUM(sod.UnitPrice * sod.OrderQty * (1 - sod.UnitPriceDiscount)) AS DECIMAL(18, 2)) AS SalesDuringCampaign,
                COUNT(*) AS DiscountUsageCount,
                CAST(COUNT(DISTINCT soh.CustomerID)::DECIMAL / NULLIF(COUNT(*), 0)::DECIMAL AS DECIMAL(10, 4)) AS CustomerUptakeRate,
                CAST(0 AS DECIMAL(10, 4)) AS PromotionROI,
                uuid_generate_v4()::TEXT AS ETLBatchID,
                CURRENT_TIMESTAMP AS LoadTimestamp
            FROM Sales.SpecialOfferProduct AS sop
            INNER JOIN Sales.SalesOrderDetail AS sod
                ON sop.SpecialOfferID = sod.SpecialOfferID AND sop.ProductID = sod.ProductID
            INNER JOIN Sales.SalesOrderHeader AS soh ON sod.SalesOrderID = soh.SalesOrderID
            INNER JOIN Sales.SpecialOffer AS so ON sop.SpecialOfferID = so.SpecialOfferID
            INNER JOIN Production.Product AS p ON sop.ProductID = p.ProductID
            LEFT JOIN Sales.Customer AS c ON soh.CustomerID = c.CustomerID
            GROUP BY
                soh.OrderDate::DATE,
                p.ProductID,
                c.StoreID,
                so.SpecialOfferID
            ORDER BY soh.OrderDate::DATE, p.ProductID, so.SpecialOfferID;
        """,
    },
    "FactFinance": {
        "insert_columns": [
            "InvoiceDateKey",
            "CustomerKey",
            "StoreKey",
            "FinanceCategoryKey",
            "InvoiceAmount",
            "PaymentDelayDays",
            "CreditUsagePct",
            "InterestCharges",
            "InvoiceNumber",
            "PaymentStatus",
            "CurrencyCode",
            "ETLBatchID",
            "LoadTimestamp",
        ],
        "extract_columns": [
            "InvoiceDateKey",
            "CustomerID",
            "StoreID",
            "FinanceCategoryKey",
            "InvoiceAmount",
            "PaymentDelayDays",
            "CreditUsagePct",
            "InterestCharges",
            "InvoiceNumber",
            "PaymentStatus",
            "CurrencyCode",
            "ETLBatchID",
            "LoadTimestamp",
        ],
        "fk_specs": [
            ("CustomerKey", "DimCustomer", "CustomerID", "CustomerID", True, 0, True),
            ("StoreKey", "DimStore", "StoreID", "StoreID", False, 0, True),
        ],
        "query": """
            SELECT
                soh.OrderDate::DATE AS InvoiceDateKey,
                c.CustomerID AS CustomerID,
                c.StoreID AS StoreID,
                CASE
                    WHEN soh.CreditCardID IS NOT NULL THEN 5
                    WHEN soh.TotalDue < 100 THEN 1
                    WHEN soh.TotalDue < 1000 THEN 2
                    WHEN soh.TotalDue <= 10000 THEN 3
                    ELSE 4
                END AS FinanceCategoryKey,
                CAST(soh.TotalDue AS DECIMAL(18, 2)) AS InvoiceAmount,
                CASE
                    WHEN soh.ShipDate IS NOT NULL THEN EXTRACT(DAY FROM (soh.ShipDate - soh.DueDate))::INT
                    ELSE 0
                END AS PaymentDelayDays,
                CAST(0 AS DECIMAL(10, 4)) AS CreditUsagePct,
                CAST(0 AS DECIMAL(18, 2)) AS InterestCharges,
                soh.SalesOrderID::TEXT AS InvoiceNumber,
                CASE WHEN soh.Status == 5 THEN 'Shipped' ELSE 'Pending' END AS PaymentStatus,
                'USD' AS CurrencyCode,
                uuid_generate_v4()::TEXT AS ETLBatchID,
                CURRENT_TIMESTAMP AS LoadTimestamp
            FROM Sales.SalesOrderHeader AS soh
            INNER JOIN Sales.Customer AS c ON soh.CustomerID = c.CustomerID
            ORDER BY soh.OrderDate::DATE, soh.SalesOrderID;
        """,
    },
    "FactReturns": {
        "insert_columns": [
            "ReturnDateKey",
            "ProductKey",
            "CustomerKey",
            "StoreKey",
            "ReturnReasonKey",
            "ReturnedQuantity",
            "RefundAmount",
            "RestockingFee",
            "ReturnID",
            "OriginalSalesID",
            "ReturnMethod",
            "ConditionOnReturn",
            "ETLBatchID",
            "LoadTimestamp",
        ],
        "extract_columns": [
            "ReturnDateKey",
            "ProductID",
            "CustomerID",
            "StoreID",
            "ReturnReasonKey",
            "ReturnedQuantity",
            "RefundAmount",
            "RestockingFee",
            "ReturnID",
            "OriginalSalesID",
            "ReturnMethod",
            "ConditionOnReturn",
            "ETLBatchID",
            "LoadTimestamp",
        ],
        "fk_specs": [
            ("ProductKey", "DimProduct", "ProductID", "ProductID", True, 0, True),
            ("CustomerKey", "DimCustomer", "CustomerID", "CustomerID", True, 0, True),
            ("StoreKey", "DimStore", "StoreID", "StoreID", False, 0, True),
        ],
        "query": """
            SELECT
                th.TransactionDate::DATE AS ReturnDateKey,
                p.ProductID AS ProductID,
                c.CustomerID AS CustomerID,
                c.StoreID AS StoreID,
                CASE
                    WHEN sr.ScrapReasonID = 3 OR sr.ScrapReasonID = 7 THEN 4::BIGINT
                    WHEN sr.ScrapReasonID = 12 OR sr.ScrapReasonID = 13 THEN 3::BIGINT
                    WHEN sr.ScrapReasonID <= 2 OR (sr.ScrapReasonID >= 4 AND sr.ScrapReasonID <= 6) OR sr.ScrapReasonID = 10 THEN 2::BIGINT
                    ELSE 1::BIGINT
                END AS ReturnReasonKey,
                ABS(th.Quantity) AS ReturnedQuantity,
                CAST(ABS(th.ActualCost * th.Quantity) AS DECIMAL(18, 2)) AS RefundAmount,
                CAST(0 AS DECIMAL(18, 2)) AS RestockingFee,
                th.TransactionID::TEXT AS ReturnID,
                th.ReferenceOrderID::TEXT AS OriginalSalesID,
                'Direct' AS ReturnMethod,
                'Unknown' AS ConditionOnReturn,
                uuid_generate_v4()::TEXT AS ETLBatchID,
                CURRENT_TIMESTAMP AS LoadTimestamp
            FROM Production.TransactionHistory AS th
            INNER JOIN Production.Product AS p ON th.ProductID = p.ProductID
            LEFT JOIN Production.WorkOrder AS wo ON th.ReferenceOrderID = wo.WorkOrderID
            LEFT JOIN Production.ScrapReason AS sr ON wo.ScrapReasonID = sr.ScrapReasonID
            INNER JOIN Sales.SalesOrderDetail AS sod ON th.ReferenceOrderID = sod.SalesOrderID
                AND th.ReferenceOrderLineID = sod.SalesOrderDetailID
            INNER JOIN Sales.SalesOrderHeader AS soh ON sod.SalesOrderID = soh.SalesOrderID
            LEFT JOIN Sales.Customer AS c ON soh.CustomerID = c.CustomerID
            WHERE th.TransactionType = 'S' AND th.Quantity < 0
            ORDER BY th.TransactionDate::DATE, th.TransactionID;
        """,
    },
}


def _resolve_and_build_rows(
    *,
    fact_table: str,
    cleaned_rows: Sequence[Sequence[Any]],
    extract_columns: Sequence[str],
    insert_columns: Sequence[str],
    fk_specs: Sequence[FKSpec],
    ch_client,
    batch_id: str,
    task_name: str,
    dim_cache: Optional[Dict[Tuple[str, str, str, bool], Dict[Any, int]]] = None,
) -> Tuple[List[Tuple[Any, ...]], List[Tuple[Any, ...]]]:
    idx = {c: i for i, c in enumerate(extract_columns)}
    if dim_cache is None:
        dim_cache = {}
    dim_maps: Dict[Tuple[str, str, str, bool], Dict[Any, int]] = {}
    for insert_col, dim_table, dim_nat_col, extract_col, required, default_val, is_scd2 in fk_specs:
        cache_key = (dim_table, dim_nat_col, extract_col, is_scd2)
        if cache_key in dim_cache:
            dim_maps[cache_key] = dim_cache[cache_key]
            continue
        if cache_key in dim_maps:
            continue
        values = [r[idx[extract_col]] for r in cleaned_rows if extract_col in idx]
        dim_maps[cache_key] = bulk_lookup_dimension_keys(
            ch_client,
            dim_table,
            dim_nat_col,
            values,
            is_scd2=is_scd2,
        )
        dim_cache[cache_key] = dim_maps[cache_key]
    valid_rows: List[Tuple[Any, ...]] = []
    error_rows: List[Tuple[Any, ...]] = []
    fk_by_insert = {s[0]: s for s in fk_specs}
    for r in cleaned_rows:
        try:
            out: List[Any] = []
            for col in insert_columns:
                if col in fk_by_insert:
                    insert_col, dim_table, dim_nat_col, extract_col, required, default_val, is_scd2 = fk_by_insert[col]
                    nat_val = r[idx[extract_col]] if extract_col in idx else None
                    if nat_val is None:
                        if required:
                            raise ValueError(f"{extract_col} is NULL (required for {col})")
                        out.append(default_val)
                        continue
                    cache_key = (dim_table, dim_nat_col, extract_col, is_scd2)
                    m = dim_maps[cache_key]
                    sk = m.get(nat_val)
                    if sk is None:
                        if required:
                            suffix = " (IsCurrent=1)" if is_scd2 else ""
                            raise ValueError(
                                f"FK miss: {dim_table}.{dim_nat_col}={nat_val} not found{suffix}"
                            )
                        out.append(default_val)
                        continue
                    out.append(sk)
                else:
                    if col not in idx:
                        raise ValueError(
                            f"Insert column '{col}' not present in extract_columns for {fact_table}"
                        )
                    out.append(r[idx[col]])
            valid_rows.append(tuple(out))
        except Exception as e:
            error_rows.append(
                build_error_record(
                    source_table=fact_table,
                    record_natural_key=_record_nk(fact_table, r, idx),
                    error_type="ForeignKeyMiss",
                    error_message=str(e),
                    failed_data={"sample": {k: r[idx[k]] for k in list(idx)[:8] if k in idx}},
                    task_name=task_name,
                    batch_id=batch_id,
                    severity="Warning",
                    is_recoverable=1,
                )
            )
    return valid_rows, error_rows


@dag(
    dag_id="adventureworks_fact_population",
    dag_display_name="Populate Fact Tables",
    schedule="@hourly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["adventureworks", "facts", "population"],
)
def populate():
    @task
    def extract_resolve_and_load(fact_table: str) -> int:
        cfg = FACTS[fact_table]
        query = cfg["query"]
        extract_columns = cfg["extract_columns"]
        insert_columns = cfg["insert_columns"]
        fk_specs = cfg.get("fk_specs", [])
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        task_name = f"load_fact_{fact_table.lower()}"
        ch_client = ch()
        dim_cache: Dict[Tuple[str, str, str, bool], Dict[Any, int]] = {}
        inserted_total = 0
        error_total = 0
        conn = pg()
        cur = conn.cursor(name=f"{fact_table.lower()}_cursor")
        cur.itersize = PG_FETCH_SIZE
        cur.execute(query)
        try:
            while True:
                rows = cur.fetchmany(PG_FETCH_SIZE)
                if not rows:
                    break
                cleaned = clean_values_in_rows(rows, extract_columns)
                try:
                    valid_rows, error_rows = _resolve_and_build_rows(
                        fact_table=fact_table,
                        cleaned_rows=cleaned,
                        extract_columns=extract_columns,
                        insert_columns=insert_columns,
                        fk_specs=fk_specs,
                        ch_client=ch_client,
                        batch_id=batch_id,
                        task_name=task_name,
                        dim_cache=dim_cache,
                    )
                except Exception as e:
                    err = build_error_record(
                        source_table=fact_table,
                        record_natural_key="batch_level_failure",
                        error_type="TransformFailed",
                        error_message=str(e),
                        failed_data={"sample_rows": cleaned[:5]},
                        task_name=task_name,
                        batch_id=batch_id,
                        severity="Error",
                        is_recoverable=1,
                    )
                    insert_error_records(ch_client, [err])
                    error_total += 1
                    raise
                for i in range(0, len(valid_rows), CH_INSERT_BATCH_SIZE):
                    batch = valid_rows[i : i + CH_INSERT_BATCH_SIZE]
                    if not batch:
                        continue
                    try:
                        ch_client.insert(
                            f"ADVENTUREWORKS_DWS.{fact_table}",
                            batch,
                            column_names=insert_columns,
                            settings=CLICKHOUSE_INSERT_SETTINGS,
                        )
                        inserted_total += len(batch)
                    except Exception as e:
                        err = build_error_record(
                            source_table=fact_table,
                            record_natural_key="insert_batch_failure",
                            error_type="InsertFailed",
                            error_message=str(e),
                            failed_data={
                                "sample": batch[0] if batch else None,
                                "batch_size": len(batch),
                                "insert_columns": insert_columns,
                            },
                            task_name=task_name,
                            batch_id=batch_id,
                            severity="Error",
                            is_recoverable=1,
                        )
                        insert_error_records(ch_client, [err])
                        error_total += len(batch)
                        raise
                if error_rows:
                    insert_error_records(ch_client, error_rows)
                    error_total += len(error_rows)
            print(f"[{fact_table}] Inserted={inserted_total} ErrorRecords={error_total}")
            return inserted_total
        except Exception as e:
            try:
                err = build_error_record(
                    source_table=fact_table,
                    record_natural_key="task_level_failure",
                    error_type="ExtractOrLoadFailed",
                    error_message=str(e),
                    failed_data={
                        "fact_table": fact_table,
                        "query_snippet": query[:500],
                    },
                    task_name=task_name,
                    batch_id=batch_id,
                    severity="Error",
                    is_recoverable=1,
                )
                insert_error_records(ch_client, [err])
            except Exception:
                pass
            raise
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            try:
                ch_client.close()
            except Exception:
                pass

    for table_name in FACTS.keys():
        extract_resolve_and_load.override(
            task_id=f"extract_resolve_load_{table_name.lower()}"
        )(table_name)


populate()


