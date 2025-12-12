from datetime import datetime

from extra_functions import pg, ch, clean_values_in_rows, KEY_GENERATION, partition_rows_fixed_batch
from airflow.sdk import dag, task
import pickle

FACTS = {
    "FactSales": {
        "columns": [
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
            "LineTotal"
        ],
        "query": """
                SELECT soh.OrderDate::DATE AS SalesDateKey,

                    ('x' || MD5(c.CustomerID::TEXT || COALESCE(c.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS CustomerKey,

                    ('x' || MD5(p.ProductID::TEXT || COALESCE(p.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS ProductKey,

                    COALESCE(('x' || MD5(s.BusinessEntityID::TEXT || COALESCE(s.ModifiedDate::TEXT, '')))
                            ::bit(32)::BIGINT, 0::BIGINT) AS StoreKey,

                    COALESCE(('x' || MD5(e.BusinessEntityID::TEXT || COALESCE(e.ModifiedDate::TEXT, '')))
                            ::bit(32)::BIGINT, 0::BIGINT) AS EmployeeKey,

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
                          LEFT JOIN Sales.Store AS s ON c.StoreID = s.BusinessEntityID
                          LEFT JOIN Production.Product AS p ON sod.ProductID = p.ProductID
                          LEFT JOIN HumanResources.Employee AS e ON soh.SalesPersonID = e.BusinessEntityID
                 ORDER BY soh.OrderDate::DATE, sod.SalesOrderID, sod.SalesOrderDetailID;
                 """
    },

    "FactPurchases": {
        "columns": [
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
            "LineTotal"
        ],
        "query": """
                 SELECT poh.OrderDate::DATE AS PurchaseDateKey,

                -- VendorKey: Hash of VendorID + ModifiedDate 
                     ('x' || MD5(v.BusinessEntityID::TEXT || COALESCE(v.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS VendorKey,

                -- ProductKey: Hash of ProductID + ModifiedDate 
                     ('x' || MD5(p.ProductID::TEXT || COALESCE(p.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS ProductKey, pod.PurchaseOrderID,
                        pod.PurchaseOrderDetailID,
                        pod.OrderQty                                         AS QuantityBought,
                        CAST(pod.UnitPrice * pod.OrderQty AS DECIMAL(18, 2)) AS PurchaseAmount,
                        CAST(0 AS DECIMAL(18, 2))                            AS DiscountAmount,
                        1                                                    AS NumberOfTransactions,
                        CAST(pod.UnitPrice AS DECIMAL(18, 2))                AS UnitPrice,
                        CAST(0 AS DECIMAL(18, 4))                            AS UnitPriceDiscount,
                        CAST(pod.UnitPrice * pod.OrderQty AS DECIMAL(18, 2)) AS LineTotal
                 FROM Purchasing.PurchaseOrderDetail AS pod
                          INNER JOIN Purchasing.PurchaseOrderHeader AS poh ON pod.PurchaseOrderID = poh.PurchaseOrderID
                          INNER JOIN Production.Product AS p ON pod.ProductID = p.ProductID
                          INNER JOIN Purchasing.Vendor AS v ON poh.VendorID = v.BusinessEntityID
                 ORDER BY poh.OrderDate::DATE, pod.PurchaseOrderID, pod.PurchaseOrderDetailID;
                 """
    },

    "FactInventory": {
        "columns": [
            "InventoryDateKey",
            "ProductKey",
            "StoreKey",
            "WarehouseKey",
            "QuantityOnHand",
            "StockAging",
            "ReorderLevel",
            "SafetyStockLevels",
            "SnapshotCreatedDateTime",
            "ETLBatchID"
        ],
        "query": """
                SELECT pi.ModifiedDate::DATE AS InventoryDateKey,

                    ('x' || MD5(p.ProductID::TEXT || COALESCE(p.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS ProductKey,

                    ('x' || MD5(s.BusinessEntityID::TEXT || COALESCE(s.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS StoreKey,

                    ('x' || MD5(l.LocationID::TEXT || COALESCE(l.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS WarehouseKey,

                    pi.Quantity AS QuantityOnHand,
                    EXTRACT(DAY FROM (CURRENT_DATE - p.SellStartDate))::INT AS StockAging, p.ReorderPoint AS ReorderLevel,
                    p.SafetyStockLevel AS SafetyStockLevels,
                    CURRENT_TIMESTAMP  AS SnapshotCreatedDateTime,
                    uuid_generate_v4() ::TEXT AS ETLBatchID
                FROM Production.ProductInventory AS pi
                    INNER JOIN Production.Product AS p ON pi.ProductID = p.ProductID
                    INNER JOIN Production.Location AS l ON pi.LocationID = l.LocationID
                    INNER JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
                    INNER JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
                    INNER JOIN Sales.Customer c ON soh.CustomerID = c.CustomerID
                    INNER JOIN Sales.Store s ON c.StoreID = s.BusinessEntityID WHERE c.StoreID IS NOT NULL
                ORDER BY pi.ModifiedDate::DATE, pi.ProductID, pi.LocationID; 
                 """
    },

    "FactProduction": {
        "columns": [
            "ProductionRunID",
            "ProductionDateKey",
            "ProductKey",
            "SupervisorKey",
            "UnitsProduced",
            "ProductionTimeHours",
            "ScrapRatePercent",
            "DefectCount",
            "ETLBatchID",
            "LoadTimestamp"
        ],
        "query": """
                SELECT 
                
                    ('x' || MD5(wo.WorkOrderID::TEXT || wo.ProductID::TEXT))::bit(32)::BIGINT AS ProductionRunID, 
                    
                    wo.StartDate::DATE AS ProductionDateKey,

                    ('x' || MD5(p.ProductID::TEXT || COALESCE(p.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS ProductKey,
                    
                    CASE
                        WHEN e.JobTitle = '%Supervisor%' 
                            THEN ('x' || MD5(e.BusinessEntityID::TEXT || COALESCE(e.ModifiedDate::TEXT, '')))::bit(32)::BIGINT
                        ELSE 0
                    END AS EmployeeKey,

                    wo.OrderQty AS UnitsProduced,
                    CAST(
                        EXTRACT(EPOCH FROM (COALESCE(wo.EndDate, CURRENT_TIMESTAMP) - wo.StartDate)) / 3600 AS DECIMAL(10, 2)
                    ) AS ProductionTimeHours,
                    CAST(
                        CASE WHEN wo.OrderQty > 0 THEN (wo.ScrappedQty::DECIMAL / wo.OrderQty::DECIMAL) * 100 ELSE 0 END AS DECIMAL(5, 2)
                    ) AS ScrapRatePercent,
                    wo.ScrappedQty                                                                                                          AS DefectCount,
                    uuid_generate_v4()::TEXT AS ETLBatchID, CURRENT_TIMESTAMP AS LoadTimestamp
                FROM Production.WorkOrder AS wo
                    INNER JOIN Production.Product AS p ON wo.ProductID = p.ProductID
                    JOIN Production.ProductReview AS pr ON p.ProductID = pr.ProductID
                    JOIN Person.EmailAddress AS ea ON LOWER(ea.EmailAddress) = LOWER(pr.EmailAddress)
                    JOIN HumanResources.Employee AS e ON e.BusinessEntityID = ea.BusinessEntityID
                    JOIN Person.Person AS person ON person.BusinessEntityID = e.BusinessEntityID
                ORDER BY wo.StartDate::DATE, wo.WorkOrderID;
                """
    },

    "FactEmployeeSales": {
        "columns": [
            "SalesDateKey",
            "EmployeeKey",
            "StoreKey",
            "SalesTerritoryKey",
            "SalesAmount",
            "SalesTarget",
            "TargetAttainment",
            "CustomerContactsCount",
            "ETLBatchID",
            "LoadTimestamp"
        ],
        "query": """
                SELECT soh.OrderDate::DATE AS SalesDateKey,

                    ('x' || MD5(e.BusinessEntityID::TEXT || COALESCE(e.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS EmployeeKey,

                    ('x' || MD5(s.BusinessEntityID::TEXT || COALESCE(s.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS StoreKey,

                    COALESCE(('x' || MD5(COALESCE(st.TerritoryID::TEXT, '') || COALESCE(st.Name::TEXT, '') ||
                        COALESCE(st."group"::TEXT, '') || COALESCE(st.CountryRegionCode::TEXT, '')))
                        ::bit(32)::BIGINT, 0::BIGINT
                    ) AS SalesTerritoryKey,

                    CAST(SUM(soh.SubTotal) AS DECIMAL(18, 2)) AS SalesAmount,
                    CAST(COALESCE(sp.SalesQuota, 0) / 12 AS DECIMAL(18, 2)) AS SalesTarget,
                    CAST(
                        CASE
                            WHEN sp.SalesQuota > 0 THEN (SUM(soh.SubTotal) / (sp.SalesQuota / 12)) * 100
                            ELSE 0
                        END AS DECIMAL(10, 4)
                    )AS TargetAttainment,
                    
                    COUNT(DISTINCT soh.CustomerID) AS CustomerContactsCount,
                    uuid_generate_v4()::TEXT AS ETLBatchID, CURRENT_TIMESTAMP AS LoadTimestamp
                FROM Sales.SalesPerson AS sp
                    INNER JOIN HumanResources.Employee AS e ON sp.BusinessEntityID = e.BusinessEntityID
                    INNER JOIN Sales.SalesOrderHeader AS soh ON sp.BusinessEntityID = soh.SalesPersonID
                    INNER JOIN Sales.Store AS s ON sp.BusinessEntityID = s.SalesPersonID
                    LEFT JOIN Sales.SalesTerritory AS st ON sp.TerritoryID = st.TerritoryID
                GROUP BY soh.OrderDate::DATE,
                e.BusinessEntityID,
                e.ModifiedDate,
                st.TerritoryID,
                st.Name,
                st."group",
                st.CountryRegionCode,
                sp.SalesQuota,
                s.BusinessEntityID
                ORDER BY soh.OrderDate::DATE, e.BusinessEntityID;
                 """
    },

    "FactCustomerFeedback": {
        "columns": [
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
            "LoadTimestamp"
        ],
        "query": """
                SELECT pr.ReviewDate::DATE AS FeedbackDateKey,

                    COALESCE(
                        ('x' || MD5(c.CustomerID::TEXT || COALESCE(c.ModifiedDate::TEXT, '')))
                        ::bit (32)::BIGINT, 0::BIGINT) AS CustomerKey,

                    COALESCE(('x' || MD5(e.BusinessEntityID::TEXT || COALESCE(e.ModifiedDate::TEXT, '')))
                        ::bit(32)::BIGINT, 0::BIGINT) AS EmployeeKey,

                    COALESCE(('x' || MD5(COALESCE(pc.ProductCategoryID::TEXT, '0') || COALESCE(pc.ModifiedDate::TEXT, '')))
                        ::bit(32)::BIGINT, 0::BIGINT) AS FeedbackCategoryKey,

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
                            LEFT JOIN Person.Person AS person ON person.BusinessEntityID = e.BusinessEntityID
                    ORDER BY pr.ReviewDate::DATE, pr.ProductReviewID;
                 """
    },

    "FactPromotionResponse": {
        "columns": [
            "PromotionDateKey",
            "ProductKey",
            "StoreKey",
            "PromotionKey",
            "SalesDuringCampaign",
            "DiscountUsageCount",
            "CustomerUptakeRate",
            "PromotionROI",
            "ETLBatchID",
            "LoadTimestamp"
        ],
        "query": """
                SELECT soh.OrderDate::DATE AS PromotionDateKey,

                    ('x' || MD5(p.ProductID::TEXT || COALESCE(p.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS ProductKey,

                    COALESCE(('x' || MD5(s.BusinessEntityID::TEXT || COALESCE(s.ModifiedDate::TEXT, '')))
                        ::bit(32)::BIGINT, 0::BIGINT) AS StoreKey,

                    ('x' || MD5(so.SpecialOfferID::TEXT || COALESCE(so.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS PromotionKey, CAST(SUM(sod.UnitPrice * sod.OrderQty * (1 - sod.UnitPriceDiscount)) AS DECIMAL(18, 2)) AS SalesDuringCampaign,
                    
                    COUNT(*) AS DiscountUsageCount,
                    CAST(COUNT(DISTINCT soh.CustomerID)::DECIMAL / NULLIF(COUNT(*), 0)::DECIMAL
                        AS DECIMAL(10, 4)) AS CustomerUptakeRate,
                    CAST(0 AS DECIMAL(10, 4)) AS PromotionROI,
                    uuid_generate_v4()::TEXT AS ETLBatchID, 
                    CURRENT_TIMESTAMP AS LoadTimestamp
                FROM Sales.SpecialOfferProduct AS sop
                INNER JOIN Sales.SalesOrderDetail AS sod
                            ON sop.SpecialOfferID = sod.SpecialOfferID
                            AND sop.ProductID = sod.ProductID
                INNER JOIN Sales.SalesOrderHeader AS soh ON sod.SalesOrderID = soh.SalesOrderID
                INNER JOIN Sales.SpecialOffer AS so ON sop.SpecialOfferID = so.SpecialOfferID
                INNER JOIN Production.Product AS p ON sop.ProductID = p.ProductID
                LEFT JOIN Sales.Customer AS c ON soh.CustomerID = c.CustomerID
                LEFT JOIN Sales.Store AS s ON c.StoreID = s.BusinessEntityID
                GROUP BY soh.OrderDate::DATE,
                p.ProductID,
                p.ModifiedDate,
                s.BusinessEntityID,
                s.ModifiedDate,
                so.SpecialOfferID,
                so.ModifiedDate
            ORDER BY soh.OrderDate::DATE, p.ProductID, so.SpecialOfferID;
            """
    },

    "FactFinance": {
        "columns": [
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
            "LoadTimestamp"
        ],
        "query": """
                SELECT soh.OrderDate::DATE AS InvoiceDateKey,

                    ('x' || MD5(c.CustomerID::TEXT || COALESCE(c.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS CustomerKey,

                    COALESCE(('x' || MD5(s.BusinessEntityID::TEXT || COALESCE(s.ModifiedDate::TEXT, '')))
                        ::bit(32)::BIGINT, 0::BIGINT) AS StoreKey,

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
                    CASE 
                        WHEN soh.Status = 5 THEN 'Shipped' ELSE 'Pending'
                    END AS PaymentStatus,
                    
                    'USD' AS CurrencyCode,
                    
                    uuid_generate_v4()::TEXT AS ETLBatchID,
                    
                    CURRENT_TIMESTAMP AS LoadTimestamp
                FROM Sales.SalesOrderHeader AS soh
                INNER JOIN Sales.Customer AS c ON soh.CustomerID = c.CustomerID
                LEFT JOIN Sales.Store AS s ON c.StoreID = s.BusinessEntityID
                ORDER BY soh.OrderDate::DATE, soh.SalesOrderID;
                """
    },

    "FactReturns": {
        "columns": [
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
            "LoadTimestamp"
        ],
        "query": """
                SELECT th.TransactionDate::DATE AS ReturnDateKey,
 
                    ('x' || MD5(p.ProductID::TEXT || COALESCE(p.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS ProductKey,

                    ('x' || MD5(c.CustomerID::TEXT || COALESCE(c.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS CustomerKey,

                    ('x' || MD5(s.BusinessEntityID::TEXT || COALESCE(s.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS StoreKey,
 
                    CASE
                        WHEN sr.ScrapReasonID = 3 OR sr.ScrapReasonID = 7 THEN 4::BIGINT
                        WHEN sr.ScrapReasonID = 12 OR sr.ScrapReasonID = 13 THEN 3::BIGINT
                        WHEN sr.ScrapReasonID <= 2 OR (sr.ScrapReasonID >= 4
                            AND sr.ScrapReasonID <= 6) OR sr.ScrapReasonID = 10 THEN 2::BIGINT
                        ELSE 1::BIGINT
                    END AS ReturnReasonKey,
                    
                        ABS(th.Quantity) AS ReturnedQuantity,
                        CAST(ABS(th.ActualCost * th.Quantity) AS DECIMAL(18, 2)) AS RefundAmount,
                        CAST(0 AS DECIMAL(18, 2)) AS RestockingFee,
                        th.TransactionID::TEXT AS ReturnID, th.ReferenceOrderID::TEXT AS OriginalSalesID, 'Direct' AS ReturnMethod,
                        'Unknown' AS ConditionOnReturn,
                        uuid_generate_v4()::TEXT AS ETLBatchID, CURRENT_TIMESTAMP AS LoadTimestamp
                FROM Production.TransactionHistory AS th
                    INNER JOIN Production.Product AS p ON th.ProductID = p.ProductID
                    LEFT JOIN Production.WorkOrder AS wo ON th.ReferenceOrderID = wo.WorkOrderID
                    LEFT JOIN Production.ScrapReason AS sr ON wo.ScrapReasonID = sr.ScrapReasonID
                    INNER JOIN Sales.SalesOrderDetail AS sod ON th.ReferenceOrderID     = sod.SalesOrderID 
                        AND th.ReferenceOrderLineID = sod.SalesOrderDetailID
                    INNER JOIN Sales.SalesOrderHeader AS soh ON sod.SalesOrderID = soh.SalesOrderID
                    LEFT JOIN Sales.Customer AS c ON soh.CustomerID = c.CustomerID
                    LEFT JOIN Sales.Store AS s ON c.StoreID = s.BusinessEntityID
                
                    WHERE th.TransactionType = 'S' AND th.Quantity < 0
                
                ORDER BY th.TransactionDate::DATE, th.TransactionID;
                """
    }
}


@dag(
    dag_id="adventureworks_fact_population",
    dag_display_name="Populate Fact Tables",
    schedule="@hourly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["adventureworks", "facts", "population"]
)
def populate():
    @task
    def mold_data(query, columns, table):
        conn = pg()
        cur = conn.cursor()

        cur.execute(query)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        cleaned_rows = clean_values_in_rows(rows, columns)

        with open(f'/tmp/fact_{table.lower}_data.pkl', 'wb') as file:
            pickle.dump(cleaned_rows, file)

        return f'/tmp/fact_{table.lower}_data.pkl'

    @task
    def load_data(data_path, table, columns):

        with open(data_path, 'rb') as file:
            data = pickle.load(file)

        cleaned_data = clean_values_in_rows(data, columns)

        try:
            client = ch()

            partitioned = partition_rows_fixed_batch(cleaned_data, batch_size=500)

            total_inserted = 0

            for partition_key, rows in partitioned.items():
                print(f"Inserting {len(rows)} rows for partition {partition_key}")
                client.insert(
                    f"ADVENTUREWORKS_DWS.{table}",
                    rows,
                    column_names=columns
                )
                total_inserted += len(rows)

            print(f"Inserted {total_inserted} rows across {len(partitioned)} partitions")
            return total_inserted

        except Exception as e:
            print(f"Error loading data: {str(e)}")
            print(f"First row causing issue: {data[0] if data else 'No data'}")
            raise
    for table, content in FACTS.items():
        qry = content["query"]
        col = content["columns"]
        md = mold_data.override(task_id=f"mold_existing_data_{table.lower()}")(qry, col, table)
        ld = load_data.override(task_id=f"load_to_clickhouse_{table.lower()}")(md, table, col)

        md >> ld


populate()