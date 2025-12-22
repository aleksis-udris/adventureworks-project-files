from datetime import date, datetime
import json
import pickle
import gc
from airflow.sdk import dag, task

from extra_functions import (
    pg,
    ch,
    clean_values_in_rows,
    check_insert,
    build_error_record,
    insert_error_records,
)


FAR_FUTURE_DATE = date(2100, 12, 31)


DIMENSIONS = {
    "DimCustomer": {
        "Columns": [
            "CustomerKey",
            "CustomerID",
            "CustomerName",
            "Email",
            "Phone",
            "City",
            "StateProvince",
            "Country",
            "PostalCode",
            "CustomerSegment",
            "CustomerType",
            "AccountStatus",
            "CreditLimit",
            "AnnualIncome",
            "YearsSinceFirstPurchase",
            "ValidFromDate",
            "ValidToDate",
            "IsCurrent",
            "SourceUpdateDate",
            "EffectiveStartDate",
            "EffectiveEndDate",
            "Version",
        ],
        "NaturalKey": "CustomerID",
        "Query": """
WITH customer_sales AS (
    SELECT
        CustomerID,
        COUNT(*) AS OrderCount,
        COALESCE(SUM(TotalDue), 0) AS TotalLifetimeValue,
        COALESCE(MAX(TotalDue), 0) AS MaxOrderValue,
        MIN(OrderDate) AS FirstOrderDate
    FROM Sales.SalesOrderHeader
    GROUP BY CustomerID
),
customer_base AS (
    SELECT
        ('x' || MD5(c.CustomerID::TEXT || COALESCE(c.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS CustomerKey,
        c.CustomerID AS CustomerID,
        CASE
            WHEN c.PersonID IS NOT NULL THEN COALESCE(p.FirstName || ' ' || COALESCE(p.MiddleName || ' ', '') || p.LastName, 'Unknown')
            WHEN c.StoreID IS NOT NULL THEN COALESCE(s.Name, 'Unknown Store')
            ELSE 'Unknown Customer'
        END AS CustomerName,
        COALESCE(ea.EmailAddress, '') AS Email,
        COALESCE(pp.PhoneNumber, '') AS Phone,
        COALESCE(addr.City, 'Unknown') AS City,
        COALESCE(sp.Name, 'Unknown') AS StateProvince,
        COALESCE(cr.Name, 'Unknown') AS Country,
        COALESCE(addr.PostalCode, '00000') AS PostalCode,
        CASE
            WHEN pd.YearlyIncome LIKE '%Greater%150000%' THEN 'Premium'
            WHEN pd.YearlyIncome LIKE '%75000%100000%' THEN 'Gold'
            WHEN pd.YearlyIncome LIKE '%50000%75000%' THEN 'Silver'
            WHEN pd.YearlyIncome LIKE '%25000%50000%' THEN 'Bronze'
            WHEN c.StoreID IS NOT NULL THEN 'Business'
            ELSE 'Standard'
        END AS CustomerSegment,
        CASE
            WHEN c.PersonID IS NOT NULL AND c.StoreID IS NULL THEN 'Individual'
            WHEN c.StoreID IS NOT NULL THEN 'Business'
            ELSE 'Unknown'
        END AS CustomerType,
        CASE
            WHEN cs.OrderCount > 0 AND cs.FirstOrderDate > CURRENT_DATE - INTERVAL '1 year' THEN 'Active'
            WHEN cs.OrderCount > 0 THEN 'Inactive'
            ELSE 'New'
        END AS AccountStatus,
        ROUND(
            CASE WHEN c.StoreID IS NOT NULL THEN 10000.00 ELSE 1000.00 END
            * CASE
                WHEN cs.OrderCount >= 20 THEN 3.0
                WHEN cs.OrderCount >= 10 THEN 2.5
                WHEN cs.OrderCount >= 5 THEN 2.0
                WHEN cs.OrderCount >= 1 THEN 1.5
                ELSE 1.0
            END
            * CASE
                WHEN cs.TotalLifetimeValue > 100000 THEN 2.0
                WHEN cs.TotalLifetimeValue > 50000 THEN 1.5
                WHEN cs.TotalLifetimeValue > 10000 THEN 1.2
                ELSE 1.0
            END
            * CASE
                WHEN pd.YearlyIncome LIKE '%Greater%150000%' THEN 1.5
                WHEN pd.YearlyIncome LIKE '%75000%' THEN 1.2
                ELSE 1.0
            END
        , 2) AS CreditLimit,
        CASE
            WHEN pd.YearlyIncome LIKE '%Greater%150000%' THEN 175000.00
            WHEN pd.YearlyIncome LIKE '%100000%150000%' THEN 125000.00
            WHEN pd.YearlyIncome LIKE '%75000%100000%' THEN 87500.00
            WHEN pd.YearlyIncome LIKE '%50000%75000%' THEN 62500.00
            WHEN pd.YearlyIncome LIKE '%25000%50000%' THEN 37500.00
            WHEN pd.YearlyIncome LIKE '%10000%25000%' THEN 17500.00
            ELSE 0.00
        END AS AnnualIncome,
        COALESCE(EXTRACT(YEAR FROM AGE(CURRENT_DATE, cs.FirstOrderDate))::INT, 0) AS YearsSinceFirstPurchase,
        COALESCE(c.ModifiedDate, CURRENT_DATE) AS ValidFromDate,
        DATE '9999-12-31' AS ValidToDate,
        1 AS IsCurrent,
        COALESCE(c.ModifiedDate, CURRENT_DATE) AS SourceUpdateDate,
        COALESCE(c.ModifiedDate, CURRENT_DATE) AS EffectiveStartDate,
        DATE '9999-12-31' AS EffectiveEndDate,
        1::BIGINT AS Version
    FROM Sales.Customer AS c
    LEFT JOIN Person.Person AS p ON c.PersonID = p.BusinessEntityID
    LEFT JOIN Sales.Store AS s ON c.StoreID = s.BusinessEntityID
    LEFT JOIN Person.EmailAddress AS ea ON ea.BusinessEntityID = c.PersonID
    LEFT JOIN Person.PersonPhone AS pp ON pp.BusinessEntityID = c.PersonID
    LEFT JOIN Person.BusinessEntityAddress AS bea ON bea.BusinessEntityID = COALESCE(c.PersonID, c.StoreID)
    LEFT JOIN Person.Address AS addr ON addr.AddressID = bea.AddressID
    LEFT JOIN Person.StateProvince AS sp ON sp.StateProvinceID = addr.StateProvinceID
    LEFT JOIN Person.CountryRegion AS cr ON cr.CountryRegionCode = sp.CountryRegionCode
    LEFT JOIN Sales.vPersonDemographics AS pd ON c.PersonID = pd.BusinessEntityID
    LEFT JOIN customer_sales AS cs ON c.CustomerID = cs.CustomerID
)
SELECT * FROM customer_base
ORDER BY CustomerKey;
""",
    },
    "DimProduct": {
        "Columns": [
            "ProductKey",
            "ProductID",
            "ProductName",
            "SKU",
            "Category",
            "SubCategory",
            "Brand",
            "ListPrice",
            "Cost",
            "ProductStatus",
            "Color",
            "Size",
            "Weight",
            "ValidFromDate",
            "ValidToDate",
            "IsCurrent",
            "SourceUpdateDate",
            "EffectiveStartDate",
            "EffectiveEndDate",
            "Version",
        ],
        "NaturalKey": "ProductID",
        "Query": """
            SELECT
                ('x' || MD5(p.ProductID::TEXT || COALESCE(p.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS ProductKey,
                p.ProductID AS ProductID,
                p.Name AS ProductName,
                p.ProductNumber AS SKU,
                COALESCE(pc.Name, 'Uncategorized') AS Category,
                COALESCE(psc.Name, 'None') AS SubCategory,
                COALESCE(
                    pm.Name,
                    CASE p.ProductLine
                        WHEN 'R' THEN 'Road'
                        WHEN 'M' THEN 'Mountain'
                        WHEN 'T' THEN 'Touring'
                        WHEN 'S' THEN 'Standard'
                        ELSE 'Generic'
                    END
                ) AS Brand,
                CAST(p.ListPrice AS DECIMAL(18, 2)) AS ListPrice,
                CAST(p.StandardCost AS DECIMAL(18, 2)) AS Cost,
                CASE
                    WHEN p.DiscontinuedDate IS NOT NULL THEN 'Discontinued'
                    WHEN p.SellEndDate IS NOT NULL AND p.SellEndDate < CURRENT_DATE THEN 'Inactive'
                    WHEN p.SellStartDate > CURRENT_DATE THEN 'Pending'
                    ELSE 'Active'
                END AS ProductStatus,
                COALESCE(p.Color, 'N/A') AS Color,
                COALESCE(p.Size, 'N/A') AS Size,
                COALESCE(CAST(p.Weight AS DECIMAL(10, 3)), 0.0) AS Weight,
                COALESCE(p.SellStartDate, p.ModifiedDate) AS ValidFromDate,
                DATE '9999-12-31' AS ValidToDate,
                CASE
                    WHEN p.SellEndDate IS NULL OR p.SellEndDate > CURRENT_DATE THEN 1
                    ELSE 0
                END AS IsCurrent,
                p.ModifiedDate AS SourceUpdateDate,
                COALESCE(p.SellStartDate, p.ModifiedDate) AS EffectiveStartDate,
                COALESCE(p.SellEndDate, DATE '9999-12-31') AS EffectiveEndDate,
                1::BIGINT AS Version
            FROM Production.Product AS p
            LEFT JOIN Production.ProductSubcategory AS psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
            LEFT JOIN Production.ProductCategory AS pc ON psc.ProductCategoryID = pc.ProductCategoryID
            LEFT JOIN Production.ProductModel AS pm ON p.ProductModelID = pm.ProductModelID
            ORDER BY p.ProductID;
        """,
    },
    "DimStore": {
        "Columns": [
            "StoreKey",
            "StoreID",
            "StoreName",
            "StoreNumber",
            "Address",
            "City",
            "StateProvince",
            "Country",
            "PostalCode",
            "Region",
            "Territory",
            "StoreType",
            "StoreStatus",
            "ManagerName",
            "OpeningDate",
            "SquareFootage",
            "ValidFromDate",
            "ValidToDate",
            "IsCurrent",
            "SourceUpdateDate",
            "Version",
        ],
        "NaturalKey": "StoreID",
        "Query": """
            SELECT
                ('x' || MD5(s.BusinessEntityID::TEXT || COALESCE(s.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS StoreKey,
                s.BusinessEntityID AS StoreID,
                s.Name AS StoreName,
                s.BusinessEntityID AS StoreNumber,
                COALESCE(
                    NULLIF(TRIM(COALESCE(a.AddressLine1, '') || ' ' || COALESCE(a.AddressLine2, '')), ''),
                    'Unknown'
                ) AS Address,
                COALESCE(a.City, 'Unknown') AS City,
                COALESCE(sp.Name, 'Unknown') AS StateProvince,
                COALESCE(cr.Name, 'Unknown') AS Country,
                COALESCE(a.PostalCode, '00000') AS PostalCode,
                COALESCE(st."group", 'Unassigned') AS Region,
                COALESCE(st.Name, 'Unassigned') AS Territory,
                COALESCE(
                    NULLIF(
                        (xpath(
                            '/ns:StoreSurvey/ns:BusinessType/text()',
                            s.Demographics,
                            ARRAY[ARRAY['ns', 'http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey']]
                        ))[1]::text,
                        ''
                    ),
                    'Retail'
                ) AS StoreType,
                'Active' AS StoreStatus,
                COALESCE(mgr.FirstName || ' ' || mgr.LastName, 'Unassigned') AS ManagerName,
                COALESCE(
                    TO_DATE(
                        (xpath(
                            '/ns:StoreSurvey/ns:YearOpened/text()',
                            s.Demographics,
                            ARRAY[ARRAY['ns', 'http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey']]
                        ))[1]::text || '-01-01',
                        'YYYY-MM-DD'
                    ),
                    s.ModifiedDate::date
                ) AS OpeningDate,
                COALESCE(
                    NULLIF(
                        CAST(
                            (xpath(
                                '/ns:StoreSurvey/ns:SquareFeet/text()',
                                s.Demographics,
                                ARRAY[ARRAY['ns', 'http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey']]
                            ))[1]::text AS INT
                        ),
                        0
                    ),
                    1000
                ) AS SquareFootage,
                s.ModifiedDate AS ValidFromDate,
                DATE '9999-12-31' AS ValidToDate,
                1 AS IsCurrent,
                s.ModifiedDate AS SourceUpdateDate,
                1::BIGINT AS Version
            FROM Sales.Store AS s
            LEFT JOIN Person.BusinessEntityAddress AS bea ON s.BusinessEntityID = bea.BusinessEntityID
            LEFT JOIN Person.Address AS a ON bea.AddressID = a.AddressID
            LEFT JOIN Person.StateProvince AS sp ON a.StateProvinceID = sp.StateProvinceID
            LEFT JOIN Person.CountryRegion AS cr ON sp.CountryRegionCode = cr.CountryRegionCode
            LEFT JOIN Sales.SalesTerritory AS st ON sp.TerritoryID = st.TerritoryID
            LEFT JOIN Sales.SalesPerson AS sp_sales ON s.SalesPersonID = sp_sales.BusinessEntityID
            LEFT JOIN Person.Person AS mgr ON sp_sales.BusinessEntityID = mgr.BusinessEntityID
            ORDER BY s.BusinessEntityID;
        """,
    },
    "DimEmployee": {
        "Columns": [
            "EmployeeKey",
            "EmployeeID",
            "EmployeeName",
            "JobTitle",
            "Department",
            "ReportingManagerKey",
            "HireDate",
            "EmployeeStatus",
            "Region",
            "Territory",
            "SalesQuota",
            "ValidFromDate",
            "ValidToDate",
            "IsCurrent",
            "SourceUpdateDate",
            "Version",
        ],
        "NaturalKey": "EmployeeID",
        "Query": """
            SELECT
                ('x' || MD5(e.BusinessEntityID::TEXT || COALESCE(e.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS EmployeeKey,
                e.BusinessEntityID AS EmployeeID,
                p.FirstName || ' ' || COALESCE(p.MiddleName || ' ', '') || p.LastName AS EmployeeName,
                e.JobTitle,
                COALESCE(d.Name, 'Unassigned') AS Department,
                NULL::BIGINT AS ReportingManagerKey,
                e.HireDate,
                CASE
                    WHEN e.CurrentFlag = TRUE THEN 'Active'
                    ELSE 'Inactive'
                END AS EmployeeStatus,
                COALESCE(st."group", 'N/A') AS Region,
                COALESCE(st.Name, 'N/A') AS Territory,
                COALESCE(CAST(sp.SalesQuota AS DECIMAL(18, 2)), 0.00) AS SalesQuota,
                e.HireDate AS ValidFromDate,
                DATE '9999-12-31' AS ValidToDate,
                CASE WHEN e.CurrentFlag THEN 1 ELSE 0 END AS IsCurrent,
                e.ModifiedDate AS SourceUpdateDate,
                1::BIGINT AS Version
            FROM HumanResources.Employee AS e
            INNER JOIN Person.Person AS p ON e.BusinessEntityID = p.BusinessEntityID
            LEFT JOIN HumanResources.EmployeeDepartmentHistory AS edh
                ON e.BusinessEntityID = edh.BusinessEntityID AND edh.EndDate IS NULL
            LEFT JOIN HumanResources.Department AS d ON edh.DepartmentID = d.DepartmentID
            LEFT JOIN Sales.SalesPerson AS sp ON e.BusinessEntityID = sp.BusinessEntityID
            LEFT JOIN Sales.SalesTerritory AS st ON sp.TerritoryID = st.TerritoryID
            ORDER BY e.BusinessEntityID;
        """,
    },
    "DimPromotion": {
        "Columns": [
            "PromotionKey",
            "PromotionID",
            "PromotionName",
            "PromotionDescription",
            "PromotionType",
            "DiscountPercentage",
            "DiscountAmount",
            "PromotionStatus",
            "TargetCustomerSegment",
            "CampaignID",
            "TargetProductKey",
            "StartDate",
            "EndDate",
            "IsActive",
        ],
        "NaturalKey": "PromotionID",
        "Query": """
            SELECT
                ('x' || MD5(so.SpecialOfferID::TEXT || COALESCE(so.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS PromotionKey,
                so.SpecialOfferID AS PromotionID,
                so.Description AS PromotionName,
                so.Description AS PromotionDescription,
                so.Type AS PromotionType,
                CAST(so.DiscountPct * 100 AS DECIMAL(5, 2)) AS DiscountPercentage,
                0.00 AS DiscountAmount,
                CASE
                    WHEN CURRENT_DATE BETWEEN so.StartDate AND so.EndDate THEN 'Active'
                    WHEN CURRENT_DATE < so.StartDate THEN 'Scheduled'
                    ELSE 'Expired'
                END AS PromotionStatus,
                so.Category AS TargetCustomerSegment,
                so.SpecialOfferID AS CampaignID,
                NULL::BIGINT AS TargetProductKey,
                so.StartDate,
                so.EndDate,
                CASE WHEN CURRENT_DATE BETWEEN so.StartDate AND so.EndDate THEN 1 ELSE 0 END AS IsActive
            FROM Sales.SpecialOffer AS so
            ORDER BY so.SpecialOfferID;
        """,
    },
    "DimProductCategory": {
        "Columns": [
            "ProductCategoryKey",
            "ProductCategoryID",
            "CategoryName",
            "CategoryDescription",
        ],
        "NaturalKey": "ProductCategoryID",
        "Query": """
            SELECT
                ('x' || MD5(pc.ProductCategoryID::TEXT || pc.Name::TEXT || pc.Name::TEXT))::bit(32)::BIGINT AS ProductCategoryKey,
                pc.ProductCategoryID,
                pc.Name AS CategoryName,
                pc.Name AS CategoryDescription
            FROM Production.ProductCategory AS pc
            ORDER BY pc.ProductCategoryID;
        """,
    },
    "DimWarehouse": {
        "Columns": [
            "WarehouseKey",
            "WarehouseID",
            "WarehouseName",
            "Location",
            "WarehouseType",
            "ValidFromDate",
            "ValidToDate",
            "IsCurrent",
            "Version",
        ],
        "NaturalKey": "WarehouseID",
        "Query": """
            SELECT
                ('x' || MD5(l.LocationID::TEXT || COALESCE(l.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS WarehouseKey,
                l.LocationID AS WarehouseID,
                l.Name AS WarehouseName,
                l.Name AS Location,
                'Manufacturing' AS WarehouseType,
                l.ModifiedDate AS ValidFromDate,
                DATE '9999-12-31' AS ValidToDate,
                1 AS IsCurrent,
                1::BIGINT Version
            FROM Production.Location AS l
            ORDER BY l.LocationID;
        """,
    },
    "DimSalesTerritory": {
        "Columns": [
            "TerritoryKey",
            "TerritoryID",
            "TerritoryName",
            "SalesRegion",
            "Country",
            "Manager",
            "SalesTarget",
        ],
        "NaturalKey": "TerritoryID",
        "Query": """
            SELECT
                ('x' || MD5(st.TerritoryID::TEXT || st.Name::TEXT || st."group"::TEXT || st.CountryRegionCode::TEXT))::bit(32)::BIGINT AS TerritoryKey,
                st.TerritoryID,
                st.Name AS TerritoryName,
                st."group" AS SalesRegion,
                st.CountryRegionCode AS Country,
                'TBD' AS Manager,
                CAST(st.CostYTD AS DECIMAL(18, 2)) AS SalesTarget
            FROM Sales.SalesTerritory AS st
            ORDER BY st.TerritoryID;
        """,
    },
    "DimVendor": {
        "Columns": [
            "VendorKey",
            "VendorID",
            "VendorName",
            "ContactPerson",
            "Email",
            "Phone",
            "Address",
            "City",
            "Country",
            "VendorRating",
            "OnTimeDeliveryRate",
            "QualityScore",
            "PaymentTerms",
            "VendorStatus",
            "ValidFromDate",
            "ValidToDate",
            "IsCurrent",
            "SourceUpdateDate",
        ],
        "NaturalKey": "VendorID",
        "Query": """
            WITH vendor_performance AS (
                SELECT
                    VendorID,
                    COUNT(*) AS TotalOrders,
                    SUM(CASE WHEN ShipDate <= OrderDate + INTERVAL '7 days' THEN 1 ELSE 0 END) AS OnTimeOrders
                FROM Purchasing.PurchaseOrderHeader
                GROUP BY VendorID
            )
            SELECT
                ('x' || MD5(v.BusinessEntityID::TEXT || COALESCE(v.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS VendorKey,
                v.BusinessEntityID AS VendorID,
                v.Name AS VendorName,
                COALESCE(cp.FirstName || ' ' || cp.LastName, 'Unknown') AS ContactPerson,
                COALESCE(ea.EmailAddress, '') AS Email,
                COALESCE(ph.PhoneNumber, '') AS Phone,
                COALESCE(a.AddressLine1, '') ||
                CASE
                    WHEN a.AddressLine2 IS NOT NULL THEN ', ' || a.AddressLine2
                    ELSE ''
                END AS Address,
                COALESCE(a.City, 'Unknown') AS City,
                COALESCE(cr.Name, 'Unknown') AS Country,
                CAST(v.CreditRating AS DECIMAL(3, 2)) / 5.0 AS VendorRating,
                COALESCE(ROUND((vp.OnTimeOrders::DECIMAL / NULLIF(vp.TotalOrders, 0)) * 100, 2), 0.00) AS OnTimeDeliveryRate,
                85.00 AS QualityScore,
                'Net 30' AS PaymentTerms,
                CASE WHEN v.ActiveFlag THEN 'Active' ELSE 'Inactive' END AS VendorStatus,
                v.ModifiedDate AS ValidFromDate,
                DATE '9999-12-31' AS ValidToDate,
                CASE WHEN v.ActiveFlag THEN 1 ELSE 0 END AS IsCurrent,
                v.ModifiedDate AS SourceUpdateDate
            FROM Purchasing.Vendor AS v
            LEFT JOIN vendor_performance AS vp ON v.BusinessEntityID = vp.VendorID
            LEFT JOIN Person.BusinessEntityContact AS bec ON v.BusinessEntityID = bec.BusinessEntityID
            LEFT JOIN Person.Person AS cp ON bec.PersonID = cp.BusinessEntityID
            LEFT JOIN Person.EmailAddress AS ea ON bec.PersonID = ea.BusinessEntityID
            LEFT JOIN Person.PersonPhone AS ph ON bec.PersonID = ph.BusinessEntityID
            LEFT JOIN Person.BusinessEntityAddress AS bea ON v.BusinessEntityID = bea.BusinessEntityID
            LEFT JOIN Person.Address AS a ON bea.AddressID = a.AddressID
            LEFT JOIN Person.StateProvince AS sp ON a.StateProvinceID = sp.StateProvinceID
            LEFT JOIN Person.CountryRegion AS cr ON sp.CountryRegionCode = cr.CountryRegionCode
            ORDER BY v.BusinessEntityID;
        """,
    },
    "DimRegion": {
        "Columns": [
            "RegionKey",
            "RegionID",
            "RegionName",
            "Country",
            "Continent",
            "TimeZone",
        ],
        "NaturalKey": "RegionID",
        "Query": """
            SELECT
                ('x' || MD5(COALESCE(ROW_NUMBER() OVER (ORDER BY st."group", cr.Name), 0::BIGINT) || COALESCE(st."group", '') || '|' || COALESCE(cr.Name, '')))::bit(32)::BIGINT AS RegionKey,

                ROW_NUMBER() OVER (ORDER BY st."group", cr.Name) AS RegionID,
                st."group" AS RegionName,
                st."name" AS Country,
                st."group" AS Continent,
                'UTC' AS TimeZone
            FROM Sales.SalesTerritory AS st
            INNER JOIN Person.CountryRegion AS cr ON st.CountryRegionCode = cr.CountryRegionCode
            ORDER BY st."group", cr.Name;
        """,
    },
}


STATIC_LOOKUPS = {
    "DimFeedbackCategory": {
        "Columns": [
            "FeedbackCategoryKey",
            "FeedbackCategoryID",
            "CategoryName",
            "CategoryDescription",
        ],
        "Data": [
            (1, 1, "Product Quality", "Issues related to product quality"),
            (2, 2, "Service", "Customer service related feedback"),
            (3, 3, "Delivery", "Shipping and delivery feedback"),
            (4, 4, "Pricing", "Price-related feedback"),
            (5, 5, "General", "General comments"),
        ],
    },
    "DimCustomerSegment": {
        "Columns": [
            "SegmentKey",
            "SegmentID",
            "SegmentName",
            "SegmentDescription",
            "DiscountTierStart",
            "DiscountTierEnd",
        ],
        "Data": [
            (1, 1, "Premium", "Customers with income > $150K", 20.00, 100.00),
            (2, 2, "Gold", "Customers with income $100K-$150K", 15.00, 19.99),
            (3, 3, "Silver", "Customers with income $50K-$100K", 10.00, 14.99),
            (4, 4, "Bronze", "Customers with income $25K-$50K", 5.00, 9.99),
            (5, 5, "Standard", "Customers with income < $25K", 0.00, 4.99),
            (6, 6, "Business", "B2B customers", 15.00, 100.00),
        ],
    },
    "DimAgingTier": {
        "Columns": [
            "AgingTierKey",
            "AgingTierID",
            "AgingTierName",
            "MinAgingDays",
            "MaxAgingDays",
        ],
        "Data": [
            (1, 1, "Fresh", 0, 30),
            (2, 2, "Aging", 31, 90),
            (3, 3, "Old", 91, 180),
            (4, 4, "Obsolete", 181, 999999),
        ],
    },
    "DimFinanceCategory": {
        "Columns": [
            "FinanceCategoryKey",
            "FinanceCategoryID",
            "CategoryName",
            "CategoryDescription",
        ],
        "Data": [
            (1, 1, "Small Transaction", "Transaction < $100"),
            (2, 2, "Medium Transaction", "Transaction $100-$1000"),
            (3, 3, "Large Transaction", "Transaction $1000-$10000"),
            (4, 4, "Enterprise Transaction", "Transaction > $10000"),
            (5, 5, "Credit Transaction", "Credit-based purchases"),
        ],
    },
    "DimReturnReason": {
        "Columns": [
            "ReturnReasonKey",
            "ReturnReasonID",
            "ReturnReasonName",
            "ReturnReasonDescription",
        ],
        "Data": [
            (1, 1, "Defective", "Product or it's part/s are defective"),
            (2, 2, "Wrong Item", "Wrong Product Received"),
            (3, 3, "Changed Mind", "Don't want this product after all"),
            (4, 4, "Damaged", "Received a damaged product"),
        ],
    },
}

MAX_ROWS_IN_MEMORY = 100000  # Safety limit


def _has_scd_columns(columns) -> bool:
    required = {"IsCurrent", "ValidFromDate", "ValidToDate", "Version"}
    return required.issubset(set(columns))


def _coerce_to_date(value):
    """Safely convert various date types to date object"""
    if value is None:
        return date.today()
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value[:10]).date()
        except Exception:
            return date.today()
    return date.today()


def _sql_quote(value) -> str:
    """Safely quote string values for SQL"""
    if isinstance(value, str):
        v = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{v}'"
    return str(value)


def _date_sql(d: date) -> str:
    """Convert date to ClickHouse date literal"""
    return f"toDate('{d.isoformat()}')"

@dag(
    dag_id="adventureworks_dimension_sync",
    dag_display_name="Sync Dimension Tables",
    schedule="@hourly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["adventureworks", "dimension", "sync"],
)
def populate():
    @task
    def extract_postgres_dimension(table, query, columns):
        conn = pg()
        cur = conn.cursor()
        try:
            cur.execute(query)
            rows = cur.fetchall()
            cleaned_rows = clean_values_in_rows(rows, columns)
            file_path = f"/tmp/airflow/pg_dimension_{table.lower()}_data.pkl"
            with open(file_path, "wb") as file:
                pickle.dump(cleaned_rows, file)
            return file_path
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    @task
    def extract_clickhouse_dimension(table, columns, natural_key):
        client = ch()
        try:
            col_str = ", ".join(columns)
            result = client.query(f"SELECT {col_str} FROM ADVENTUREWORKS_DWS.{table}")
            file_path = f"/tmp/airflow/ch_dimension_{table.lower()}_data.pkl"
            with open(file_path, "wb") as file:
                pickle.dump(result.result_rows, file)
            return file_path
        finally:
            try:
                client.close()
            except Exception:
                pass

    @task
    def sync_dimension_table(table, columns, natural_key, pg_file, ch_file):
        """
        Synchronize dimension table between PostgreSQL source and ClickHouse warehouse.
        Handles both SCD Type 2 and regular dimension tables.
        """
        client = None
        processing_batch_id = f"{table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        error_rows = []

        print(f"[{table}] Starting sync for batch {processing_batch_id}")

        try:
            # Load data from pickle files
            print(f"[{table}] Loading PostgreSQL data...")
            with open(pg_file, "rb") as pg_file_data:
                pg_rows = pickle.load(pg_file_data)
            print(f"[{table}] Loaded {len(pg_rows)} rows from PostgreSQL")

            # Check row count safety
            if len(pg_rows) > MAX_ROWS_IN_MEMORY:
                raise Exception(f"PostgreSQL data too large: {len(pg_rows)} rows (max: {MAX_ROWS_IN_MEMORY})")

            print(f"[{table}] Loading ClickHouse data...")
            with open(ch_file, "rb") as ch_file_data:
                ch_rows = pickle.load(ch_file_data)
            print(f"[{table}] Loaded {len(ch_rows)} rows from ClickHouse")

            if len(ch_rows) > MAX_ROWS_IN_MEMORY:
                raise Exception(f"ClickHouse data too large: {len(ch_rows)} rows (max: {MAX_ROWS_IN_MEMORY})")

            client = ch()

            nk_idx = columns.index(natural_key)
            is_scd = _has_scd_columns(columns)

            print(f"[{table}] Is SCD: {is_scd}, Natural Key: {natural_key} (index: {nk_idx})")

            # Get SCD column indices if applicable
            if is_scd:
                idx_validfrom = columns.index("ValidFromDate")
                idx_validto = columns.index("ValidToDate")
                idx_iscurrent = columns.index("IsCurrent")
                idx_version = columns.index("Version")
                print(
                    f"[{table}] SCD indices - ValidFrom: {idx_validfrom}, ValidTo: {idx_validto}, IsCurrent: {idx_iscurrent}, Version: {idx_version}")

            # Build lookup maps with progress tracking
            print(f"[{table}] Building ClickHouse lookup map...")
            ch_map = {}
            for idx, row in enumerate(ch_rows):
                if idx % 10000 == 0 and idx > 0:
                    print(f"[{table}] Processed {idx}/{len(ch_rows)} ClickHouse rows...")

                row_list = list(row)
                # Coerce date columns in ClickHouse data
                if is_scd:
                    row_list[idx_validfrom] = _coerce_to_date(row_list[idx_validfrom])
                    row_list[idx_validto] = _coerce_to_date(row_list[idx_validto])
                ch_map[row_list[nk_idx]] = row_list

            print(f"[{table}] Building PostgreSQL lookup map...")
            pg_map = {}
            for idx, row in enumerate(pg_rows):
                if idx % 10000 == 0 and idx > 0:
                    print(f"[{table}] Processed {idx}/{len(pg_rows)} PostgreSQL rows...")

                row_list = list(row)
                # Coerce date columns in PostgreSQL data
                if is_scd:
                    row_list[idx_validfrom] = _coerce_to_date(row_list[idx_validfrom])
                    row_list[idx_validto] = _coerce_to_date(row_list[idx_validto])
                pg_map[row_list[nk_idx]] = row_list

            # Free up memory
            del pg_rows
            del ch_rows
            gc.collect()

            ch_keys = set(ch_map.keys())
            pg_keys = set(pg_map.keys())

            print(f"[{table}] Keys - PG: {len(pg_keys)}, CH: {len(ch_keys)}")

            # Track changes
            to_insert = []
            to_delete = []
            to_update = []
            scd_expire_updates = []
            scd_expire_deletes = []

            # New records in source (not in warehouse)
            print(f"[{table}] Finding new records...")
            for key in pg_keys - ch_keys:
                row = list(pg_map[key])
                if is_scd:
                    row[idx_version] = 1
                    row[idx_iscurrent] = 1
                    row[idx_validto] = FAR_FUTURE_DATE
                    row[idx_validfrom] = _coerce_to_date(row[idx_validfrom])
                to_insert.append(row)

            # Deleted records in source (exist in warehouse but not in source)
            print(f"[{table}] Finding deleted records...")
            for key in ch_keys - pg_keys:
                if is_scd:
                    scd_expire_deletes.append(key)
                else:
                    to_delete.append(key)

            # Changed records (exist in both) - THIS IS THE DANGEROUS PART
            print(f"[{table}] Comparing changed records...")
            comparison_count = 0
            for key in pg_keys & ch_keys:
                comparison_count += 1
                if comparison_count % 5000 == 0:
                    print(f"[{table}] Compared {comparison_count}/{len(pg_keys & ch_keys)} records...")

                pg_row = list(pg_map[key])
                ch_row = list(ch_map[key])

                if is_scd:
                    # For SCD, exclude surrogate key, ValidToDate, IsCurrent, and Version from comparison
                    exclude = {0, idx_validto, idx_iscurrent, idx_version}

                    # Simple direct comparison without helper function
                    rows_differ = False
                    for i in range(len(columns)):
                        if i in exclude:
                            continue

                        pg_val = pg_row[i]
                        ch_val = ch_row[i]

                        # Handle dates
                        if i == idx_validfrom:
                            pg_val = _coerce_to_date(pg_val)
                            ch_val = _coerce_to_date(ch_val)

                        if pg_val != ch_val:
                            rows_differ = True
                            break

                    if rows_differ:
                        pg_vf = _coerce_to_date(pg_row[idx_validfrom])
                        ch_vf = _coerce_to_date(ch_row[idx_validfrom])

                        # Only create new version if ValidFromDate has advanced
                        if pg_vf > ch_vf:
                            old_ver = int(ch_row[idx_version] or 0)
                            pg_row[idx_version] = old_ver + 1
                            pg_row[idx_iscurrent] = 1
                            pg_row[idx_validto] = FAR_FUTURE_DATE
                            pg_row[idx_validfrom] = pg_vf
                            to_insert.append(pg_row)
                            scd_expire_updates.append((key, pg_vf))
                else:
                    # For non-SCD, simple comparison excluding surrogate key
                    if pg_row[1:] != ch_row[1:]:
                        to_update.append((key, pg_row))

            # Free more memory
            del pg_map
            del ch_map
            gc.collect()

            print(
                f"[{table}] Summary: insert={len(to_insert)}, "
                f"update={len(to_update) if not is_scd else len(scd_expire_updates)}, "
                f"delete={len(to_delete) if not is_scd else len(scd_expire_deletes)}"
            )

            # Execute deletes for non-SCD tables
            if to_delete:
                print(f"[{table}] Executing {len(to_delete)} deletes...")
                try:
                    if isinstance(to_delete[0], str):
                        key_list = ",".join(_sql_quote(k) for k in to_delete)
                    else:
                        key_list = ",".join(str(k) for k in to_delete)
                    client.command(
                        f"ALTER TABLE ADVENTUREWORKS_DWS.{table} DELETE WHERE {natural_key} IN ({key_list})"
                    )
                    client.command(f"OPTIMIZE TABLE ADVENTUREWORKS_DWS.{table} FINAL")
                except Exception as e:
                    print(f"[{table}] Delete failed: {e}")
                    err = build_error_record(
                        source_table=table,
                        record_natural_key="delete_batch",
                        error_type="DeleteFailed",
                        error_message=str(e),
                        failed_data=json.dumps({"keys": to_delete[:5]}, default=str),
                        task_name=f"sync_dimension_{table.lower()}",
                        batch_id=processing_batch_id,
                        severity="Error",
                        is_recoverable=1,
                    )
                    error_rows.append(err)

            # Expire deleted records for SCD tables
            if is_scd and scd_expire_deletes:
                print(f"[{table}] Expiring {len(scd_expire_deletes)} deleted records...")
                try:
                    today = date.today()
                    if isinstance(scd_expire_deletes[0], str):
                        key_list = ",".join(_sql_quote(k) for k in scd_expire_deletes)
                    else:
                        key_list = ",".join(str(k) for k in scd_expire_deletes)
                    client.command(
                        f"ALTER TABLE ADVENTUREWORKS_DWS.{table} "
                        f"UPDATE IsCurrent = 0, ValidToDate = {_date_sql(today)} "
                        f"WHERE {natural_key} IN ({key_list}) AND IsCurrent = 1"
                    )
                    client.command(f"OPTIMIZE TABLE ADVENTUREWORKS_DWS.{table} FINAL")
                except Exception as e:
                    print(f"[{table}] Expire deletes failed: {e}")
                    err = build_error_record(
                        source_table=table,
                        record_natural_key="expire_delete_batch",
                        error_type="ExpireDeleteFailed",
                        error_message=str(e),
                        failed_data=json.dumps({"keys": scd_expire_deletes[:5]}, default=str),
                        task_name=f"sync_dimension_{table.lower()}",
                        batch_id=processing_batch_id,
                        severity="Error",
                        is_recoverable=1,
                    )
                    error_rows.append(err)

            # Expire old versions for SCD updates (BATCHED)
            if is_scd and scd_expire_updates:
                print(f"[{table}] Expiring {len(scd_expire_updates)} updated records...")
                try:
                    # Batch all expire operations into a single UPDATE command
                    keys_to_expire = [key for key, _ in scd_expire_updates]

                    if isinstance(keys_to_expire[0], str):
                        key_list = ",".join(_sql_quote(k) for k in keys_to_expire)
                    else:
                        key_list = ",".join(str(k) for k in keys_to_expire)

                    # Use the minimum new ValidFromDate as the expire date
                    expire_date = min(new_vf for _, new_vf in scd_expire_updates)

                    client.command(
                        f"ALTER TABLE ADVENTUREWORKS_DWS.{table} "
                        f"UPDATE IsCurrent = 0, ValidToDate = {_date_sql(expire_date)} "
                        f"WHERE {natural_key} IN ({key_list}) AND IsCurrent = 1"
                    )
                    client.command(f"OPTIMIZE TABLE ADVENTUREWORKS_DWS.{table} FINAL")
                except Exception as e:
                    print(f"[{table}] Expire updates failed: {e}")
                    err = build_error_record(
                        source_table=table,
                        record_natural_key="expire_update_batch",
                        error_type="ExpireUpdateFailed",
                        error_message=str(e),
                        failed_data=json.dumps(
                            {"sample": scd_expire_updates[0] if scd_expire_updates else None},
                            default=str,
                        ),
                        task_name=f"sync_dimension_{table.lower()}",
                        batch_id=processing_batch_id,
                        severity="Error",
                        is_recoverable=1,
                    )
                    error_rows.append(err)

            # Handle updates for non-SCD tables (DELETE + INSERT pattern)
            if not is_scd and to_update:
                print(f"[{table}] Updating {len(to_update)} records...")
                try:
                    # First delete the old records
                    update_keys = [key for key, _ in to_update]
                    if isinstance(update_keys[0], str):
                        key_list = ",".join(_sql_quote(k) for k in update_keys)
                    else:
                        key_list = ",".join(str(k) for k in update_keys)

                    client.command(
                        f"ALTER TABLE ADVENTUREWORKS_DWS.{table} DELETE WHERE {natural_key} IN ({key_list})"
                    )

                    # Then insert the updated records
                    updated_rows = [row for _, row in to_update]
                    batch_size = 200
                    for i in range(0, len(updated_rows), batch_size):
                        batch = updated_rows[i: i + batch_size]
                        check_insert(batch, columns)
                        client.insert(
                            f"ADVENTUREWORKS_DWS.{table}",
                            batch,
                            column_names=columns,
                        )

                    client.command(f"OPTIMIZE TABLE ADVENTUREWORKS_DWS.{table} FINAL")
                except Exception as e:
                    print(f"[{table}] Update failed: {e}")
                    err = build_error_record(
                        source_table=table,
                        record_natural_key="update_batch",
                        error_type="UpdateFailed",
                        error_message=str(e),
                        failed_data=json.dumps(
                            {"sample": to_update[0] if to_update else None},
                            default=str,
                        ),
                        task_name=f"sync_dimension_{table.lower()}",
                        batch_id=processing_batch_id,
                        severity="Error",
                        is_recoverable=1,
                    )
                    error_rows.append(err)

            # Insert new records (both new keys and new SCD versions)
            if to_insert:
                print(f"[{table}] Inserting {len(to_insert)} records...")
                try:
                    batch_size = 200
                    for i in range(0, len(to_insert), batch_size):
                        if i % 2000 == 0 and i > 0:
                            print(f"[{table}] Inserted {i}/{len(to_insert)} records...")
                        batch = to_insert[i: i + batch_size]
                        check_insert(batch, columns)
                        client.insert(
                            f"ADVENTUREWORKS_DWS.{table}",
                            batch,
                            column_names=columns,
                        )
                except Exception as e:
                    print(f"[{table}] Insert failed: {e}")
                    traceback.print_exc()
                    err = build_error_record(
                        source_table=table,
                        record_natural_key="insert_batch",
                        error_type="InsertFailed",
                        error_message=str(e),
                        failed_data=json.dumps(
                            {"sample": to_insert[0] if to_insert else None},
                            default=str,
                        ),
                        task_name=f"sync_dimension_{table.lower()}",
                        batch_id=processing_batch_id,
                        severity="Error",
                        is_recoverable=1,
                    )
                    error_rows.append(err)

            # Log any errors to error tracking table
            if error_rows:
                insert_error_records(client, error_rows)

            print(f"[INFO] {table}: Batch {processing_batch_id} complete")
            return {
                "inserted": len(to_insert),
                "updated": len(to_update) if not is_scd else len(scd_expire_updates),
                "deleted": len(to_delete) if not is_scd else len(scd_expire_deletes),
                "errors": len(error_rows),
            }

        except Exception as e:
            print(f"[ERROR] {table}: Fatal error - {e}")
            traceback.print_exc()
            raise

        finally:
            # Cleanup
            if client:
                try:
                    client.close()
                except Exception:
                    pass
            gc.collect()

    @task
    def insert_static_dimension_columns(data, table, columns):
        client = ch()
        processing_batch_id = f"{table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            client.command(f"TRUNCATE TABLE ADVENTUREWORKS_DWS.{table}")
            client.insert(
                f"ADVENTUREWORKS_DWS.{table}",
                data,
                column_names=columns,
            )
            return len(data)
        except Exception as e:
            err = build_error_record(
                source_table=table,
                record_natural_key="static_load",
                error_type="InsertFailed",
                error_message=str(e),
                failed_data=json.dumps({"rows": len(data)}, default=str),
                task_name=f"insert_static_{table.lower()}",
                batch_id=processing_batch_id,
                severity="Error",
                is_recoverable=0,
            )
            insert_error_records(client, [err])
            raise
        finally:
            try:
                client.close()
            except Exception:
                pass

    for table, content in DIMENSIONS.items():
        columns = content["Columns"]
        query = content["Query"]
        natural_key = content["NaturalKey"]
        pg_data = extract_postgres_dimension.override(
            task_display_name=f"Extract {table} from Postgres"
        )(table, query, columns)
        ch_data = extract_clickhouse_dimension.override(
            task_display_name=f"Extract {table} from ClickHouse"
        )(table, columns, natural_key)
        sync_dimension_table.override(
            task_display_name=f"Sync {table}"
        )(table, columns, natural_key, pg_data, ch_data)

    for table, items in STATIC_LOOKUPS.items():
        col = items["Columns"]
        dat = items["Data"]
        insert_static_dimension_columns.override(
            task_display_name=f"Load Static {table}"
        )(dat, table, col)


populate()