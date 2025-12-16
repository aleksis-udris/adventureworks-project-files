from datetime import date, datetime
import json
import pickle

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
    FROM Sales.Customer c
    LEFT JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
    LEFT JOIN Sales.Store s ON c.StoreID = s.BusinessEntityID
    LEFT JOIN Person.EmailAddress ea ON ea.BusinessEntityID = c.PersonID
    LEFT JOIN Person.PersonPhone pp ON pp.BusinessEntityID = c.PersonID
    LEFT JOIN Person.BusinessEntityAddress bea ON bea.BusinessEntityID = COALESCE(c.PersonID, c.StoreID)
    LEFT JOIN Person.Address addr ON addr.AddressID = bea.AddressID
    LEFT JOIN Person.StateProvince sp ON sp.StateProvinceID = addr.StateProvinceID
    LEFT JOIN Person.CountryRegion cr ON cr.CountryRegionCode = sp.CountryRegionCode
    LEFT JOIN Sales.vPersonDemographics pd ON c.PersonID = pd.BusinessEntityID
    LEFT JOIN customer_sales cs ON c.CustomerID = cs.CustomerID
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
            FROM Production.Product p
            LEFT JOIN Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
            LEFT JOIN Production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
            LEFT JOIN Production.ProductModel pm ON p.ProductModelID = pm.ProductModelID
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
            FROM Sales.Store s
            LEFT JOIN Person.BusinessEntityAddress bea ON s.BusinessEntityID = bea.BusinessEntityID
            LEFT JOIN Person.Address a ON bea.AddressID = a.AddressID
            LEFT JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
            LEFT JOIN Person.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
            LEFT JOIN Sales.SalesTerritory st ON sp.TerritoryID = st.TerritoryID
            LEFT JOIN Sales.SalesPerson sp_sales ON s.SalesPersonID = sp_sales.BusinessEntityID
            LEFT JOIN Person.Person mgr ON sp_sales.BusinessEntityID = mgr.BusinessEntityID
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
            FROM HumanResources.Employee e
            INNER JOIN Person.Person p ON e.BusinessEntityID = p.BusinessEntityID
            LEFT JOIN HumanResources.EmployeeDepartmentHistory edh
                ON e.BusinessEntityID = edh.BusinessEntityID AND edh.EndDate IS NULL
            LEFT JOIN HumanResources.Department d ON edh.DepartmentID = d.DepartmentID
            LEFT JOIN Sales.SalesPerson sp ON e.BusinessEntityID = sp.BusinessEntityID
            LEFT JOIN Sales.SalesTerritory st ON sp.TerritoryID = st.TerritoryID
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
            FROM Sales.SpecialOffer so
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
            FROM Production.ProductCategory pc
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
                1 AS IsCurrent
            FROM Production.Location AS l
            JOIN Production.WorkOrderRouting AS wor ON l.LocationID = wor.LocationID
            JOIN HumanResources.EmployeeDepartmentHistory AS edh ON wor.OperationSequence % 10 = edh.ShiftID
            JOIN HumanResources.Employee AS e ON edh.BusinessEntityID = e.BusinessEntityID
            JOIN HumanResources.Department AS d ON edh.DepartmentID = d.DepartmentID
            ORDER BY l.LocationID, d.DepartmentID;
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
            FROM Sales.SalesTerritory st
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
            FROM Purchasing.Vendor v
            LEFT JOIN vendor_performance vp ON v.BusinessEntityID = vp.VendorID
            LEFT JOIN Person.BusinessEntityContact bec ON v.BusinessEntityID = bec.BusinessEntityID
            LEFT JOIN Person.Person cp ON bec.PersonID = cp.BusinessEntityID
            LEFT JOIN Person.EmailAddress ea ON bec.PersonID = ea.BusinessEntityID
            LEFT JOIN Person.PersonPhone ph ON bec.PersonID = ph.BusinessEntityID
            LEFT JOIN Person.BusinessEntityAddress bea ON v.BusinessEntityID = bea.BusinessEntityID
            LEFT JOIN Person.Address a ON bea.AddressID = a.AddressID
            LEFT JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
            LEFT JOIN Person.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
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
            SELECT DISTINCT
                ('x' || MD5(COALESCE(st."group", '') || '|' || COALESCE(cr.Name, '')))::bit(32)::BIGINT AS RegionKey,
                ROW_NUMBER() OVER (ORDER BY st."group", cr.Name) AS RegionID,
                st."group" AS RegionName,
                cr.Name AS Country,
                CASE
                    WHEN cr.Name IN ('United States', 'Canada') THEN 'North America'
                    WHEN cr.Name IN ('United Kingdom', 'Germany', 'France') THEN 'Europe'
                    WHEN cr.Name IN ('Australia') THEN 'Asia Pacific'
                    ELSE 'Other'
                END AS Continent,
                'UTC' AS TimeZone
            FROM Sales.SalesTerritory st
            INNER JOIN Person.CountryRegion cr ON st.CountryRegionCode = cr.CountryRegionCode
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


def _has_scd_columns(columns) -> bool:
    required = {"IsCurrent", "ValidFromDate", "ValidToDate", "Version"}
    return required.issubset(set(columns))


def _coerce_to_date(value):
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
    if isinstance(value, str):
        v = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{v}'"
    return str(value)


def _date_sql(d: date) -> str:
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
        client = ch()
        processing_batch_id = f"{table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        error_rows = []
        with open(pg_file, "rb") as pg_file_data:
            pg_rows = pickle.load(pg_file_data)
        with open(ch_file, "rb") as ch_file_data:
            ch_rows = pickle.load(ch_file_data)
        try:
            nk_idx = columns.index(natural_key)
            is_scd = _has_scd_columns(columns)
            if is_scd:
                idx_validfrom = columns.index("ValidFromDate")
                idx_validto = columns.index("ValidToDate")
                idx_iscurrent = columns.index("IsCurrent")
                idx_version = columns.index("Version")
            ch_map = {row[nk_idx]: row for row in ch_rows}
            pg_map = {row[nk_idx]: row for row in pg_rows}
            ch_keys = set(ch_map.keys())
            pg_keys = set(pg_map.keys())
            to_insert = []
            to_delete = []
            to_update = []
            scd_expire_updates = []
            scd_expire_deletes = []
            for key in pg_keys - ch_keys:
                row = list(pg_map[key])
                if is_scd:
                    row[idx_version] = 1
                    row[idx_iscurrent] = 1
                    row[idx_validto] = FAR_FUTURE_DATE
                    row[idx_validfrom] = _coerce_to_date(row[idx_validfrom])
                to_insert.append(row)
            for key in ch_keys - pg_keys:
                if is_scd:
                    scd_expire_deletes.append(key)
                else:
                    to_delete.append(key)
            for key in pg_keys & ch_keys:
                pg_row = list(pg_map[key])
                ch_row = list(ch_map[key])
                if is_scd:
                    exclude = {0, idx_validto, idx_iscurrent, idx_version}
                    compare_idx = [i for i in range(len(columns)) if i not in exclude]
                    if [pg_row[i] for i in compare_idx] != [ch_row[i] for i in compare_idx]:
                        pg_vf = _coerce_to_date(pg_row[idx_validfrom])
                        ch_vf = _coerce_to_date(ch_row[idx_validfrom])
                        if pg_vf > ch_vf:
                            old_ver = int(ch_row[idx_version] or 0)
                            pg_row[idx_version] = old_ver + 1
                            pg_row[idx_iscurrent] = 1
                            pg_row[idx_validto] = FAR_FUTURE_DATE
                            pg_row[idx_validfrom] = pg_vf
                            to_insert.append(pg_row)
                            scd_expire_updates.append((key, pg_vf))
                else:
                    if pg_row[1:] != ch_row[1:]:
                        to_update.append(pg_row)
            print(
                f"[{table}] Summary: insert={len(to_insert)}, "
                f"update={len(to_update) if not is_scd else len(scd_expire_updates)}, "
                f"delete={len(to_delete) if not is_scd else len(scd_expire_deletes)}"
            )
            if to_delete:
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
            if is_scd and scd_expire_deletes:
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
            if is_scd and scd_expire_updates:
                try:
                    for key, new_vf in scd_expire_updates:
                        key_sql = _sql_quote(key) if isinstance(key, str) else str(key)
                        client.command(
                            f"ALTER TABLE ADVENTUREWORKS_DWS.{table} "
                            f"UPDATE IsCurrent = 0, ValidToDate = {_date_sql(new_vf)} "
                            f"WHERE {natural_key} = {key_sql} AND IsCurrent = 1"
                        )
                    client.command(f"OPTIMIZE TABLE ADVENTUREWORKS_DWS.{table} FINAL")
                except Exception as e:
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
            if not is_scd and to_update:
                try:
                    update_keys = [row[nk_idx] for row in to_update]
                    if isinstance(update_keys[0], str):
                        key_list = ",".join(_sql_quote(k) for k in update_keys)
                    else:
                        key_list = ",".join(str(k) for k in update_keys)
                    batch_size = 200
                    for i in range(0, len(to_update), batch_size):
                        batch = to_update[i : i + batch_size]
                        check_insert(batch, columns)
                        client.insert(
                            f"ADVENTUREWORKS_DWS.{table}",
                            batch,
                            column_names=columns,
                        )
                    client.command(f"OPTIMIZE TABLE ADVENTUREWORKS_DWS.{table} FINAL")
                except Exception as e:
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
            if to_insert:
                try:
                    batch_size = 200
                    for i in range(0, len(to_insert), batch_size):
                        batch = to_insert[i : i + batch_size]
                        check_insert(batch, columns)
                        client.insert(
                            f"ADVENTUREWORKS_DWS.{table}",
                            batch,
                            column_names=columns,
                        )
                except Exception as e:
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
            if error_rows:
                insert_error_records(client, error_rows)
            print(f"[INFO] {table}: Batch {processing_batch_id} complete")
            return {
                "inserted": len(to_insert),
                "updated": len(to_update) if not is_scd else len(scd_expire_updates),
                "deleted": len(to_delete) if not is_scd else len(scd_expire_deletes),
                "errors": len(error_rows),
            }
        finally:
            try:
                client.close()
            except Exception:
                pass

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