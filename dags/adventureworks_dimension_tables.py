from extra_functions import pg, ch, clean_values_in_rows, check_insert, partition_rows_fixed_batch
from airflow.sdk import dag, task
from datetime import datetime
import pickle

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
            "Version"
        ],
        "Query":"""
           WITH customer_sales AS (SELECT CustomerID,
                                          COUNT(*)                   AS OrderCount,
                                          COALESCE(SUM(TotalDue), 0) AS TotalLifetimeValue,
                                          COALESCE(MAX(TotalDue), 0) AS MaxOrderValue,
                                          MIN(OrderDate)             AS FirstOrderDate
                                   FROM Sales.SalesOrderHeader
                                   GROUP BY CustomerID),
                customer_base AS (SELECT
                                      -- Surrogate Key: Hash of CustomerID + ValidFrom for uniqueness
                                      ('x' || MD5(c.CustomerID::TEXT || COALESCE(c.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS CustomerKey,

                    -- Natural Key c.CustomerID,
                    c.CustomerID AS CustomerID,

                                      -- Customer attributes
                                      CASE
                                          WHEN c.PersonID IS NOT NULL THEN
                                              COALESCE(p.FirstName || ' ' || COALESCE(p.MiddleName || ' ', '') ||
                                                       p.LastName, 'Unknown')
                                          WHEN c.StoreID IS NOT NULL THEN
                                              COALESCE(s.Name, 'Unknown Store')
                                          ELSE 'Unknown Customer'
                                          END                            AS CustomerName,

                                      COALESCE(ea.EmailAddress, '')      AS Email,
                                      COALESCE(pp.PhoneNumber, '')       AS Phone,

                                      -- Location
                                      COALESCE(addr.City, 'Unknown')     AS City,
                                      COALESCE(sp.Name, 'Unknown')       AS StateProvince,
                                      COALESCE(cr.Name, 'Unknown')       AS Country,
                                      COALESCE(addr.PostalCode, '00000') AS PostalCode,

                                      -- Segment info (derived from demographics)
                                      CASE
                                          WHEN pd.YearlyIncome LIKE '%Greater%150000%' THEN 'Premium'
                                          WHEN pd.YearlyIncome LIKE '%75000%100000%' THEN 'Gold'
                                          WHEN pd.YearlyIncome LIKE '%50000%75000%' THEN 'Silver'
                                          WHEN pd.YearlyIncome LIKE '%25000%50000%' THEN 'Bronze'
                                          WHEN c.StoreID IS NOT NULL THEN 'Business'
                                          ELSE 'Standard'
                                          END                            AS CustomerSegment,

                                      CASE
                                          WHEN c.PersonID IS NOT NULL AND c.StoreID IS NULL THEN 'Individual'
                                          WHEN c.StoreID IS NOT NULL THEN 'Business'
                                          ELSE 'Unknown'
                                          END                            AS CustomerType,

                                      CASE
                                          WHEN cs.OrderCount > 0 AND cs.FirstOrderDate > CURRENT_DATE - INTERVAL '1 year' THEN 'Active'
               WHEN cs.OrderCount > 0 THEN 'Inactive'
               ELSE 'New'
           END
           AS AccountStatus,

                    -- Credit Limit: Hybrid calculation
                    ROUND(
                        CASE 
                            WHEN c.StoreID IS NOT NULL THEN 10000.00
                            ELSE 1000.00
                        END
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

                    -- Annual Income: Parse from demographics
                    CASE 
                        WHEN pd.YearlyIncome LIKE '%Greater%150000%' THEN 175000.00
                        WHEN pd.YearlyIncome LIKE '%100000%150000%' THEN 125000.00
                        WHEN pd.YearlyIncome LIKE '%75000%100000%' THEN 87500.00
                        WHEN pd.YearlyIncome LIKE '%50000%75000%' THEN 62500.00
                        WHEN pd.YearlyIncome LIKE '%25000%50000%' THEN 37500.00
                        WHEN pd.YearlyIncome LIKE '%10000%25000%' THEN 17500.00
                        ELSE 0.00
           END
           AS AnnualIncome,

                    -- Years since first purchase
                    COALESCE(
                        EXTRACT(YEAR FROM AGE(CURRENT_DATE, cs.FirstOrderDate))::INT,
                        0
                    ) AS YearsSinceFirstPurchase,

                    -- SCD2 fields
                    COALESCE(c.ModifiedDate, CURRENT_DATE) AS ValidFromDate,
                    DATE '9999-12-31' AS ValidToDate,
                    1 AS IsCurrent,
                    COALESCE(c.ModifiedDate, CURRENT_DATE) AS SourceUpdateDate,
                    COALESCE(c.ModifiedDate, CURRENT_DATE) AS EffectiveStartDate,
                    DATE '9999-12-31' AS EffectiveEndDate,

                    -- Version (start at 1)
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
           SELECT *
           FROM customer_base
           ORDER BY CustomerKey;
           """
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
            "Version"
        ],
        "Query": """
           SELECT
               -- Surrogate Key: Hash of ProductID + ModifiedDate
               ('x' || MD5(p.ProductID::TEXT || COALESCE(p.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS ProductKey,

                -- Natural Key p.ProductID,
                p.ProductID AS ProductID,
               -- Product attributes
               p.Name                                 AS ProductName,
               p.ProductNumber                        AS SKU,
               COALESCE(pc.Name, 'Uncategorized')     AS Category,
               COALESCE(psc.Name, 'None')             AS SubCategory,

               -- Brand: Derive from ProductLine or ProductModel
               COALESCE(
                       pm.Name,
                       CASE p.ProductLine
                           WHEN 'R' THEN 'Road'
                           WHEN 'M' THEN 'Mountain'
                           WHEN 'T' THEN 'Touring'
                           WHEN 'S' THEN 'Standard'
                           ELSE 'Generic'
                           END
               )                                      AS Brand,

               CAST(p.ListPrice AS DECIMAL(18, 2))    AS ListPrice,
               CAST(p.StandardCost AS DECIMAL(18, 2)) AS Cost,

               -- Product Status: Derived from dates
               CASE
                   WHEN p.DiscontinuedDate IS NOT NULL THEN 'Discontinued'
                   WHEN p.SellEndDate IS NOT NULL AND p.SellEndDate < CURRENT_DATE THEN 'Inactive'
                   WHEN p.SellStartDate > CURRENT_DATE THEN 'Pending'
                   ELSE 'Active'
                   END                                AS ProductStatus,

               COALESCE(p.Color, 'N/A')               AS Color,
               COALESCE(p.Size, 'N/A') AS Size,
                COALESCE(CAST(p.Weight AS DECIMAL(10,3)), 0.0) AS Weight,

                -- SCD2 fields
                COALESCE(p.SellStartDate, p.ModifiedDate) AS ValidFromDate,
                DATE '9999-12-31' AS ValidToDate,
                CASE 
                    WHEN p.SellEndDate IS NULL OR p.SellEndDate > CURRENT_DATE THEN 1 
                    ELSE 0
           END
           AS IsCurrent,
                p.ModifiedDate AS SourceUpdateDate,
                COALESCE(p.SellStartDate, p.ModifiedDate) AS EffectiveStartDate,
                COALESCE(p.SellEndDate, DATE '9999-12-31') AS EffectiveEndDate,

                1::BIGINT AS Version

            FROM Production.Product p
            LEFT JOIN Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
            LEFT JOIN Production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
            LEFT JOIN Production.ProductModel pm ON p.ProductModelID = pm.ProductModelID
            ORDER BY p.ProductID;
           """
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
            "Version"
        ],
        "Query": """
           {% raw %}
           SELECT
               -- Surrogate Key: Hash of StoreID + ModifiedDate
               ('x' || MD5(s.BusinessEntityID::TEXT || COALESCE(s.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS StoreKey, 

                -- Natural Key 
                s.BusinessEntityID AS StoreID, 

               s.Name                   AS StoreName,
               s.BusinessEntityID AS StoreNumber,

               -- Address
               (a.AddressLine1 || COALESCE(a.AddressLine2, '')) || 'Unknown' AS Address,
               COALESCE(a.City, 'Unknown')                      AS City,
               COALESCE(sp.Name, 'Unknown')                   AS StateProvince,
               COALESCE(cr.Name, 'Unknown')                       AS Country,
               COALESCE(a.PostalCode, '00000')               AS PostalCode,

               -- Territory info
               COALESCE(st."group", 'Unassigned')           AS Region,
               COALESCE(st.Name, 'Unassigned')             AS Territory,
               
               -- Store Type: Parse from XML demographics
               COALESCE(
                       (xpath('/ns:StoreSurvey/ns:BusinessType/text()',
                              s.Demographics,
                              '{{ns,http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey}}'))[1]::VARCHAR,
                    'Retail'
                ) AS StoreType,

               'Active' AS StoreStatus,

               -- Manager
               COALESCE(mgr.FirstName || ' ' || mgr.LastName, 'Unassigned')  AS ManagerName,

               -- Opening Date: Parse from XML or use ModifiedDate
               COALESCE(
                       TO_DATE(
                               (xpath('/ns:StoreSurvey/ns:YearOpened/text()',
                                      s.Demographics,
                                      '{{ns,http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey}}'))[1]:: VARCHAR || '-01-01',
                               'YYYY-MM-DD'
                       ),
                       s.ModifiedDate
               )      AS OpeningDate,

               -- Square Footage: Parse from XML
               COALESCE(
                       CAST(
                               (xpath('/ns:StoreSurvey/ns:SquareFeet/text()',
                                      s.Demographics,
                                      '{{ns,http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey}}'))[1]::VARCHAR AS INT
                    ),
                       1000
               )      AS SquareFootage,

               -- SCD2 fields
               s.ModifiedDate            AS ValidFromDate,
               DATE '9999-12-31'              AS ValidToDate,
               1                                AS IsCurrent,
               s.ModifiedDate         AS SourceUpdateDate,
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
           {% endraw %}
           """
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
            "Version"
        ],
        "Query": """
           SELECT
            -- Surrogate Key
            ('x' || MD5(e.BusinessEntityID::TEXT || COALESCE(e.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS EmployeeKey,
        
            -- Natural Key 
            e.BusinessEntityID AS EmployeeID,  -- ✓ Added comma
        
            p.FirstName || ' ' || COALESCE(p.MiddleName || ' ', '') || p.LastName AS EmployeeName,
            e.JobTitle,
        
            -- Current Department
            COALESCE(d.Name, 'Unassigned') AS Department,
        
            -- Reporting Manager (from OrganizationNode - simplified)
            NULL::BIGINT AS ReportingManagerKey,  -- ✓ Added comma
            e.HireDate,
        
            CASE
                WHEN e.CurrentFlag = TRUE THEN 'Active'
                ELSE 'Inactive'
            END AS EmployeeStatus,
        
            -- Sales territory info (if sales person)
            COALESCE(st."group", 'N/A') AS Region,
            COALESCE(st.Name, 'N/A') AS Territory,
            COALESCE(CAST(sp.SalesQuota AS DECIMAL(18, 2)), 0.00) AS SalesQuota,
        
            -- SCD2 fields
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
        """
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
            "IsActive"
        ],
        "Query": """
           SELECT
               -- Surrogate Key
               ('x' || MD5(so.SpecialOfferID::TEXT || COALESCE(so.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS PromotionKey, so.SpecialOfferID AS PromotionID,
               so.Description                                                               AS PromotionName,
               so.Description                                                               AS PromotionDescription,
               so.Type                                                                      AS PromotionType,

               CAST(so.DiscountPct * 100 AS DECIMAL(5, 2))                                  AS DiscountPercentage,
               0.00                                                                         AS DiscountAmount, -- Percentage-based, not fixed amount

               CASE
                   WHEN CURRENT_DATE BETWEEN so.StartDate AND so.EndDate THEN 'Active'
                   WHEN CURRENT_DATE < so.StartDate THEN 'Scheduled'
                   ELSE 'Expired'
                   END                                                                      AS PromotionStatus,

               so.Category                                                                  AS TargetCustomerSegment,
               so.SpecialOfferID                                                            AS CampaignID,
               NULL::BIGINT AS TargetProductKey, so.StartDate,
               so.EndDate,
               CASE WHEN CURRENT_DATE BETWEEN so.StartDate AND so.EndDate THEN 1 ELSE 0 END AS IsActive

           FROM Sales.SpecialOffer so
           ORDER BY so.SpecialOfferID;
           """
    },
    "DimProductCategory": {
        "Columns": [
            "ProductCategoryKey",
            "ProductCategoryID",
            "CategoryName",
            "CategoryDescription"
        ],
        "Query": """
            SELECT 
                ('x' || MD5(pc.ProductCategoryID::TEXT || pc.Name::TEXT ||pc.Name::TEXT ))::bit(32)::BIGINT AS ProductCategoryKey,
                pc.ProductCategoryID,
                pc.Name AS CategoryName,
                pc.Name AS CategoryDescription
            FROM Production.ProductCategory pc
            ORDER BY pc.ProductCategoryID;
        """
    },
    "DimWarehouse": {
        "Columns": [
            "WarehouseKey",
            "WarehouseID",
            "WarehouseName",
            "Location",
            "WarehouseType",
            "ManagerKey",
            "ValidFromDate",
            "ValidToDate",
            "IsCurrent"
        ],
        "Query": """
            SELECT 
                ('x' || MD5(l.LocationID::TEXT || COALESCE(l.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS WarehouseKey,
                l.LocationID AS WarehouseID,
                l.Name AS WarehouseName,
                l.Name AS Location,
                'Manufacturing' AS WarehouseType,
                NULL::BIGINT AS ManagerKey,
                l.ModifiedDate AS ValidFromDate,
                DATE '9999-12-31' AS ValidToDate,
                1 AS IsCurrent
            FROM Production.Location l
            ORDER BY l.LocationID;
        """
    },
    "DimSalesTerritory": {
        "Columns": [
            "TerritoryKey",
            "TerritoryID",
            "TerritoryName",
            "SalesRegion",
            "Country",
            "Manager",
            "SalesTarget"
        ],
        "Query": """
           SELECT
               -- Simple numeric surrogate key for lookup tables
               (
                   'x' || 
                   MD5(st.TerritoryID::TEXT || 
                   st.Name::TEXT || 
                   st."group"::TEXT || 
                   st.CountryRegionCode::TEXT)
                   )::bit(32)::BIGINT AS TerritoryKey,
               st.TerritoryID,
               st.Name                            AS TerritoryName,
               st."group"                         AS SalesRegion,
               st.CountryRegionCode               AS Country,
               'TBD'                              AS Manager,
               CAST(st.CostYTD AS DECIMAL(18, 2)) AS SalesTarget
           FROM Sales.SalesTerritory st
           ORDER BY st.TerritoryID;
           """
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
            "SourceUpdateDate"
        ],
        "Query": """
                 WITH vendor_performance AS (SELECT VendorID,
                                                    COUNT(*)                                                                   AS TotalOrders,
                                                    SUM(CASE WHEN ShipDate <= OrderDate + INTERVAL '7 days' THEN 1 ELSE 0 END) AS OnTimeOrders
                                             FROM Purchasing.PurchaseOrderHeader
                                             GROUP BY VendorID)
                 SELECT ('x' || MD5(v.BusinessEntityID::TEXT || COALESCE(v.ModifiedDate::TEXT, '')))::bit(32)::BIGINT AS VendorKey, v.BusinessEntityID AS VendorID,
                        v.Name                                                                                 AS VendorName,
                        COALESCE(cp.FirstName || ' ' || cp.LastName, 'Unknown')                                AS ContactPerson,
                        COALESCE(ea.EmailAddress, '')                                                          AS Email,
                        COALESCE(ph.PhoneNumber, '')                                                           AS Phone,
                        COALESCE(a.AddressLine1, '') ||
                        CASE
                            WHEN a.AddressLine2 IS NOT NULL THEN ', ' || a.AddressLine2
                            ELSE '' END                                                                        AS Address,
                        COALESCE(a.City, 'Unknown')                                                            AS City,
                        COALESCE(cr.Name, 'Unknown')                                                           AS Country,
                        CAST(v.CreditRating AS DECIMAL(3, 2)) / 5.0                                            AS VendorRating,
                        COALESCE(ROUND((vp.OnTimeOrders::DECIMAL / NULLIF(vp.TotalOrders, 0)) * 100, 2), 0.00) AS OnTimeDeliveryRate,
                        85.00                                                                                  AS QualityScore,
                        'Net 30'                                                                               AS PaymentTerms,
                        CASE WHEN v.ActiveFlag THEN 'Active' ELSE 'Inactive' END                               AS VendorStatus,
                        v.ModifiedDate                                                                         AS ValidFromDate,
                        DATE '9999-12-31'                                                                      AS ValidToDate,
                        CASE WHEN v.ActiveFlag THEN 1 ELSE 0 END                                               AS IsCurrent,
                        v.ModifiedDate                                                                         AS SourceUpdateDate
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
                 """
    },
    "DimRegion": {
        "Columns": [
            "RegionKey",
            "RegionID",
            "RegionName",
            "Country",
            "Continent",
            "TimeZone"
        ],
        "Query": """
                 -- Derive regions from SalesTerritory
                 SELECT DISTINCT ('x' || MD5(
                                    COALESCE(st."group", '') || '|' || 
                                    COALESCE(cr.name, '')
                                 ))::bit(32)::BIGINT AS RegionKey, 
                     ROW_NUMBER() OVER (ORDER BY st."group", cr.Name) AS RegionID, 
                     st."group" AS RegionName,
                                 cr.Name AS Country,
                                 CASE
                                     WHEN cr.Name IN ('United States', 'Canada') THEN 'North America'
                                     WHEN cr.Name IN ('United Kingdom', 'Germany', 'France') THEN 'Europe'
                                     WHEN cr.Name IN ('Australia') THEN 'Asia Pacific'
                                     ELSE 'Other'
                                     END AS Continent,
                                 'UTC'   AS TimeZone
                 FROM Sales.SalesTerritory st
                          INNER JOIN Person.CountryRegion cr ON st.CountryRegionCode = cr.CountryRegionCode
                 ORDER BY st."group", cr.Name;
                 """
    },
}

STATIC_LOOKUPS = {
    "DimFeedbackCategory": {
        "Columns": [
            "FeedbackCategoryKey",
            "FeedbackCategoryID",
            "CategoryName",
            "CategoryDescription"
        ],
        "Data": [
        (1, 1, 'Product Quality', 'Issues related to product quality'),
        (2, 2, 'Service', 'Customer service related feedback'),
        (3, 3, 'Delivery', 'Shipping and delivery feedback'),
        (4, 4, 'Pricing', 'Price-related feedback'),
        (5, 5, 'General', 'General comments')
        ]
    },
    "DimCustomerSegment": {
        "Columns": [
            "SegmentKey",
            "SegmentID",
            "SegmentName",
            "SegmentDescription",
            "DiscountTierStart",
            "DiscountTierEnd"
        ],
        "Data": [
            (1, 1, 'Premium', 'Customers with income > $150K', 20.00, 100.00),
            (2, 2, 'Gold', 'Customers with income $100K-$150K', 15.00, 19.99),
            (3, 3, 'Silver', 'Customers with income $50K-$100K', 10.00, 14.99),
            (4, 4, 'Bronze', 'Customers with income $25K-$50K', 5.00, 9.99),
            (5, 5, 'Standard', 'Customers with income < $25K', 0.00, 4.99),
            (6, 6, 'Business', 'B2B customers', 15.00, 100.00)
        ]
    },
    "DimAgingTier": {
        "Columns": [
            "AgingTierKey",
            "AgingTierID",
            "AgingTierName",
            "MinAgingDays",
            "MaxAgingDays"
        ],
        "Data": [
            (1, 1, 'Fresh', 0, 30),
            (2, 2, 'Aging', 31, 90),
            (3, 3, 'Old', 91, 180),
            (4, 4, 'Obsolete', 181, 999999)
        ]
    },
    "DimFinanceCategory":{
        "Columns": [
            "FinanceCategoryKey",
            "FinanceCategoryID",
            "CategoryName",
            "CategoryDescription"
        ],
        "Data": [
            (1, 1, 'Small Transaction', 'Transaction < $100'),
            (2, 2, 'Medium Transaction', 'Transaction $100-$1000'),
            (3, 3, 'Large Transaction', 'Transaction $1000-$10000'),
            (4, 4, 'Enterprise Transaction', 'Transaction > $10000'),
            (5, 5, 'Credit Transaction', 'Credit-based purchases')
        ]
    }
}

@dag(
    dag_id="adventureworks_dimension_population",
    dag_display_name="Populate Dimension Tables",
    schedule="@hourly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["adventureworks", "dimension", "population"]
)
def populate():

    @task
    def extract_dimension_columns(table, query):
        conn = pg()
        cur = conn.cursor()

        cur.execute(query)

        rows = cur.fetchall()

        with open(f'/tmp/dimension_{table.lower()}_data.pkl', 'wb') as file:
            pickle.dump(rows,file)

        return f'/tmp/dimension_{table.lower()}_data.pkl'


    @task
    def edit_dimension_data(file_path, columns, table):

        with open(file_path, 'rb') as file:
            data = pickle.load(file)

        clean_data = clean_values_in_rows(data, columns)

        with open(f'/tmp/clean_dimension_{table.lower()}_data.pkl', 'wb') as file:
            pickle.dump(clean_data, file)

        return f'/tmp/clean_dimension_{table.lower()}_data.pkl'


    @task
    def insert_dimension_columns(clean_file_path, columns, table):
        client = ch()

        try:
            with open(clean_file_path, 'rb') as file:
                data = pickle.load(file)

            partitioned = partition_rows_fixed_batch(data, batch_size=200)

            total_inserted = 0

            for partition_key, rows in partitioned.items():
                print(f"Inserting {len(rows)} rows for partition {partition_key}")
                check_insert(rows, columns)

                client.insert(
                    f"ADVENTUREWORKS_DWS.{table}",
                    rows,
                    columns
                )

                total_inserted += len(rows)

            print(f"Inserted {total_inserted} rows across {len(partitioned)} partitions")

            return total_inserted

        except Exception as e:
            print(f"Error loading data: {str(e)}")
            print(f"First row causing issue: {data[0] if data else 'No data'}")
            raise

    @task()
    def insert_static_dimension_columns(data, table, columns):
        client = ch()

        client.insert(
            f"ADVENTUREWORKS_DWS.{table}",
            data,
            columns
        )

    for table, content in DIMENSIONS.items():

        columns = content["Columns"]
        query = content["Query"]

        edc = extract_dimension_columns.override(task_display_name=f"Extract Data For {table}")(table, query)
        edd = edit_dimension_data.override(task_display_name=f"Edit Data For {table}")(edc, columns, table)
        idc = insert_dimension_columns.override(task_display_name=f"Insert Into {table}")(edd, columns, table)

        edc >> edd >> idc

    for table, items in STATIC_LOOKUPS.items():
        col = items["Columns"]
        dat = items["Data"]
        idc = insert_static_dimension_columns.override(task_display_name=f"Insert Into Static {table}")(dat, table, col)

        idc

populate()