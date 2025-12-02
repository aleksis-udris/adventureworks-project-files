CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimDate
(
    # Surogātatslēga
    DateKey            Int32,

    # Pilnais Datums
    FullDate           Date,

    # Datumu vienības
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

    # Boolean vērtības
    IsWeekend          UInt8,
    IsHoliday          UInt8,

    # Apzīmētāji
    HolidayName        LowCardinality(String),
    FiscalYear         UInt16,
    FiscalQuarter      UInt8,
    FiscalMonth        UInt8,
    Season             LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY DateKey
ORDER BY DateKey;

CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimCustomer (
    # Surogātatslēga & naturālā atslēga
    CustomerKey Int32,
    CustomerID Int32,

    # Pircēja Atribūti
    CustomerName String,
    Email String,
    Phone String,
    City LowCardinality(String),
    StateProvince LowCardinality(String),
    Country LowCardinality(String),
    PostalCode String,

    # Apzīmētāji
    CustomerSegment LowCardinality(String),
    CustomerType LowCardinality(String),
    AccountStatus LowCardinality(String),
    CreditLimit Decimal(18, 2),
    AnnualIncome Decimal(18, 2),
    YearsSinceFirstPurchase Int32,

    # Vēsturei aktuālie mainīgie
    ValidFromDate Date,
    ValidToDate Date,
    IsCurrent UInt8,
    SourceUpdateDate Date,
    EffectiveStartDate Date,
    EffectiveEndDate Date,

    # Versionēšana
    Version UInt64
) ENGINE = ReplacingMergeTree(Version) PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY
    (CustomerID, ValidFromDate);

CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimProduct (
    # Atslēgas
    ProductKey Int32,
    ProductID Int32,

    # Atribūti
    ProductName String,
    SKU String,
    Category LowCardinality(String),
    SubCategory LowCardinality(String),
    Brand LowCardinality(String),
    ListPrice Decimal(18, 2),
    Cost Decimal(18, 2),
    ProductStatus LowCardinality(String),
    Color LowCardinality(String),
    Size LowCardinality(String),
    Weight Decimal(10, 3),

    # Vēstures Dati
    ValidFromDate Date,
    ValidToDate Date,
    IsCurrent UInt8,
    SourceUpdateDate Date,
    EffectiveStartDate Date,
    EffectiveEndDate Date,

    # Versionēšana
    Version UInt64
) ENGINE = ReplacingMergeTree(Version) PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY
    (ProductID, ValidFromDate);

CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimStore (
    # Atslēgas
    StoreKey Int32,
    StoreID Int32,

    # Atribūti
    StoreName String,
    StoreNumber Int32,
    Address String,
    City LowCardinality(String),
    StateProvince LowCardinality(String),
    Country LowCardinality(String),
    PostalCode String,
    Region LowCardinality(String),
    Territory LowCardinality(String),
    StoreType LowCardinality(String),
    StoreStatus LowCardinality(String),
    ManagerName String,
    OpeningDate Date,
    SquareFootage Int32,

    # Vēstures Atribūti
    ValidFromDate Date,
    ValidToDate Date,
    IsCurrent UInt8,
    SourceUpdateDate Date,

    # Versionēšana
    Version UInt64
) ENGINE = ReplacingMergeTree(Version) PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY
    (StoreID, ValidFromDate);

CREATE TABLE IF NOT EXISTS ADVENTUREWORKS_DWS.DimEmployee
(
    # Atslēgas
    EmployeeKey          Int32,
    EmployeeID           Int32,

    # Atribūti
    EmployeeName         String,
    JobTitle             LowCardinality(String),
    Department           LowCardinality(String),

    # Pašreferencētā atslēga
    ReportingManagerKey  Int32,

    # Aprakstošie lielumi
    HireDate             Date,
    EmployeeStatus       LowCardinality(String),
    Region               LowCardinality(String),
    Territory            LowCardinality(String),
    SalesQuota           Decimal(18,2),

    # Vēstures atribūti
    ValidFromDate        Date,
    ValidToDate          Date,
    IsCurrent            UInt8,
    SourceUpdateDate     Date,

    # Versionēšana
    Version              UInt64
)
ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMM(ValidFromDate)
ORDER BY (EmployeeID, ValidFromDate);