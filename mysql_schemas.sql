DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
    CustomerKey varchar(64),
    Prefix varchar(10),
    FirstName varchar(64),
    LastName varchar(64),
    BirthDate varchar(64),
    MaritalStatus char(1),
    Gender char(1),
    EmailAddress varchar(64),
    AnnualIncome varchar(64),
    TotalChildren int4,
    EducationLevel varchar(64),
    Occupation varchar(64),
    HomeOwner char(1),
    PRIMARY KEY (CustomerKey)
);

DROP TABLE IF EXISTS productcategory;
CREATE TABLE productcategory (
    ProductCategoryKey int4,
    CategoryName varchar(64),
    PRIMARY KEY (ProductCategoryKey)
);

DROP TABLE IF EXISTS productsubcategory;
CREATE TABLE productsubcategory (
    ProductSubCategoryKey int4,
    SubcategoryName varchar(64),
    ProductCategoryKey int4,
    PRIMARY KEY (ProductSubCategoryKey)
);

DROP TABLE IF EXISTS product;
CREATE TABLE product (
    ProductKey int4,
    ProductSubCategoryKey int4,
    ProductSKU varchar(64),
    ProductName varchar(64),
    ModelName varchar(64),
    ProductDescription varchar(200),
    ProductColor varchar(64),
    ProductSize varchar(20),
    ProductStyle varchar(20),
    ProductCost float4,
    ProductPrice float4,
    PRIMARY KEY (ProductKey)
);

DROP TABLE IF EXISTS territory;
CREATE TABLE territory (
    SalesTerritoryKey int4,
    Region varchar(64),
    Country varchar(64),
    Continent varchar(64),
    PRIMARY KEY (SalesTerritoryKey)
);

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    OrderDate varchar(64),
    StockDate varchar(64),
    OrderNumber varchar(64),
    ProductKey int4,
    CustomerKey varchar(64),
    TerritoryKey int4,
    OrderLine int4,
    OrderQuantity int4,
    PRIMARY KEY (OrderNumber, ProductKey)
);
