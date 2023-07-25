DROP TABLE IF EXISTS sales_by_category CASCADE;
CREATE TABLE sales_by_category (
    "Date" varchar(64),
    CategoryName varchar(64),
    "sum(Profit)" float4,
    "sum(Total_bill)" float4,
    PRIMARY KEY ("Date", CategoryName)
);

DROP TABLE IF EXISTS sales_by_territory CASCADE;
CREATE TABLE sales_by_territory (
    "Date" varchar(64),
    Region varchar(64),
    "sum(Profit)" float4,
    "sum(Total_bill)" float4,
    PRIMARY KEY ("Date", Region)
);

