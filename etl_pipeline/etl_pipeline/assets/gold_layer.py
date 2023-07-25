import pandas as pd
from dagster import asset, Output, AssetIn, multi_asset, AssetOut
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
@asset(
    ins = {
        "fact_sales"  : AssetIn(key_prefix = ["silver", "ecom"]),
        "raw_productcategory"  : AssetIn(key_prefix = ["bronze", "ecom"]),
        "raw_productsubcategory"  : AssetIn(key_prefix = ["bronze", "ecom"]),
    },
    key_prefix=["gold", "ecom"],
    io_manager_key="spark_io_manager",
    group_name="GOLD",
    compute_kind="PySpark"
)
def sales_by_category(fact_sales: pd.DataFrame, 
                      raw_productcategory: pd.DataFrame,
                      raw_productsubcategory: pd.DataFrame) -> Output[pd.DataFrame]:
    spark = (SparkSession.builder.appName("adventureworks-{}".format(datetime.today()))
            .master("spark://spark-master:7077")
            .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    factsales = spark.createDataFrame(fact_sales)
    cat = spark.createDataFrame(raw_productcategory)
    subcat = spark.createDataFrame(raw_productsubcategory)
    df = factsales.join(subcat, ["ProductSubcategoryKey"]).join(cat, ["ProductCategoryKey"])

    df = df.select(col("*"),to_date(col("OrderDate"),"yyyy-MM-dd").alias("date"))
    df4 = df.groupBy(date_format("date", "y-MM").alias('Date'), "CategoryName").sum("Total_bill")
    df5 = df.groupBy(date_format("date", "y-MM").alias('Date'), "CategoryName").sum("Profit")
    df6 = df5.join(df4, ["Date", "CategoryName"]).orderBy(col("Date").asc())
    
    pd_data = df6.toPandas()
    return Output(
        pd_data,
        metadata={
            "table": "sales_by_category",
            "records counts": len(pd_data),
        },
    )

@asset(
    ins = {
        "fact_sales"  : AssetIn(key_prefix = ["silver", "ecom"]),
        "raw_territory"  : AssetIn(key_prefix = ["bronze", "ecom"]),
    },
    key_prefix=["gold", "ecom"],
    io_manager_key="spark_io_manager",
    group_name="GOLD",
    compute_kind="PySpark"
)
def sales_by_territory(fact_sales: pd.DataFrame, 
                      raw_territory: pd.DataFrame) -> Output[pd.DataFrame]:
    spark = (SparkSession.builder.appName("adventureworks-{}".format(datetime.today()))
            .master("spark://spark-master:7077")
            .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    factsales = spark.createDataFrame(fact_sales)
    ter = spark.createDataFrame(raw_territory)
    df = factsales.join(ter, factsales.TerritoryKey == ter.SalesTerritoryKey)

    df = df.select(col("*"),to_date(col("OrderDate"),"yyyy-MM-dd").alias("date"))
    df4 = df.groupBy(date_format("date", "y-MM").alias('Date'), "Region").sum("Total_bill")
    df5 = df.groupBy(date_format("date", "y-MM").alias('Date'), "Region").sum("Profit")
    df6 = df5.join(df4, ["Date", "Region"]).orderBy(col("Date").asc())
    
    pd_data = df6.toPandas()
    return Output(
        pd_data,
        metadata={
            "table": "sales_by_territory",
            "records counts": len(pd_data),
        },
    )

@multi_asset(
    ins={
        "sales_by_category": AssetIn(key_prefix=["gold", "ecom"],)
    },
    outs={
        "sales_by_category": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            group_name="WAREHOUSE",
            metadata={
                "primary_keys": [
                    "Date",
                    "CategoryName",
                ],
                "columns": [
                    "Date",
                    "CategoryName",
                    "sum(Profit)",
                    "sum(Total_bill)",
                ],
            }
        )
    },
    compute_kind="Postgres"
    )
def dwh_sales_by_category(sales_by_category) -> Output[pd.DataFrame]:
    return Output(
        sales_by_category,
        metadata={
            "schema": "public",
            "table": "sales_by_category",
            "records counts": len(sales_by_category),
        },
    )

@multi_asset(
    ins={
        "sales_by_territory": AssetIn(key_prefix=["gold", "ecom"],)
    },
    outs={
        "sales_by_territory": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            group_name="WAREHOUSE",
            metadata={
                "primary_keys": [
                    "Date",
                    "Region",
                ],
                "columns": [
                    "Date",
                    "Region",
                    "sum(Profit)",
                    "sum(Total_bill)",
                ],
            }
        )
    },
    compute_kind="Postgres"
    )
def dwh_sales_by_territory(sales_by_territory) -> Output[pd.DataFrame]:
    return Output(
        sales_by_territory,
        metadata={
            "schema": "public",
            "table": "sales_by_territory",
            "records counts": len(sales_by_territory),
        },
    )

@asset(
    ins = {
        "fact_sales"  : AssetIn(key_prefix = ["silver", "ecom"]),
        "clean_customer"  : AssetIn(key_prefix = ["silver", "ecom"]),
    },
    key_prefix=["gold", "ecom"],
    io_manager_key="spark_io_manager",
    group_name="GOLD",
    compute_kind="PySpark"
)
def Loyal_customer(fact_sales: pd.DataFrame, 
                    clean_customer: pd.DataFrame) -> Output[pd.DataFrame]:
    spark = (SparkSession.builder.appName("adventureworks-{}".format(datetime.today()))
            .master("spark://spark-master:7077")
            .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    factsales = spark.createDataFrame(fact_sales)
    cus = spark.createDataFrame(clean_customer)
    factsales = factsales.groupBy("CustomerKey").sum("OrderQuantity")
    factsales = factsales.select(col("CustomerKey"), col("sum(OrderQuantity)").alias("Total"))
    factsales = factsales.select(col("*")).where(factsales.Total >50)

    cus = cus.select(col("*")).where((cus.Prefix != 'NA') & (cus.EmailAddress != 'NA'))

    df = cus.join(factsales, ["CustomerKey"])
    df = df.select("CustomerKey", "Prefix", "FirstName", "LastName", "EmailAddress", "Total")
    pd_data = df.toPandas()
    return Output(
        pd_data,
        metadata={
            "table": "Loyal_customer",
            "records counts": len(pd_data),
        },
    )

@multi_asset(
    ins={
        "Loyal_customer": AssetIn(key_prefix=["gold", "ecom"],)
    },
    outs={
        "Loyal_customer": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            group_name="WAREHOUSE",
            metadata={
                "primary_keys": [
                    "CustomerKey",
                ],
                "columns": [
                    "CustomerKey",
                    "Prefix",
                    "FirstName",
                    "LastName",
                    "EmailAddress",
                    "Total"
                ],
            }
        )
    },
    compute_kind="Postgres"
    )
def dwh_Loyal_customer(Loyal_customer) -> Output[pd.DataFrame]:
    return Output(
        Loyal_customer,
        metadata={
            "schema": "public",
            "table": "Loyal_customer",
            "records counts": len(Loyal_customer),
        },
    )