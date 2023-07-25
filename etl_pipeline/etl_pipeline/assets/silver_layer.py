import pandas as pd
from dagster import asset, Output, AssetIn
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
@asset(
    ins = {
        "raw_customer"  : AssetIn(key_prefix = ["bronze", "ecom"]),
    },
    key_prefix=["silver", "ecom"],
    io_manager_key="spark_io_manager",
    group_name="SILVER",
    compute_kind="PySpark"
)
def clean_customer(raw_customer: pd.DataFrame) -> Output[pd.DataFrame]:
    spark = (SparkSession.builder.appName("adventureworks-{}".format(datetime.today()))
            .master("spark://spark-master:7077")
            .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    df = spark.createDataFrame(raw_customer)
    df = df.withColumn('Prefix', when(col('Prefix') == '', "NA").otherwise(col('Prefix')))
    pd_data = df.toPandas()
    return Output(
        pd_data,
        metadata={
            "table": "customer",
            "records counts": len(pd_data),
        },
    )

@asset(
    ins = {
        "raw_product"  : AssetIn(key_prefix = ["bronze", "ecom"]),
        "raw_orders"  : AssetIn(key_prefix = ["bronze", "ecom"]),
    },
    key_prefix=["silver", "ecom"],
    io_manager_key="spark_io_manager",
    group_name="SILVER",
    compute_kind="PySpark"
)
def fact_sales(raw_product: pd.DataFrame, raw_orders: pd.DataFrame) -> Output[pd.DataFrame]:
    spark = (SparkSession.builder.appName("adventureworks-{}".format(datetime.today()))
            .master("spark://spark-master:7077")
            .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    product = spark.createDataFrame(raw_product)
    orders = spark.createDataFrame(raw_orders)
    df = orders.join(product, ["ProductKey"])
    df = df.withColumn("Total_bill", df.OrderQuantity * df.ProductPrice)
    df = df.withColumn("Profit", df.OrderQuantity * (df.ProductPrice - df.ProductCost))
    df = df.select(col("OrderNumber"), col("OrderQuantity"), col("ProductKey"), col("CustomerKey"), col("ProductSubcategoryKey"), col("OrderDate"), col("TerritoryKey"), col("Total_bill"), col("Profit"))
    pd_data = df.toPandas()
    return Output(
        pd_data,
        metadata={
            "table": "fact_sales",
            "records counts": len(pd_data),
        },
    )

