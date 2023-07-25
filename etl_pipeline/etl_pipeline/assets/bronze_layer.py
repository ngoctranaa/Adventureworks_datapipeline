import pandas as pd
from dagster import asset, Output

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    group_name="BRONZE",
    compute_kind="MySQL"
)
def raw_customer(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM customer"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
        "table": "customer",
        "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    group_name="BRONZE",
    compute_kind="MySQL"
)
def raw_product(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM product"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
        "table": "product",
        "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    group_name="BRONZE",
    compute_kind="MySQL"
)
def raw_productcategory(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM productcategory"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
        "table": "productcategory",
        "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    group_name="BRONZE",
    compute_kind="MySQL"
)
def raw_productsubcategory(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM productsubcategory"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
        "table": "productsubcategory",
        "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    group_name="BRONZE",
    compute_kind="MySQL"
)
def raw_territory(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM territory"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
        "table": "territory",
        "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    group_name="BRONZE",
    compute_kind="MySQL"
)
def raw_orders(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM orders"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
        "table": "orders",
        "records count": len(pd_data),
        },
    )