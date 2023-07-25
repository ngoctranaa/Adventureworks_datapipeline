from dagster import Definitions, file_relative_path, load_assets_from_modules
from dagster_dbt import load_assets_from_dbt_project, DbtCliClientResource
import os
from . import assets
# from .assets.bronze_layer import raw_customer, raw_orders, raw_product, raw_productcategory, raw_productsubcategory, raw_territory
# from .assets.silver_layer import clean_customer, fact_sales
# from .assets.gold_layer import sales_by_category, sales_by_territory, dwh_sales_by_category, dwh_sales_by_territory
from .resources.minio_io_manager import MinIOIOManager
from .resources.spark_io_manager import SparkIOManager
from .resources.mysql_io_manager import MySQLIOManager
from .resources.psql_io_manager import PostgreSQLIOManager
#all_assets = load_assets_from_modules([assets])
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"),
    "database": os.getenv("MYSQL_DATABASE"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
}
MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
}
PSQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

DBT_PROJECT_PATH = file_relative_path(__file__, "../analytics")
DBT_PROFILES = file_relative_path(__file__, "../analytics")

resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager" : PostgreSQLIOManager(PSQL_CONFIG),
        "spark_io_manager": SparkIOManager(MINIO_CONFIG),
        # "dbt": DbtCliClientResource(
        #         project_dir= DBT_PROJECT_PATH,
        #         profiles_dir= DBT_PROFILES,
        # ),
}
defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources=resources
)
