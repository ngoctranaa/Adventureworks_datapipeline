# ETL DATA PIPELINE
# Project main data flow diagram
![image](https://github.com/ngoctranaa/Adventureworks_datapipeline/assets/93460170/1af23fc1-a79d-428e-afe6-d112f27d1505)
- Docker: build services in the project (DBMS, Spark, MinIO, ...)
- Dagster: orchestraion - Visualize data pipeline into data lineage
- MySQL: Initial data in .csv format will be loaded into mysql database
- MinIO: build data platform (data lake), in which:
  + Bronze Layer: raw data extracted from mysql
  + Silver Layer: data cleaning
  + Gold Layer: data used for analysis, will be loaded to Postgres (Data Warehouse)
- Postgres: Data warehouse
- Metabase: get table from Postgres to visualize data
- DBT (in progess): Used for data analysis purposes, often used to transform data when sql query are complex when using python or pyspark library
- Apache Spark, Pandas: Transform data at Silver and Gold Layer, using Pyspark Library and Spark Session from Docker
# Data Lineage 
![image](https://github.com/ngoctranaa/Adventureworks_datapipeline/assets/93460170/90ecbe77-c068-41ca-b794-bb6de9d04cba)


