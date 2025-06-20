# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS project_db_gold.status;
# MAGIC DROP TABLE IF EXISTS project_db_gold.address;
# MAGIC DROP TABLE IF EXISTS project_db_gold.customer;
# MAGIC DROP TABLE IF EXISTS project_db_gold.product;
# MAGIC DROP TABLE IF EXISTS project_db_gold.orderdate;
# MAGIC DROP TABLE IF EXISTS project_db_gold.deal;
# MAGIC DROP TABLE IF EXISTS project_db_gold.sales;

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/project_db_gold.db/", recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC use database project_db_gold

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE Status AS
    SELECT DISTINCT
        STATUS
    FROM project_db_silver.salessilver
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_db_gold.status

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE Product AS
    SELECT DISTINCT
       PRODUCTCODE,
       PRODUCTLINE,
       MSRP
    FROM project_db_silver.salessilver
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_db_gold.Product

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE Customer AS
    SELECT DISTINCT
          CUSTOMERNAME ,
          CONTACTFIRSTNAME  ,
          CONTACTLASTNAME ,
          PHONE,
          ADDRESSLINE1 ,
          ADDRESSLINE2  ,
          CITY ,
          STATE ,
          POSTALCODE ,
          COUNTRY ,
          TERRITORY 
    FROM project_db_silver.salessilver
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_db_gold.Customer

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE OrderDate AS
    SELECT DISTINCT
            ORDERDATE,
            QTR_ID,
            MONTH_ID,
            YEAR_ID          
    FROM project_db_silver.salessilver
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_db_gold.OrderDate
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS project_db_gold.sales;

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE Sales AS
    SELECT 
     ORDERLINENUMBER ,
     ORDERNUMBER ,
     QUANTITYORDERED ,
     PRICEEACH,
     SALES ,
     ORDERDATE,
     STATUS,
     PRODUCTCODE,
     CUSTOMERNAME
    From project_db_silver.salessilver
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from project_db_gold.Sales