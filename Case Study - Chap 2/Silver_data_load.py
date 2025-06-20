# Databricks notebook source
# MAGIC %sql
# MAGIC use database project_db_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS salesSilver

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/project_db_silver.db/", recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table IF NOT EXISTS salesSilver(
# MAGIC ORDERNUMBER integer,
# MAGIC QUANTITYORDERED  integer,
# MAGIC PRICEEACH  double,
# MAGIC ORDERLINENUMBER  integer,
# MAGIC SALES  double,
# MAGIC ORDERDATE  DATE,
# MAGIC STATUS  string,
# MAGIC QTR_ID  integer,
# MAGIC MONTH_ID  integer,
# MAGIC YEAR_ID  integer,
# MAGIC PRODUCTLINE  string,
# MAGIC MSRP  integer,
# MAGIC PRODUCTCODE  string,
# MAGIC CUSTOMERNAME  string,
# MAGIC PHONE  string,
# MAGIC ADDRESSLINE1  string,
# MAGIC ADDRESSLINE2  string,
# MAGIC CITY  string,
# MAGIC STATE  string,
# MAGIC POSTALCODE  string,
# MAGIC COUNTRY  string,
# MAGIC TERRITORY  string,
# MAGIC CONTACTLASTNAME string,
# MAGIC CONTACTFIRSTNAME string,
# MAGIC DEALSIZE string,
# MAGIC last_refresh_day TIMESTAMP
# MAGIC )

# COMMAND ----------

display(spark.sql("select * from salesSilver"))

# COMMAND ----------

last_processed_df = spark.sql("SELECT MAX(last_refresh_day) as last_processed FROM salesSilver")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
    last_processed_timestamp = "1900-01-01T00:00:00.000+00:00"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW bronze_incremental AS
SELECT *
FROM project_db_bronze.salesbronze c where  c.loading_timestamp > '{last_processed_timestamp}'
""")

# COMMAND ----------

spark.sql("select count(*) from bronze_incremental").show()

# COMMAND ----------

spark.sql("select * from bronze_incremental limit 2").show()

# COMMAND ----------

temp_view_name = "bronze_incremental"

# Get list of columns
columns = spark.table(temp_view_name).columns

# Create individual SELECT statements for each column
select_statements = [
    f"SELECT '{col}' AS column_name, SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_count FROM {temp_view_name}"
    for col in columns
]

# Combine them with UNION ALL
final_query = " UNION ALL ".join(select_statements)

# Run the final query
spark.sql(final_query).show()

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW silver_incremental AS
SELECT
   ORDERNUMBER ,
   QUANTITYORDERED ,
   PRICEEACH  ,
   ORDERLINENUMBER ,
   SALES  ,
   TO_DATE(orderdate, 'M/d/yyyy H:mm') as ORDERDATE ,
   STATUS ,
   QTR_ID  ,
   MONTH_ID ,
   YEAR_ID ,
   PRODUCTLINE ,
   MSRP  ,
   PRODUCTCODE ,
   CUSTOMERNAME ,
   PHONE ,
   ADDRESSLINE1 ,
   ADDRESSLINE2  ,
   CITY ,
   STATE ,
   CASE
        WHEN POSTALCODE IS NULL  THEN 0
        ELSE POSTALCODE
    END AS POSTALCODE,
   COUNTRY ,
   TERRITORY ,
   CONTACTLASTNAME,
   CONTACTFIRSTNAME,
   DEALSIZE ,  
   CURRENT_TIMESTAMP() AS last_refresh_day
FROM bronze_incremental
""")

# COMMAND ----------

display(spark.sql("select * from silver_incremental"))

# COMMAND ----------

display(spark.sql("select * from salesSilver"))

# COMMAND ----------

spark.sql("""
MERGE INTO salesSilver target
USING silver_incremental source
ON target.ORDERNUMBER = source.ORDERNUMBER
AND target.ORDERLINENUMBER = source.ORDERLINENUMBER
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")

# COMMAND ----------

spark.sql("select count(*) from salesSilver").show()