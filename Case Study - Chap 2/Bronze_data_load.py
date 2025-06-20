# Databricks notebook source
filePath="dbfs:/FileStore/Project/Bronze/sales_data_sample.csv"



# COMMAND ----------

df=spark.read.csv(filePath, header=True, inferSchema=True)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  For incremental loading purpose
# MAGIC Added new timestamp column
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp 
df2=df.withColumn("loading_timestamp", current_timestamp())
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC Save as Delta table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use database project_db_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS SalesBronze

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/project_db_bronze.db/salesbronze", recurse=True)

# COMMAND ----------

df2.write.format("delta").mode("append").saveAsTable("SalesBronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from SalesBronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SalesBronze limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC Move the already loaded file to archive folder

# COMMAND ----------

import datetime
archive_folder="dbfs:/FileStore/Project/Bronze/archive/"
archive_filepath=archive_folder+'_'+datetime.datetime.now().strftime("/%Y%m%d%H%M%s")
dbutils.fs.mv(filePath,archive_filepath)
print(archive_filepath)

# COMMAND ----------

