# Databricks notebook source
dbutils.fs.mkdirs("/FileStore/Project/Bronze")

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/Project/Silver")


# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/Project/Gold")


# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/Project/Bronze/archive/_/2025061119371749670628")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/Project/Bronze/archive/_")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/Project/Bronze/archive")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/Project/Bronze/sales_data_sample-1.csv")