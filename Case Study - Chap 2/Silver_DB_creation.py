# Databricks notebook source
spark.sql("drop database if exists project_db_silver")

# COMMAND ----------

spark.sql("create database  if not exists project_db_silver")

# COMMAND ----------

spark.sql("Show databases").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC use project_db_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()