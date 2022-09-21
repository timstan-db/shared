# Databricks notebook source
# DBTITLE 1,Explore databricks-datasets on DBFS
# Explore DBFS
display(dbutils.fs.ls('/databricks-datasets/retail-org/sales_orders'))

# COMMAND ----------

# DBTITLE 1,View raw json
# MAGIC %fs head dbfs:/databricks-datasets/retail-org/sales_orders/part-00000-tid-1771549084454148016-e2275afd-a5bb-40ed-b044-1774c0fdab2b-105592-1-c000.json

# COMMAND ----------

# DBTITLE 1,Convert raw json into Spark DataFrame
sdf1 = spark.read.format("json").load("/databricks-datasets/retail-org/sales_orders/")
display(sdf1)

# COMMAND ----------

# DBTITLE 1,Schema shows a nested structure
sdf1.printSchema()

# COMMAND ----------

# DBTITLE 1,Confirming number of DataFrame partitions
sdf1.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC Now, let's write this DataFrame as a Delta table, read it back into memory, and see if the data structure has changed

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.timstanton

# COMMAND ----------

# DBTITLE 1,Write nested Spark DataFrame to Delta
sdf1.write.mode("overwrite").saveAsTable('hive_metastore.timstanton.json_test')

# COMMAND ----------

# DBTITLE 1,Read back into memory from Delta
sdf2 = spark.read.table('hive_metastore.timstanton.json_test')

# COMMAND ----------

# DBTITLE 1,DataFrames are identical
print(sdf1.schema == sdf2.schema)
print(sdf1.collect() == sdf2.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC Similarly, let's try writing back out to a json file, then reading back in to test if anything has changed.

# COMMAND ----------

# DBTITLE 1,Write out a new json file
sdf2.write.mode("overwrite").json("/tmp/sales_orders.json")

# COMMAND ----------

# DBTITLE 1,Read new json back into another Spark DataFrame
sdf3 = spark.read.format("json").load("/tmp/sales_orders.json")

# COMMAND ----------

# DBTITLE 1,DataFrames are still identical
print(sdf1.schema == sdf3.schema)
print(sdf1.collect() == sdf3.collect())
