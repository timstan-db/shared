# Databricks notebook source
# MAGIC %md
# MAGIC ### Using R
# MAGIC Example from https://stackoverflow.com/questions/51205139/export-tojson-list-of-list-in-r

# COMMAND ----------

# MAGIC %r
# MAGIC l1 <- list(timestamp = "2018-02-01 10:20", 
# MAGIC            aux = list(list(id = "x1", prog = rep('A',3)),
# MAGIC                       list(id = "x2", prog = rep('A',3)),
# MAGIC                       list(id = "x3", prog = rep('A',3))))
# MAGIC 
# MAGIC json_output <- jsonlite::toJSON(l1, pretty = TRUE, auto_unbox = TRUE)
# MAGIC json_output

# COMMAND ----------

# MAGIC %r
# MAGIC write(json_output, "/dbfs/Users/tim.stanton@databricks.com/output.json")

# COMMAND ----------


