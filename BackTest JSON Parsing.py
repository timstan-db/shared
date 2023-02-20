# Databricks notebook source
# MAGIC %md
# MAGIC ## Read json, infer schema, convert column between nested struct and string

# COMMAND ----------

# import
raw = (spark.read
       .format("json")
       .option("multiline", "true")
       .load("dbfs:/FileStore/timstanton/backTest.json"))

# inspect
raw.printSchema()
display(raw)

# COMMAND ----------

# DBTITLE 1,Convert TestResults column from nested structure into string format to be combined with other tests
from pyspark.sql.functions import to_json
string_col = raw.withColumn("TestResults", to_json(raw.TestResults))
string_col.printSchema()
display(string_col)

# COMMAND ----------

# DBTITLE 1,Convert back to original nested json structure
from pyspark.sql.functions import from_json, schema_of_json, lit, col

# this is a string with the json structure
stringTestResults = string_col.select("TestResults").collect()[0][0]

# create string of schema in the correct format for from_json()
schemaTestResults = string_col.select(schema_of_json(lit(stringTestResults))).collect()[0][0]

# converts TestResults column from string back to nested json
nested_col = string_col.withColumn("TestResults", from_json(string_col.TestResults, schemaTestResults))

# confirm this worked properly
print("schema matches with original: ", raw.schema == nested_col.schema)
nested_col.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## FYI - an example of how to dynamically flatten any json

# COMMAND ----------

# DBTITLE 1,Define json flattening function
from typing import Final, Dict, Tuple
from pyspark.sql import DataFrame as SDF
from pyspark.sql.functions import *

def rename_dataframe_cols(df: SDF, col_names: Dict[str, str]) -> SDF:
  """
  Rename all columns in dataframe
  """
  return df.select(*[col(col_name).alias(col_names.get(col_name, col_name)) for col_name in df.columns])

def update_column_names(df: SDF, index: int) -> SDF:
  df_temp = df
  all_cols = df_temp.columns
  new_cols = dict((column, f"{column}*{index}") for column in all_cols)
  df_temp = df_temp.transform(lambda df_x: rename_dataframe_cols(df_x, new_cols))
  
  return df_temp

def flatten_json(df_arg: SDF, index: int = 1) -> SDF:
  """
  Flatten json in a Spark DataFrame using recursion
  """
  # Update all column names with index 1
  df = update_column_names(df_arg, index) if index == 1 else df_arg
  
  # Get all field names from DataFrame
  fields = df.schema.fields
  
  # For all columns in the DataFrame
  for field in fields:
    data_type = str(field.dataType)
    column_name = field.name
    
    first_10_chars = data_type[0:10]
    
    # If it is Array column
    if first_10_chars == 'ArrayType(':
      # Explode Array column
      df_temp = df.withColumn(column_name, explode_outer(col(column_name)))
      return flatten_json(df_temp, index + 1)
    
    # If it is a json object
    elif first_10_chars == 'StructType':
      current_col = column_name
      
      append_str = current_col
      
      # Get data type of current column
      data_type_str = str(df.schema[current_col].dataType)
      
      # Change the column name if the current column name exists in the data type string
      df_temp = df.withColumnRenamed(column_name, column_name + "#1") \
          if column_name in data_type_str else df
      current_col = current_col + "#1" if column_name in data_type_str else current_col
      
      # Expand struct column values
      df_before_expanding = df_temp.select(f"{current_col}.*")
      newly_gen_cols = df_before_expanding.columns
      
      # Find next level value for the column
      begin_index = append_str.rfind('*')
      end_index = len(append_str)
      level = append_str[begin_index + 1: end_index]
      next_level = int(level) + 1
      
      # Update column names with new level
      custom_cols = dict((field, f"{append_str}->{field}*{next_level}") for field in newly_gen_cols)
      df_temp2 = df_temp.select("*", f"{current_col}.*").drop(current_col)
      df_temp3 = df_temp2.transform(lambda df_x: rename_dataframe_cols(df_x, custom_cols))
      return flatten_json(df_temp3, index + 1)
  return df


# COMMAND ----------

flattened = flatten_json(raw)
flattened.printSchema()
display(flattened)

# COMMAND ----------


