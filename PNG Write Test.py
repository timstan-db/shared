# Databricks notebook source
# MAGIC %md
# MAGIC # Method 1: Mount ADLS Container onto DBFS
# MAGIC This can be any storage container within Azure (ADLS Gen1, ADLS Gen2, Blob, etc.)

# COMMAND ----------

# DBTITLE 1,Set up mount (this only needs to be done once)
# from docs: https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<application-id>",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Write PNG file to mounted location. This is now accessible directly from Azure.
import matplotlib.pyplot as plt
 
# data for plotting
x = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
y = [5, 7, 8, 1, 4, 9, 6, 3, 5, 2, 1, 8]
 
plt.plot(x, y)
 
plt.xlabel('x-axis label')
plt.ylabel('y-axis label')
plt.title('Matplotlib Example')
 
plt.savefig("/dbfs/mnt/<mount-name>/output.png")

# COMMAND ----------

# MAGIC %md
# MAGIC # Method 2: Use External Location with Unity Catalog
# MAGIC This is the preferred method long-term because it is fully integrated with Unity Catalog. Must be ADLS Gen2.

# COMMAND ----------

# DBTITLE 1,Background info
# from docs: https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-external-locations-and-credentials
# Once storage credential and permissions are set up (per docs above), then run the next cell.

# COMMAND ----------

# DBTITLE 1,Create external location (this only needs to be done once)
spark.sql("CREATE EXTERNAL LOCATION <location_name> "
  "URL 'abfss://<container_name>@<storage_account>.dfs.core.windows.net/<path>' "
  "WITH ([STORAGE] CREDENTIAL <storage_credential_name>)")

# COMMAND ----------

# DBTITLE 1,Write PNG file to UC external location. This is now accessible directly from Azure.
plt.savefig("abfss://<container_name>@<storage_account>.dfs.core.windows.net/<path>/output.png")
