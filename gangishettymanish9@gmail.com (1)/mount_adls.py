# Databricks notebook source
dbutils.widgets.text("storage_account_name","storage_account_name")
storage_account_name =  dbutils.widgets.get("storage_account_name")

# COMMAND ----------

print(storage_account_name)

# COMMAND ----------

storage_account_name = storage_account_name
client_id            = "30e4a67e-7be6-45be-b3c7-71dee2977373"
tenant_id            = "fb7865b5-f282-43d0-b910-8c1fc745ae97"
client_secret        = "9EzbVlrDR.M5R5ef9b.SLI.-qKw3i~e6l3"

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder\
     .master("local")\
     .appName("Sample")\
     .getOrCreate()

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.unmount("/mnt/tempmanish/raw/")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

container_name = "raw"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/tempmanish/raw/

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/tempmanish/raw")

# COMMAND ----------

df = spark.read.option("header",True).csv("abfss://tempmanish.blob.core.windows.net/raw/ExportCSV.csv")

# COMMAND ----------

df = spark.read.option("header",True).csv("/mnt/tempmanish/raw/ExportCSV.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

print("No of Records in a DataFrame ==> ",df.count())

# COMMAND ----------

#from pyspark.sql.functions import col
#df = df.select(col("Job Title").alias("Job_Title"))
#df = df.select(col("Email Address").alias("Email"))
#df = df.select(col("FirstName LastName").alias("First_Last_Name"))

# COMMAND ----------

df.columns

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.parquet("/mnt/tempmanish/raw/ExportCSV.parquet")

# COMMAND ----------

for file in dbutils.fs.ls("/mnt/tempmanish/raw"):
  print(file)

# COMMAND ----------

for file in dbutils.fs.ls("/mnt/tempmanish/raw"):
  print("-------------------------")
  print("Dbfs path  ==> ",file[0])
  print("File  Name ==> ",file[1])
  print("Size       ==> ",file[2])
  print("-------------------------")

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("demo")

# COMMAND ----------

# To Unmount
#dbutils.fs.unmount("/mnt/tempmanish/raw")
#dbutils.fs.unmount("/mnt/tempmanish/processed")

# COMMAND ----------

dbutils.fs.mv("/mnt/tempmanish/check/circuits.csv","/mnt/tempmanish/raw/circuits.csv")

# COMMAND ----------


