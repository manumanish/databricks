# Databricks notebook source
print("Hello World")
from pyspark.sql import SparkSession

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder\
     .master("local")\
     .appName("Sample")\
     .getOrCreate()

# COMMAND ----------

import pandas

# COMMAND ----------


