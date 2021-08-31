# Databricks notebook source
circuits_df =  spark.read.option("header",True).csv("/mnt/tempmanish/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

#Schema before applying
circuits_df.printSchema()

# COMMAND ----------

infer_schema_circuits_df =  spark.read.option("inferschema",True).option("header",True).csv("/mnt/tempmanish/raw/circuits.csv")

# COMMAND ----------

infer_schema_circuits_df.printSchema()

# COMMAND ----------

#applying schema manually
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

manual_circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv("/mnt/tempmanish/raw/circuits.csv")

# COMMAND ----------

manual_circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#Ways to select columns from a data frame
circuits_selected_df = manual_circuits_df.select("circuitId").display()

# COMMAND ----------

#Ways to select columns from a data frame
circuits_selected_df = manual_circuits_df.select(manual_circuits_df["circuitId"]).display()

# COMMAND ----------

#using inbuilt function col to select required columns
circuits_selected_df = manual_circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt")).display()

# COMMAND ----------

#alias column name
#using inbuilt function col to select required columns
circuits_selected_df = manual_circuits_df.select(col("circuitId"), col("circuitRef"), col("name").alias("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt")).display()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import *

# COMMAND ----------

#Rename Column Names
circuits_renamed_df = manual_circuits_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#To add new columnn we use withColumn
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

#circuits_df.write.parquet("/mnt/tempmanish/processed/circuits/")
circuits_final_df.write.mode("overwrite").parquet("/mnt/tempmanish/processed/circuits/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/tempmanish/processed/circuits/

# COMMAND ----------

display(spark.read.parquet("/mnt/tempmanish/processed/circuits/"))

# COMMAND ----------

#run
