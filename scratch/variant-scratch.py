# Databricks notebook source
# MAGIC %md
# MAGIC Uncomment and run the following `%pip install` commands or set the Python Base Environment for the notebook using "resources/environment.yml".  

# COMMAND ----------

# %pip install databricks-sdk --upgrade

# COMMAND ----------

# %pip install git+https://github.com/databrickslabs/dbignite.git

# COMMAND ----------

from dbignite.fhir_mapping_model import FhirSchemaModel

# COMMAND ----------

# MAGIC %sql
# MAGIC USE redox.main;

# COMMAND ----------

df = spark.table("bundle_meta_parsed")
display(df)

# COMMAND ----------

from pyspark.sql import functions as F

def transpose_dataframe(df):
    # Collect column names
    cols = df.columns
    
    # Create a new DataFrame with columns as rows
    transposed_df = df.select(F.posexplode(F.array(*[F.struct(F.lit(c).alias("key"), F.col(c).alias("value")) for c in cols]))).select("pos", "col.key", "col.value")
    
    # Pivot the DataFrame to get the transposed format
    transposed_df = transposed_df.groupBy("key").pivot("pos").agg(F.first("value"))
    
    return transposed_df

transposed_df = transpose_dataframe(df)
display(transposed_df)
