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

grouping_cols = [col for col in df.columns if col not in ["pos", "key", "value"]]

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

display(df.select(*grouping_cols))

# COMMAND ----------

tdf = df.groupBy(*grouping_cols).pivot("key").agg(first("value"))

# COMMAND ----------

display(tdf)

# COMMAND ----------

display(tdf2)
