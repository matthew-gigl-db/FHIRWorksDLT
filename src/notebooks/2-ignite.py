# Databricks notebook source
# MAGIC %pip install git+https://github.com/databrickslabs/dbignite.git

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import dbignite

# COMMAND ----------

help(dbignite)

# COMMAND ----------

from dbignite.readers import FhirFormat

# COMMAND ----------

from dbignite.readers import read_from_stream

# COMMAND ----------

df = spark.table("redox.main.fhir_bronze")

# COMMAND ----------

display(df)

# COMMAND ----------

df2 = (
  df
  .withColumn("bundle", dbignite.fhir_mapping_model.json.loads(dbignite.readers.col("response")))
)

# COMMAND ----------


