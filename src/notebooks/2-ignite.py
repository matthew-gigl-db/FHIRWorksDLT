# Databricks notebook source
# MAGIC %pip install git+https://github.com/databrickslabs/dbignite.git

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import dbignite

# COMMAND ----------

help(dbignite)

# COMMAND ----------

from dbignite.fhir_resource import FhirResource

# COMMAND ----------

df = spark.table("redox.main.fhir_bronze")

# COMMAND ----------

display(df)

# COMMAND ----------

bundle = FhirResource.from_raw_bundle_resource(df)

# COMMAND ----------

bundle.entry()

# COMMAND ----------

from dbignite.fhir_mapping_model import FhirSchemaModel

# COMMAND ----------

FhirSchemaModel().list_keys()

# COMMAND ----------

df = bundle.entry()

# COMMAND ----------

display(df.limit(1))

# COMMAND ----------

from pyspark.sql.functions import explode, col

patient = df.withColumn("Patient", explode("Patient").alias("Patient")).select(col("id"), col("timestamp"), col("bundleUUID"), col("Patient.*"))

# COMMAND ----------

display(patient)
