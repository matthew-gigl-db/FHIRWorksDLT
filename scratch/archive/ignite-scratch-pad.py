# Databricks notebook source
# MAGIC %pip install git+https://github.com/databrickslabs/dbignite.git

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import dbignite

# COMMAND ----------

help(dbignite)

# COMMAND ----------

from pyspark.sql.functions import *
from dbignite.readers import read_from_directory

# COMMAND ----------

#Read sample FHIR Bundle data from this repo
redox_data = "/Volumes/redox/main/landing/Files/default/"
bundle = read_from_directory(redox_data)

#Read all the bundles and parse
df = bundle.entry()

# COMMAND ----------

display(df, limit = 1)

# COMMAND ----------

display(df.select(explode("Patient").alias("Patient"), col("bundleUUID"), col("Claim")).select(col("Patient"), col("bundleUUID"), explode("Claim").alias("Claim")).select(
  col("bundleUUID").alias("UNIQUE_FHIR_ID")))

# COMMAND ----------

from dbignite.fhir_resource import FhirResource

# COMMAND ----------

df = spark.table("redox.main.fhir_bronze")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

bundle = FhirResource.from_raw_bundle_resource(df)

# COMMAND ----------

type(bundle)

# COMMAND ----------

bundle.entry()

# COMMAND ----------



# COMMAND ----------

from dbignite.fhir_mapping_model import FhirSchemaModel

# COMMAND ----------

fhir_resource = "Patient"
fhir_custom = FhirSchemaModel().custom_fhir_resource_mapping([fhir_resource])

# COMMAND ----------

bundle.entry(fhir_custom)

# COMMAND ----------

from dbignite.fhir_mapping_model import FhirSchemaModel
fhir_schema = FhirSchemaModel()

# COMMAND ----------

sorted(fhir_schema.list_keys())

# COMMAND ----------

sdf = bundle.entry(fhir_custom)

# COMMAND ----------

display(sdf)

# COMMAND ----------

type(sdf)

# COMMAND ----------

from pyspark.sql.functions import explode, col

patient = (
  sdf
  .withColumn(fhir_resource, explode(fhir_resource).alias(fhir_resource))
  .withColumn("bundle_id", col("id"))
  .select(col("bundle_id"), col("timestamp"), col("bundleUUID"), col("Patient.*"))
)

# COMMAND ----------

display(patient)
