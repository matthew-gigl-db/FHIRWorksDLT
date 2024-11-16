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

from pyspark.sql.functions import col

# COMMAND ----------

display(df.filter(col("resource.id") == "57c09138-aa92-4665-9fc7-3b5611b81f58"))

# COMMAND ----------

bundle = FhirResource.from_raw_bundle_resource(df)

# COMMAND ----------

type(bundle)

# COMMAND ----------

from dbignite.fhir_mapping_model import FhirSchemaModel

# COMMAND ----------

fhir_resource = "Patient"
fhir_custom = FhirSchemaModel().custom_fhir_resource_mapping([fhir_resource])

# COMMAND ----------

bundle.entry(fhir_custom)

# COMMAND ----------

from dbignite.fhir_mapping_model import FhirSchemaModel

# COMMAND ----------

FhirSchemaModel().list_keys()

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
