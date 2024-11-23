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

spark.table("redox.main.fhir_bronze").printSchema()

# COMMAND ----------

bronze_df = spark.table("redox.main.fhir_bronze")

# COMMAND ----------

display(bronze_df.head(1))
