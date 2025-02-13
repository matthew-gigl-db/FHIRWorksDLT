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

all_schemas = FhirSchemaModel()

# COMMAND ----------

us_core = FhirSchemaModel.us_core_fhir_resource_mapping()

# COMMAND ----------

all_schemas_keys = set(all_schemas.list_keys())
us_core_keys = set(us_core.list_keys())

difference_in_keys = all_schemas_keys.symmetric_difference(us_core_keys)
difference_in_keys = list(difference_in_keys)
difference_in_keys = [key for key in difference_in_keys if key != 'Bundle']
sorted(difference_in_keys)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

BUNDLE_SCHEMA = (
    StructType()
      .add("resourceType", StringType())
      .add("entry", ArrayType(
          StructType().add("resource", StringType())
          ,StructType().add("fullUrl", StringType())
      ))
      .add("id", StringType())
      .add("timestamp", StringType())
)

# COMMAND ----------

BUNDLE_SCHEMA

# COMMAND ----------

schema = StructType([
    StructField("arrayField", ArrayType(
        StructType([
            StructField("field1", StringType(), True),
            StructField("field2", StringType(), True)
        ])
    ), True)
])

# COMMAND ----------

schema2 = (
  StructType()
  .add("arrayField", ArrayType(
    StructType()
    .add("field1", StringType())
    .add("field2", StringType())
  ))
)

schema == schema2
