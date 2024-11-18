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
sorted(difference_in_keys)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------


