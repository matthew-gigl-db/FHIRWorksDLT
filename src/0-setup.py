# Databricks notebook source
# DBTITLE 1,Install or Upgrade the Databricks Python SDK
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Set Up Notebook Input Parameters
dbutils.widgets.text("bundle.catalog", "redox", "Catalog")
dbutils.widgets.text("bundle.schema", "main", "Schema")
dbutils.widgets.text("bundle.external_location", "", "External Location")

# COMMAND ----------

# DBTITLE 1,Retrieve Notebook Input Parameters
catalog_use = dbutils.widgets.get("bundle.catalog")
schema_use = dbutils.widgets.get("bundle.schema")
external_location_use = dbutils.widgets.get("bundle.external_location")

# COMMAND ----------

# DBTITLE 1,Initialize the Workspace Client
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,List the Available Catalogs in Unity Catalog
catalogs = w.catalogs.list()
catalogs = [catalog.as_dict() for catalog in catalogs]

# COMMAND ----------

# DBTITLE 1,Create Catalog If Not Exists
if any(catalog['name'] == catalog_use for catalog in catalogs):
  print(f"Catalog '{catalog_use}' already exists")
else:
  w.catalogs.create(name=catalog_use)

# COMMAND ----------

# DBTITLE 1,List Schemas in the Catalog to Use
schemas = w.schemas.list(catalog_name=catalog_use)
schemas = [schema.as_dict() for schema in schemas]

# COMMAND ----------

# DBTITLE 1,Print Schemas Array
schemas

# COMMAND ----------

# DBTITLE 1,Create Schema to Use if Not Exists
if any(schema['name'] == schema_use for schema in schemas):
  print(f"Schema '{catalog_use}.{schema_use}' already exists")
else:
  w.schemas.create(
    name=schema_use
    ,catalog_name=catalog_use
  )

# COMMAND ----------

# DBTITLE 1,List Availabe External Locations in Unity Catalog
external_locations = w.external_locations.list()
external_locations = [e.as_dict() for e in external_locations]

# COMMAND ----------

# DBTITLE 1,List Volumes in the Catalog.Schema
volumes = w.volumes.list(
  catalog_name=catalog_use
  ,schema_name=schema_use
)
volumes = [v.as_dict() for v in volumes]

# COMMAND ----------

# DBTITLE 1,Create Volume if Not Exists, Raise Exception if External Location is Not Set
if any(v['name'] == "landing" for v in volumes):
  print(f'Volume {catalog_use}.{schema_use}.landing already exists')
else:
  if any(e['name'] == external_location_use for e in external_locations): 
    ext_local_url = w.external_locations.get(name=external_location_use).url
    w.volumes.create(
      name="landing"
      ,catalog_name=catalog_use
      ,schema_name=schema_use
      ,volume_type=catalog.VolumeType.EXTERNAL
      ,storage_location=ext_local_url
    )
  else: 
    raise Exception(f'External location {external_location_use} does not exist. Please speak to your Databricks admin or review the documentation for creating external locations in Unity Catalog: https://docs.databricks.com/en/connect/unity-catalog/external-locations.html')

# COMMAND ----------

# DBTITLE 1,List Volume Contents
dbutils.fs.ls(f"/Volumes/{catalog_use}/{schema_use}/landing")
