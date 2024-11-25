# Databricks notebook source
dbutils.widgets.text("bundle.catalog", "redox", "Catalog")
dbutils.widgets.text("bundle.schema", "main", "Schema")

# COMMAND ----------

catalog_use = dbutils.widgets.get("bundle.catalog")
schema_use = dbutils.widgets.get("bundle.schema")

# COMMAND ----------

resource_types = [row.resourceType for row in spark.sql(f"select distinct resourceType from {catalog_use}.{schema_use}.resource_types").collect()]
resource_types

# COMMAND ----------

dbutils.jobs.taskValues.set("resource_types", resource_types)
