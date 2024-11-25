# Databricks notebook source
import dlt

# COMMAND ----------

sourcePath = spark.conf.get('bundle.sourcePath')
source_catalog = spark.conf.get('workflow_inputs.source_catalog')
source_schema = spark.conf.get('workflow_inputs.source_schema')

# COMMAND ----------

resources_table = f"{source_catalog}.{source_schema}.resource_types"
resources_table

# COMMAND ----------

resources = spark.table(resources_table).select("resourceType").collect()
resources = sorted([row.resourceType for row in resources])
resources

# COMMAND ----------

from pyspark.sql.functions import explode, col, collect_set

resource_key_table = f"{source_catalog}.{source_schema}.resource_type_keys"
resource_keys = spark.table(resource_key_table).groupBy("resourceType").agg(collect_set("key").alias("keys"))
resource_keys_dict = {row['resourceType']: row['keys'] for row in resource_keys.collect()}
resource_keys_dict

# COMMAND ----------

import sys
import os
sys.path.append(os.path.abspath(f"{sourcePath}/fhirworks_dlt"))

import fhirWorksDLT

# COMMAND ----------

Pipeline = fhirWorksDLT.silverPipeline(
    spark = spark
)

# COMMAND ----------

for resource_type in resources:
  Pipeline.resource_stage_silver(
    parsed_variant_resource_table = f"{source_catalog}.{source_schema}.resources_parsed"
    ,resource_keys = resource_keys_dict[resource_type]
    ,resource = resource_type
    ,live = False
    ,temporary = False
  )

# COMMAND ----------

for resource_type in resources:
  Pipeline.stream_silver_apply_changes(
    source = f"{resource_type}_stage".lower()
    ,target = f"{resource_type}"
    ,keys = ["bundleUUID", f"{resource_type}_uuid"] # for now-- need to look for other ids with Redox
    ,sequence_by = "bundle_timestamp"
    ,stored_as_scd_type = 1
    ,comment = f"{resource_type} FHIR Resource Data"
    ,spark_conf = None
    ,table_properties = {
      "pipelines.autoOptimize.managed" : "true"
      ,"pipelines.reset.allowed" : "true"
      ,"delta.feature.variantType-preview" : "supported"
    }
    ,partition_cols = None
    ,cluster_by = ["bundleUUID", f"{resource_type}_uuid"]
    ,path = None
    ,schema = None
    ,expect_all = None
    ,expect_all_or_drop = {
      "valid_bundleUUID" : "bundleUUID IS NOT NULL"
      ,f"valid_{resource_type}_uuid" : f"{resource_type}_uuid IS NOT NULL"
    }
    ,expect_all_or_fail  = None
    ,row_filter  = None
    ,ignore_null_updates = False
    ,apply_as_deletes  = None
    ,apply_as_truncates = None
    ,column_list = None
    ,except_column_list = ["bundle_timestamp"]
    ,track_history_column_list = None
    ,track_history_except_column_list = None
  )
