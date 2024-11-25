# Databricks notebook source
import dlt

# COMMAND ----------

sourcePath = spark.conf.get('bundle.sourcePath')
source_catalog = spark.conf.get('workflow_inputs.source_catalog')
source_schema = spark.conf.get('workflow_inputs.source_schema')

# COMMAND ----------

meta_key_table = f"{source_catalog}.{source_schema}.meta_keys"

# COMMAND ----------

distinct_keys = spark.table(meta_key_table).select("key").distinct().collect()
distinct_keys = sorted([row.key for row in distinct_keys])

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

Pipeline.meta_stage_silver(
  parsed_variant_meta_table = f"{source_catalog}.{source_schema}.bundle_meta_parsed"
  ,meta_keys = distinct_keys
  ,live = False
  ,temporary = False
)

# COMMAND ----------

Pipeline.stream_silver_apply_changes(
  source = "bundle_meta_stage"
  ,target = "bundle_meta"
  ,keys = ["bundleUUID"] # for now-- need to look for other ids with Redox
  ,sequence_by = "bundle_timestamp"
  ,stored_as_scd_type = 1
  ,comment = "The bundle Meta Table contains the header information for each FHIR Bundle including the type of event, the event date and time, and other information about the FHIR bundle's transmission."
  ,spark_conf = None
  ,table_properties = {
    "pipelines.autoOptimize.managed" : "true"
    ,"pipelines.reset.allowed" : "true"
    ,"delta.feature.variantType-preview" : "supported"
  }
  ,partition_cols = None
  ,cluster_by = ["bundleUUID"]
  ,path = None
  ,schema = None
  ,expect_all = None
  ,expect_all_or_drop = {"valid_bundleUUID" : "bundleUUID IS NOT NULL"}
  ,expect_all_or_fail  = None
  ,row_filter  = None
  ,ignore_null_updates = False
  ,apply_as_deletes  = None
  ,apply_as_truncates = None
  ,column_list = None
  ,except_column_list = None
  ,track_history_column_list = None
  ,track_history_except_column_list = None
)
