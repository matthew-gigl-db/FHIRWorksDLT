# Databricks notebook source
import dlt

# COMMAND ----------

sourcePath = spark.conf.get('bundle.sourcePath')
# volume_path = spark.conf.get("workflow_inputs.volume_path")
# source_folder_path_from_volume = spark.conf.get("workflow_inputs.source_folder_path_from_volume")
source_catalog = spark.conf.get('workflow_inputs.source_catalog')
source_schema = spark.conf.get('workflow_inputs.source_schema')

# COMMAND ----------

import sys
import os
sys.path.append(os.path.abspath(f"{sourcePath}/fhirworks_dlt"))

import fhirWorksDLT

# COMMAND ----------

Pipeline = fhirWorksDLT.silverPipeline(
    # spark = spark
    # ,volume = volume_path
)

# COMMAND ----------

Pipeline.meta_stage_silver(
  parsed_variant_meta_table = f"{source_catalog}.{source_schema}.bundle_meta_parsed"
  ,live = False
  ,temporary = False
)
