# Databricks notebook source
import dlt

# COMMAND ----------

sourcePath = spark.conf.get('bundle.sourcePath')
volume_path = spark.conf.get("workflow_inputs.volume_path")
source_folder_path_from_volume = spark.conf.get("workflow_inputs.source_folder_path_from_volume")

# COMMAND ----------

import sys
import os
sys.path.append(os.path.abspath(f"{sourcePath}/fhirworks_dlt"))

import fhirWorksDLT

# COMMAND ----------

Pipeline = fhirWorksDLT.ignitePipeline(
    spark = spark
    ,volume = volume_path
)

# COMMAND ----------

Pipeline.meta_stage_silver(
  parsed_variant_meta_table: "bundle_meta_parsed"
  ,live: bool = True
  ,temporary: bool = False
)
