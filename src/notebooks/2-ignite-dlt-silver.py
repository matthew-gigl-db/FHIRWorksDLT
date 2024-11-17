# Databricks notebook source
# MAGIC %pip install git+https://github.com/databrickslabs/dbignite.git

# COMMAND ----------

# %restart_python

# COMMAND ----------

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

Pipeline.fhir_entry(
    bronze_table="fhir_bronze"
)