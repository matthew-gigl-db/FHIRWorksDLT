# Databricks notebook source
import dlt

# COMMAND ----------

sourcePath = spark.conf.get('bundle.sourcePath')
volume_path = spark.conf.get("workflow_inputs.volume_path")
source_folder_path_from_volume = spark.conf.get("workflow_inputs.source_folder_path_from_volume")

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(f"{sourcePath}/fhirworks_dlt"))

import fhirWorksDLT

# COMMAND ----------

Pipeline = fhirWorksDLT.fhirIngestionDLT(
    spark = spark
    ,volume = volume_path
)

# COMMAND ----------

Pipeline.ingest_raw_to_bronze(
    table_name="fhir_bronze"
    ,table_comment=f"A full text record of every FHIR JSON file recieved from Redox and located in {volume_path}."
    ,table_properties={"quality":"bronze", "source":"Redox", "delta.feature.variantType-preview":"supported"}
    ,source_folder_path_from_volume=source_folder_path_from_volume
    ,maxFiles = 1000
    ,maxBytes = "10g"
)
