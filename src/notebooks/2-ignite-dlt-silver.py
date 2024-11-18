# Databricks notebook source
# MAGIC %pip install git+https://github.com/databrickslabs/dbignite.git

# COMMAND ----------

# %restart_python

# COMMAND ----------

import dlt

# COMMAND ----------

sourcePath = spark.conf.get('bundle.sourcePath')
volume_path = spark.conf.get("workflow_inputs.volume_path")
source_catalog = spark.conf.get("workflow_inputs.source_catalog")
source_schema = spark.conf.get("workflow_inputs.source_schema")
source_folder_path_from_volume = spark.conf.get("workflow_inputs.source_folder_path_from_volume")
fhir_schemas = spark.conf.get("workflow.inputs.fhir_schemas")

# COMMAND ----------

# fhir_schemas = "AllergyIntolerance,Provenance,Organization,Encounter,Medication,Practitioner,Condition,RelatedPerson,DocumentReference,QuestionnaireResponse,CareTeam,Coverage,Observation,Goal,CarePlan,ServiceRequest,Questionnaire,MedicationDispense,Immunization,Patient,Specimen,Device,DiagnosticReport,Location,MedicationRequest,Procedure"

# COMMAND ----------

fhir_schemas = fhir_schemas.split(',')
fhir_schemas

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

# from dbignite.fhir_mapping_model import FhirSchemaModel
# fhir_schema = FhirSchemaModel()

# COMMAND ----------

for resource in fhir_schemas:
  Pipeline.fhir_entry(
    bronze_table=f"{source_catalog}.{source_schema}.fhir_bronze"
    ,fhir_resource = resource
    ,live = False
    ,temporary = False
  )

# COMMAND ----------

# for resource in fhir_schemas:
#   if resource not in ("Bundle"):
#     Pipeline.stage_silver(
#       bronze_table = "fhir_bronze"
#       ,fhir_resource = resource
#     )
