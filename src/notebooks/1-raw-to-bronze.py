# Databricks notebook source
import dlt

# COMMAND ----------

# sourcePath = spark.conf.get('bundle.sourcePath')
sourcePath = "/Workspace/Users/matthew.giglia@databricks.com/FHIRWorksDLT/src"

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(f"{sourcePath}/fhirworks_dlt"))

import fhirWorksDLT
