-- Databricks notebook source
DECLARE OR REPLACE VARIABLE catalog_use STRING DEFAULT 'redox';
DECLARE OR REPLACE VARIABLE schema_use STRING DEFAULT 'main';
DECLARE OR REPLACE VARIABLE resource_type STRING DEFAULT 'Patient';

-- COMMAND ----------

SET VARIABLE catalog_use = :`bundle.catalog`;
SET VARIABLE schema_use = :`bundle.schema`;
SET VARIABLE resource_type = :resource_type; 

-- COMMAND ----------

SELECT catalog_use, schema_use, resource_type;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE strm_sql_stmnt STRING;

SET VARIABLE strm_sql_stmnt = "
  CREATE OR REFRESH STREAMING TABLE " || catalog_use || "." || schema_use || "." || resource_type || " 
  CLUSTER BY (
  bundle_id
  )
  TBLPROPERTIES (
    'quality' = 'bronze'
    ,'source' = 'Redox'
    ,'pipelines.autoOptimize.managed' = 'true'
    ,'pipelines.reset.allowed' = 'true'
    ,'delta.feature.variantType-preview' = 'supported'
  )
  COMMENT '" ||   resource_type || " resource entry parsing from streaming FHIR bundles data.'
  AS SELECT
    
  "
