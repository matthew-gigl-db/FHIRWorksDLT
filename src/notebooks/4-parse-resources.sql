-- Databricks notebook source
DECLARE OR REPLACE VARIABLE catalog_use STRING DEFAULT 'redox';
DECLARE OR REPLACE VARIABLE schema_use STRING DEFAULT 'main';
DECLARE OR REPLACE VARIABLE resource_type STRING DEFAULT 'Patient';

-- COMMAND ----------

SET VARIABLE catalog_use = :`bundle.catalog`;
SET VARIABLE schema_use = :`bundle.schema`;
SET VARIABLE resource_type = :resource_type; 

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE resource_lower STRING DEFAULT lower(resource_type);

-- COMMAND ----------

SELECT catalog_use, schema_use, resource_type, resource_lower;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE sql_stmnt STRING;

SET VARIABLE sql_stmnt = "
  CREATE OR REFRESH STREAMING TABLE " || catalog_use || "." || schema_use || "." || resource_lower || "_entry 
  TBLPROPERTIES (
    'quality' = 'bronze'
    ,'source' = 'Redox'
    ,'pipelines.channel' = 'preview'
    ,'pipelines.autoOptimize.managed' = 'true'
    ,'pipelines.reset.allowed' = 'true'
    ,'delta.feature.variantType-preview' = 'supported'
  )
  COMMENT '" ||   resource_type || " resource type entry parsing from streaming FHIR bundles data.'
  AS SELECT
    bundle_id
    ,bundle_timestamp
    ,bundleUUID
    ,fullUrl as " || resource_lower || "_uuid
    ,resource_exploded.key as key
    ,resource_exploded.value as value
  FROM 
    STREAM(" || catalog_use || "." || schema_use || ".fhir_bronze_parsed)
    ,lateral variant_explode(resource) as resource_exploded
  WHERE
    resourceType = '" || resource_type || "'
  ;
  "

-- COMMAND ----------

SELECT sql_stmnt;

-- COMMAND ----------

EXECUTE IMMEDIATE sql_stmnt;
