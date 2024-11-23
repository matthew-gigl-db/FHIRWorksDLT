-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE fhir_bronze_parsed
TBLPROPERTIES (
  "quality" = "bronze"
  ,"source" = "Redox"
  ,"pipelines.autoOptimize.managed" = "true"
  ,"pipelines.reset.allowed" = "true"
  ,"delta.feature.variantType-preview" = "supported"
)
COMMENT "Parsed streaming FHIR bundle data ingested from bronze."
AS SELECT
  fileMetadata 
  ,ingestDate
  ,ingestTime
  ,resource:id as bundle_id
  ,resource:timestamp as bundle_timestamp
  ,resource:Meta as meta
  ,entry.value as entry
  ,entry.value:fullUrl as fullUrl
  ,entry.value:resource.resourceType as resourceType
  ,entry.value:resource as resource
FROM
  STREAM(LIVE.fhir_bronze),
  lateral variant_explode(resource:entry) as entry

-- COMMAND ----------

CREATE MATERIALIZED VIEW resource_types
TBLPROPERTIES (
  "quality" = "silver"
  ,"pipelines.autoOptimize.managed" = "true"
  ,"pipelines.reset.allowed" = "true"
  ,"delta.feature.variantType-preview" = "supported"
)
COMMENT 'Current Resource Types Ingested from FHIR Bundles in Bronze'
AS SELECT
  resourceType
  ,COUNT(*) AS count
FROM
  STREAM(LIVE.fhir_bronze_parsed)
GROUP BY
  resourceType
