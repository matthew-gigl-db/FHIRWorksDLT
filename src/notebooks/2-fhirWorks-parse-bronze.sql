-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE fhir_bronze_parsed
CLUSTER BY (
  bundle_id
)
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
  ,bundleUUID
  ,CAST(resource:id AS STRING) as bundle_id
  ,CAST(resource:timestamp AS TIMESTAMP) as bundle_timestamp
  ,resource:Meta as meta
  ,CAST(entry.value:fullUrl AS STRING) as fullUrl
  ,CAST(entry.value:resource.resourceType AS STRING) as resourceType
  -- ,entry.value:resource as resource
  ,resource_data.key as key
  ,resource_data.value as value
FROM
  STREAM(LIVE.fhir_bronze),
  lateral variant_explode(resource:entry) as entry,
  lateral variant_explode(entry.value:resource) as resource_data 

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW resource_types
TBLPROPERTIES (
  "quality" = "silver"
  ,"pipelines.autoOptimize.managed" = "true"
  ,"pipelines.reset.allowed" = "true"
  ,"delta.feature.variantType-preview" = "supported"
)
COMMENT 'Current Resource Types Ingested from FHIR Bundles in Bronze'
AS SELECT
  resourceType
  ,COUNT(distinct bundleUUID) AS bundle_count
FROM
  LIVE.fhir_bronze_parsed
GROUP BY
  resourceType
ORDER BY 
  count DESC
