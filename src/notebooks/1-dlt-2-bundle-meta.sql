-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE bundle_meta_parsed
CLUSTER BY (
  bundleUUID
)
TBLPROPERTIES (
  "quality" = "bronze"
  ,"source" = "Redox"
  ,"pipelines.autoOptimize.managed" = "true"
  ,"pipelines.reset.allowed" = "true"
  ,"delta.feature.variantType-preview" = "supported"
  -- ,"delta.enableChangeDataFeed" = "true"
)
COMMENT "Exploded Paresed FHIR Bundle Meta Data to Prepare for Stage Silver."
AS SELECT
  bundleUUID
  ,fileMetadata 
  ,ingestDate
  ,ingestTime
  ,CAST(resource:id AS STRING) as bundle_id
  ,CAST(resource:timestamp AS TIMESTAMP) as bundle_timestamp
  ,meta_exploded.pos as pos
  ,meta_exploded.key as key
  ,meta_exploded.value as value
FROM
  STREAM(LIVE.fhir_bronze),
  lateral variant_explode(resource:Meta) as meta_exploded

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW meta_keys
TBLPROPERTIES (
  "quality" = "gold"
  ,"pipelines.autoOptimize.managed" = "true"
  ,"pipelines.reset.allowed" = "true"
  ,"delta.feature.variantType-preview" = "supported"
)
COMMENT 'Keys Used in the Bundle Meta Data'
AS SELECT
  key
  ,COUNT(distinct bundleUUID) AS raw_bundle_count
FROM
  LIVE.bundle_meta_parsed
GROUP BY
  key
ORDER BY 
  key
