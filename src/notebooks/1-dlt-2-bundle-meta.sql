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
)
COMMENT "Exploded Paresed FHIR Bundle Meta Data to Prepare for Stage Silver."
AS SELECT DISTINCT
  bundleUUID
  ,fileMetadata
  ,ingestDate
  ,ingestTime
  ,bundle_id
  ,meta_exploded.key as key
  ,meta_exploded.value as value
FROM
  STREAM(LIVE.fhir_bronze_parsed),
  lateral variant_explode(meta) as meta_exploded
