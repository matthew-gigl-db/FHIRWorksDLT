-- Databricks notebook source
CREATE OR REFRESH MATERIALIZED VIEW redox.main.bundle_meta_stage
CLUSTER BY (
  bundleUUID
)
TBLPROPERTIES (
  "quality" = "silver"
  ,"source" = "Redox"
  ,"pipelines.autoOptimize.managed" = "true"
  ,"pipelines.reset.allowed" = "true"
  ,"delta.feature.variantType-preview" = "supported"
)
COMMENT "Exploded Paresed FHIR Bundle Meta Data to Prepare for Stage Silver."
AS SELECT 
