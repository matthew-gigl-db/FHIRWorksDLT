-- Databricks notebook source
USE IDENTIFIER(:catalog || "." || :schema);
SELECT current_catalog(), current_schema();

-- COMMAND ----------

SHOW VOLUMES;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE volume_path STRING;

SET VAR volume_path = "/Volumes/" || current_catalog() || "/" || current_schema() || "/synthetic_files_raw/output/fhir/";

-- COMMAND ----------

select volume_path;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE list_stmnt STRING;

SET VARIABLE list_stmnt = "LIST '" || volume_path || "'";

SELECT list_stmnt;

-- COMMAND ----------

EXECUTE IMMEDIATE list_stmnt;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW list_files AS
SEE
;
