-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 1b / Ingesting the Absence events of both subsidiaries (Auto Loader + Append Flows)
-- MAGIC #### Second source: a stream of absence events (sick leave, paid leave, training…).
-- MAGIC Same pattern as employees: each subsidiary drops its files into its Volume, a single target
-- MAGIC table, **one Append Flow per subsidiary**. New files dropped over time are automatically
-- MAGIC picked up on the next run.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_absences
CLUSTER BY AUTO
COMMENT "Raw absence events of the 2 subsidiaries (FR, BE), concatenated by Append Flows from 2 UC Volumes — bronze layer"
TBLPROPERTIES ('quality' = 'bronze');

-- COMMAND ----------

-- Flow 1: ACME France (landing_fr Volume)
CREATE FLOW bronze_absences_fr
AS INSERT INTO bronze_absences BY NAME
SELECT
  *,
  'FR'                             AS source_entity,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _source_file_ts,
  current_timestamp()              AS _ingested_at
FROM STREAM read_files(
  '${absences_path_fr}',
  format => 'json',
  inferColumnTypes => true,
  schemaEvolutionMode => 'rescue'
);

-- COMMAND ----------

-- Flow 2: ACME Belgium (landing_be Volume)
CREATE FLOW bronze_absences_be
AS INSERT INTO bronze_absences BY NAME
SELECT
  *,
  'BE'                             AS source_entity,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _source_file_ts,
  current_timestamp()              AS _ingested_at
FROM STREAM read_files(
  '${absences_path_be}',
  format => 'json',
  inferColumnTypes => true,
  schemaEvolutionMode => 'rescue'
);
