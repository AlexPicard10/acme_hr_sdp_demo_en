-- Databricks notebook source
-- MAGIC %md
-- MAGIC # HR 360 — Lakeflow Declarative Pipeline (LDP)
-- MAGIC ### Enablement asset — upskilling HR Data teams on Databricks SDP (fictional company ACME)
-- MAGIC
-- MAGIC This pipeline builds the **360° employee view**: the HRIS extracts of two subsidiaries
-- MAGIC (ACME France and ACME Belgium) each land in **their own Unity Catalog Volume**
-- MAGIC (`landing_fr`, `landing_be`), are ingested with **Auto Loader**, concatenated, then
-- MAGIC consolidated into a single employee table.
-- MAGIC
-- MAGIC | Step | Lakeflow (SDP) feature |
-- MAGIC |---|---|
-- MAGIC | Extract ingestion (UC Volume) | **Streaming Table** + **Auto Loader** (`STREAM read_files`) — *bronze* layer |
-- MAGIC | Concatenating the 2 subsidiaries | **Append Flows** (`CREATE FLOW … INSERT INTO`) — *bronze* layer |
-- MAGIC | Cleaning, typing, derived columns | **Streaming Table** + **Expectations** — *silver* layer |
-- MAGIC | Reference data join | SQL join on master data |
-- MAGIC | Latest row per employee in the latest file | **Window function** `ROW_NUMBER` in a **Materialized View** |
-- MAGIC | Deduplication / history | **Auto CDC** (SCD1/SCD2) — *gold* layer |
-- MAGIC | Output / consumption | **Materialized View** (Unity Catalog) |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1 / Ingesting the Employee extracts of both subsidiaries (Auto Loader + Append Flows)
-- MAGIC #### `STREAM read_files(...)` automatically detects and ingests new JSON files dropped into a Volume.
-- MAGIC Auto Loader handles schema inference, schema evolution (`rescue` mode) and incremental file
-- MAGIC tracking: the first run loads the history, subsequent runs only ingest the new files
-- MAGIC (fast and cheap incremental processing).
-- MAGIC
-- MAGIC **Concatenating two sources with the same structure: Append Flows.** ACME France and ACME Belgium
-- MAGIC deliver the same format to two different storage locations. We declare **one** target table
-- MAGIC (`CREATE OR REFRESH STREAMING TABLE` without a query), then **one flow per source**
-- MAGIC (`CREATE FLOW … AS INSERT INTO … BY NAME`). Each flow has its own file tracking, and the
-- MAGIC `source_entity` column records the originating subsidiary.
-- MAGIC
-- MAGIC | | Append Flows (this pipeline) | `UNION ALL` of two streams |
-- MAGIC |---|---|---|
-- MAGIC | Adding a 3rd subsidiary | one more `CREATE FLOW`, **no** full refresh | the query changes: full refresh required |
-- MAGIC | File tracking | one checkpoint per source, independent | a single checkpoint for the whole query |
-- MAGIC | Readability | one source = one block | an ever-growing query |
-- MAGIC
-- MAGIC `BY NAME` aligns columns **by name** (not by position): a different column order between the
-- MAGIC two HRIS is not a problem.
-- MAGIC
-- MAGIC `CLUSTER BY AUTO` enables **automatic liquid clustering**: Databricks picks (and evolves) the
-- MAGIC clustering keys based on the queries actually run, via Predictive Optimization. No need to
-- MAGIC guess partitioning columns. Every table in the pipeline uses it.

-- COMMAND ----------

-- Single target table: its schema is inferred from the flows that feed it.
CREATE OR REFRESH STREAMING TABLE bronze_employees
CLUSTER BY AUTO
COMMENT "Raw Employee extracts of the 2 subsidiaries (FR, BE), concatenated by Append Flows from 2 UC Volumes — bronze layer"
TBLPROPERTIES ('quality' = 'bronze');

-- COMMAND ----------

-- Flow 1: ACME France (landing_fr Volume)
CREATE FLOW bronze_employees_fr
AS INSERT INTO bronze_employees BY NAME
SELECT
  *,
  'FR'                             AS source_entity,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _source_file_ts,
  current_timestamp()              AS _ingested_at
FROM STREAM read_files(
  '${employees_path_fr}',
  format => 'json',
  inferColumnTypes => true,
  schemaEvolutionMode => 'rescue'
);

-- COMMAND ----------

-- Flow 2: ACME Belgium (landing_be Volume) — same structure, different storage
CREATE FLOW bronze_employees_be
AS INSERT INTO bronze_employees BY NAME
SELECT
  *,
  'BE'                             AS source_entity,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _source_file_ts,
  current_timestamp()              AS _ingested_at
FROM STREAM read_files(
  '${employees_path_be}',
  format => 'json',
  inferColumnTypes => true,
  schemaEvolutionMode => 'rescue'
);
