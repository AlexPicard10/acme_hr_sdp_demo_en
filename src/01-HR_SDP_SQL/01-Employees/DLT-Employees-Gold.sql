-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 3 / Consolidation — Employees (Auto CDC, SCD Type 1)
-- MAGIC #### Deduplication / latest version: we only keep the current state per employee.
-- MAGIC `CREATE FLOW … AS AUTO CDC INTO … STORED AS SCD TYPE 1` applies the changes and keeps, for each
-- MAGIC `employee_id`, the **latest value**. This is the reference **current state**: one row per
-- MAGIC employee, always up to date, fed by the daily extracts of both subsidiaries.
-- MAGIC
-- MAGIC **Version ordering: `SEQUENCE BY STRUCT(extract_ts, updated_at)`.** `extract_ts` orders files;
-- MAGIC `updated_at` breaks ties between versions of the same employee **within** a file (intra-day
-- MAGIC corrections). Without `updated_at`, these duplicates would share the same sequence and the
-- MAGIC result would be arbitrary.
-- MAGIC
-- MAGIC > This is the **streaming equivalent** of `ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY
-- MAGIC > extract_ts DESC, updated_at DESC) = 1`, but **incremental**: each run only processes the new
-- MAGIC > rows, whatever the history size. The explicit window function is shown in
-- MAGIC > `silver_employees_last_extract` (file `DLT-Employees-Last-Extract`).

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold_employees_current
CLUSTER BY AUTO
COMMENT "Employees — current state: latest known version of each employee (SCD1)"
TBLPROPERTIES (
  'quality' = 'gold',
  'delta.enableChangeDataFeed' = 'true'
);

CREATE FLOW gold_employees_current_flow
AS AUTO CDC INTO
  gold_employees_current
FROM STREAM(silver_employees)
  KEYS (employee_id)
  SEQUENCE BY STRUCT(extract_ts, updated_at)
  STORED AS SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4 / History — Employees (Auto CDC, SCD Type 2)
-- MAGIC #### Keep the history of department, contract or job title changes over time.
-- MAGIC `STORED AS SCD TYPE 2` adds the `__START_AT` / `__END_AT` system columns. `TRACK HISTORY ON`
-- MAGIC restricts new history rows to mobility changes only (department / contract / job title) —
-- MAGIC useful for HR analytics (internal mobility, time in role). The current state is read with
-- MAGIC `WHERE __END_AT IS NULL`.
-- MAGIC
-- MAGIC > An intra-day correction (job title entered by mistake, then fixed) shows up as a **very short**
-- MAGIC > version in the history: SCD2 faithfully records everything the HRIS delivered.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold_employees_history
CLUSTER BY AUTO
COMMENT "Employee history (SCD2) — department / contract / job title mobility over time"
TBLPROPERTIES ('quality' = 'gold');

CREATE FLOW gold_employees_history_flow
AS AUTO CDC INTO
  gold_employees_history
FROM STREAM(silver_employees)
  KEYS (employee_id)
  SEQUENCE BY STRUCT(extract_ts, updated_at)
  STORED AS SCD TYPE 2
  TRACK HISTORY ON department_id, contract_type, job_title;