-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Exploration & validation — HR 360
-- MAGIC Ready-to-use queries for the demo / enablement, to run on a SQL Warehouse after the pipelines
-- MAGIC have run. The SQL pipeline tables live in `alp_demo_catalog.acme_hr_en_sql`, the Python
-- MAGIC pipeline tables in `alp_demo_catalog.acme_hr_en_python` (same names). Adapt the catalog /
-- MAGIC schema if needed.

-- COMMAND ----------

-- MAGIC %md ## Streaming tables (bronze / silver)

-- COMMAND ----------

SELECT count(*) AS bronze_rows FROM alp_demo_catalog.acme_hr_en_sql.bronze_employees;
SELECT * FROM alp_demo_catalog.acme_hr_en_sql.silver_employees LIMIT 20;

-- COMMAND ----------

-- MAGIC %md ## Concatenating the 2 subsidiaries (Append Flows)
-- MAGIC A single bronze table, fed by one flow per Volume: `source_entity` tells the origin.

-- COMMAND ----------

SELECT source_entity, count(*) AS row_count, count(DISTINCT _source_file) AS file_count,
       count(DISTINCT employee_id) AS employee_count
FROM alp_demo_catalog.acme_hr_en_sql.bronze_employees
GROUP BY source_entity ORDER BY source_entity;

SELECT source_entity, count(*) AS absence_count
FROM alp_demo_catalog.acme_hr_en_sql.bronze_absences
GROUP BY source_entity ORDER BY source_entity;

-- COMMAND ----------

-- MAGIC %md ## Deduplicated latest delivery (window functions)
-- MAGIC `silver_employees_last_extract`: one row per employee in the latest file of each subsidiary.

-- COMMAND ----------

-- Intra-file duplicates: how many employees had several versions in the latest file?
SELECT source_entity, versions_in_file, count(*) AS employee_count
FROM alp_demo_catalog.acme_hr_en_sql.silver_employees_last_extract
GROUP BY source_entity, versions_in_file
ORDER BY source_entity, versions_in_file;

-- What the intra-day corrections changed (LAG)
SELECT source_entity, employee_id, versions_in_file,
       previous_department_id, department_id, previous_job_title, job_title, updated_at
FROM alp_demo_catalog.acme_hr_en_sql.silver_employees_last_extract
WHERE versions_in_file > 1
ORDER BY employee_id
LIMIT 20;

-- COMMAND ----------

-- Before / after for one duplicated employee: all raw versions, then the row kept
WITH one_emp AS (
  SELECT employee_id, _source_file
  FROM alp_demo_catalog.acme_hr_en_sql.silver_employees_last_extract
  WHERE versions_in_file > 1
  ORDER BY employee_id
  LIMIT 1
),
all_versions AS (
  SELECT 'silver (all versions)' AS source, s.employee_id, s.updated_at, s.department_id, s.job_title
  FROM alp_demo_catalog.acme_hr_en_sql.silver_employees s
  JOIN one_emp o ON s.employee_id = o.employee_id AND s._source_file = o._source_file
),
kept_version AS (
  SELECT 'last_extract (rank 1)' AS source, l.employee_id, l.updated_at, l.department_id, l.job_title
  FROM alp_demo_catalog.acme_hr_en_sql.silver_employees_last_extract l
  JOIN one_emp o ON l.employee_id = o.employee_id
)
SELECT * FROM all_versions
UNION ALL
SELECT * FROM kept_version
ORDER BY source, updated_at;

-- COMMAND ----------

-- Consistency: for the employees of the latest file, Auto CDC (gold) and the window function
-- must keep the same version. Expected: 0 differences.
SELECT count(*) AS differences
FROM alp_demo_catalog.acme_hr_en_sql.silver_employees_last_extract l
JOIN alp_demo_catalog.acme_hr_en_sql.gold_employees_current g ON l.employee_id = g.employee_id
WHERE l.department_id <> g.department_id OR l.job_title <> g.job_title
   OR l.updated_at <> g.updated_at;

-- COMMAND ----------

-- MAGIC %md ## Gold — current state (SCD Type 1): one row per employee

-- COMMAND ----------

SELECT count(*) AS current_employees FROM alp_demo_catalog.acme_hr_en_sql.gold_employees_current;

SELECT age_bracket, count(*) AS headcount
FROM alp_demo_catalog.acme_hr_en_sql.gold_employees_current
WHERE status = 'Active'
GROUP BY age_bracket ORDER BY age_bracket;

-- COMMAND ----------

-- MAGIC %md ## Gold — history (SCD Type 2): mobility over time
-- MAGIC Current state = `__END_AT IS NULL`. Closed rows are the history.

-- COMMAND ----------

-- Employees with at least one tracked change
SELECT employee_id FROM alp_demo_catalog.acme_hr_en_sql.gold_employees_history
GROUP BY employee_id HAVING count(*) > 1;

-- COMMAND ----------

SELECT employee_id, department_id, contract_type, job_title, __START_AT, __END_AT
FROM alp_demo_catalog.acme_hr_en_sql.gold_employees_history
WHERE employee_id = 'EMPFR0000010'  -- replace with an employee_id returned by the query above
ORDER BY __START_AT;

-- COMMAND ----------

-- MAGIC %md ## SQL vs Python: same results?
-- MAGIC Both pipelines read the **same Volumes** and apply the same logic: their tables (same names,
-- MAGIC two schemas) must hold the same data. Silver/gold tables do not carry the ingestion timestamp
-- MAGIC (`_ingested_at`, specific to each run), so they can be compared as is. `EXCEPT` compares
-- MAGIC columns **by position**: both pipelines produce the columns in the same order.

-- COMMAND ----------

-- Side-by-side row counts
WITH sql_counts AS (
  SELECT 'bronze_employees' AS table_name, count(*) AS n FROM alp_demo_catalog.acme_hr_en_sql.bronze_employees
  UNION ALL SELECT 'bronze_absences',               count(*) FROM alp_demo_catalog.acme_hr_en_sql.bronze_absences
  UNION ALL SELECT 'silver_employees',              count(*) FROM alp_demo_catalog.acme_hr_en_sql.silver_employees
  UNION ALL SELECT 'silver_employees_last_extract', count(*) FROM alp_demo_catalog.acme_hr_en_sql.silver_employees_last_extract
  UNION ALL SELECT 'silver_absences',               count(*) FROM alp_demo_catalog.acme_hr_en_sql.silver_absences
  UNION ALL SELECT 'gold_employees_current',        count(*) FROM alp_demo_catalog.acme_hr_en_sql.gold_employees_current
  UNION ALL SELECT 'gold_employees_history',        count(*) FROM alp_demo_catalog.acme_hr_en_sql.gold_employees_history
  UNION ALL SELECT 'gold_employee_360',             count(*) FROM alp_demo_catalog.acme_hr_en_sql.gold_employee_360
),
python_counts AS (
  SELECT 'bronze_employees' AS table_name, count(*) AS n FROM alp_demo_catalog.acme_hr_en_python.bronze_employees
  UNION ALL SELECT 'bronze_absences',               count(*) FROM alp_demo_catalog.acme_hr_en_python.bronze_absences
  UNION ALL SELECT 'silver_employees',              count(*) FROM alp_demo_catalog.acme_hr_en_python.silver_employees
  UNION ALL SELECT 'silver_employees_last_extract', count(*) FROM alp_demo_catalog.acme_hr_en_python.silver_employees_last_extract
  UNION ALL SELECT 'silver_absences',               count(*) FROM alp_demo_catalog.acme_hr_en_python.silver_absences
  UNION ALL SELECT 'gold_employees_current',        count(*) FROM alp_demo_catalog.acme_hr_en_python.gold_employees_current
  UNION ALL SELECT 'gold_employees_history',        count(*) FROM alp_demo_catalog.acme_hr_en_python.gold_employees_history
  UNION ALL SELECT 'gold_employee_360',             count(*) FROM alp_demo_catalog.acme_hr_en_python.gold_employee_360
)
SELECT s.table_name, s.n AS rows_sql, p.n AS rows_python, s.n = p.n AS identical
FROM sql_counts s
JOIN python_counts p USING (table_name)
ORDER BY s.table_name;

-- COMMAND ----------

-- Identical content, both ways (expected: 0 rows for each query)
WITH sql_current AS (
  SELECT * FROM alp_demo_catalog.acme_hr_en_sql.gold_employees_current
),
python_current AS (
  SELECT * FROM alp_demo_catalog.acme_hr_en_python.gold_employees_current
),
sql_only AS (
  SELECT * FROM sql_current EXCEPT SELECT * FROM python_current
),
python_only AS (
  SELECT * FROM python_current EXCEPT SELECT * FROM sql_current
)
SELECT 'in sql, not in python' AS difference, count(*) AS n FROM sql_only
UNION ALL
SELECT 'in python, not in sql', count(*) FROM python_only;

WITH sql_last AS (
  SELECT * FROM alp_demo_catalog.acme_hr_en_sql.silver_employees_last_extract
),
python_last AS (
  SELECT * FROM alp_demo_catalog.acme_hr_en_python.silver_employees_last_extract
),
sql_only AS (
  SELECT * FROM sql_last EXCEPT SELECT * FROM python_last
),
python_only AS (
  SELECT * FROM python_last EXCEPT SELECT * FROM sql_last
)
SELECT 'in sql, not in python' AS difference, count(*) AS n FROM sql_only
UNION ALL
SELECT 'in python, not in sql', count(*) FROM python_only;

-- COMMAND ----------

-- MAGIC %md ## Automatic liquid clustering (`CLUSTER BY AUTO`)

-- COMMAND ----------

DESCRIBE DETAIL alp_demo_catalog.acme_hr_en_sql.silver_absences;

-- COMMAND ----------

-- MAGIC %md ## Materialized View — 360° employee view (consumption)

-- COMMAND ----------

SELECT source_entity, employee_id, last_name, department_name, business_unit, age_bracket,
       seniority_years, absence_days_ytd
FROM alp_demo_catalog.acme_hr_en_sql.gold_employee_360
ORDER BY absence_days_ytd DESC
LIMIT 20;
