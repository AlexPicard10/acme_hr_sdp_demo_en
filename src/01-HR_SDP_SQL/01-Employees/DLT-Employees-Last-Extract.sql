-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 2b / Checking the latest delivery — Window functions (Materialized View)
-- MAGIC #### For the latest file delivered by each subsidiary: one row per employee, the most recent one.
-- MAGIC An extract can contain **several rows for the same `employee_id`**: the HRIS exports every
-- MAGIC version entered during the day (e.g. a job title entered by mistake at 7am, corrected at 9am),
-- MAGIC timestamped by `updated_at`. We want to see the **latest delivery**, deduplicated.
-- MAGIC
-- MAGIC **The window function, step by step**
-- MAGIC
-- MAGIC ```sql
-- MAGIC ROW_NUMBER() OVER (PARTITION BY source_entity, employee_id ORDER BY updated_at DESC)
-- MAGIC ```
-- MAGIC 1. `PARTITION BY`: groups the rows of the same employee (within a subsidiary);
-- MAGIC 2. `ORDER BY updated_at DESC`: sorts their versions from most recent to oldest;
-- MAGIC 3. `ROW_NUMBER()`: numbers 1, 2, 3… within each group; we keep **rank 1**.
-- MAGIC
-- MAGIC Equivalent shortcut: `QUALIFY ROW_NUMBER() OVER (…) = 1` right after the `FROM`.
-- MAGIC Other window functions enrich the check: `COUNT(*) OVER` (number of versions in the file) and
-- MAGIC `LAG()` (value of the previous version: what the correction changed).
-- MAGIC
-- MAGIC **Why a Materialized View?** Window functions such as `ROW_NUMBER` / `LAG` are **not supported
-- MAGIC on a streaming table** (a stream has no "end" to sort on). So we write them in a
-- MAGIC **Materialized View**, which reads `silver_employees` in batch.
-- MAGIC
-- MAGIC **Auto CDC or window function: when to use which**
-- MAGIC
-- MAGIC | | Auto CDC (`gold_employees_current`) | Window function (this MV) |
-- MAGIC |---|---|---|
-- MAGIC | Result | latest **known** version of each employee, across all files | whatever the query says: here, the **latest file** only |
-- MAGIC | Cost per run | only processes **new rows** (incremental) | rereads the source table (recompute, incremental only in some cases) |
-- MAGIC | Flexibility | one logic: latest version (SCD1) or history (SCD2) | any logic: rank, top N, previous version (`LAG`), filters… |
-- MAGIC | Table type | streaming table | Materialized View (batch) |
-- MAGIC | Use it for | the production "current state" chain | checks, analysis, rules richer than a plain "latest" |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_employees_last_extract
CLUSTER BY AUTO
COMMENT "Latest file delivered by each subsidiary, one row per employee (window functions ROW_NUMBER, COUNT, LAG)"
TBLPROPERTIES ('quality' = 'silver')
AS
WITH last_file AS (
  -- The most recent file of each subsidiary (max_by: the _source_file value at the maximum extract_ts)
  SELECT
    source_entity,
    max_by(_source_file, extract_ts) AS last_source_file
  FROM silver_employees
  GROUP BY source_entity
),
ranked AS (
  SELECT
    e.*,
    -- How many versions of this employee in the file?
    COUNT(*) OVER (PARTITION BY e.source_entity, e.employee_id) AS versions_in_file,
    -- Values of the previous version (NULL if there is only one): what the correction changed
    LAG(e.department_id) OVER (PARTITION BY e.source_entity, e.employee_id ORDER BY e.updated_at) AS previous_department_id,
    LAG(e.job_title)     OVER (PARTITION BY e.source_entity, e.employee_id ORDER BY e.updated_at) AS previous_job_title,
    -- Rank 1 = the most recent version
    ROW_NUMBER()         OVER (PARTITION BY e.source_entity, e.employee_id ORDER BY e.updated_at DESC) AS rn
  FROM silver_employees e
  INNER JOIN last_file l
    ON  e.source_entity = l.source_entity
    AND e._source_file  = l.last_source_file
)
SELECT * EXCEPT (rn)
FROM ranked
WHERE rn = 1;
