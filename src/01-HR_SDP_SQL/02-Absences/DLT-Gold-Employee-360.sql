-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 6 / 360° employee view — consumption Materialized View (Unity Catalog)
-- MAGIC #### A **Materialized View**: the result is stored and refreshed on every pipeline update.
-- MAGIC This is the wide "360° view" table exposed to consumers (BI, apps, analysts): the current
-- MAGIC state of each employee enriched with their year-to-date absence KPIs. We materialize it because
-- MAGIC it is read often and costs a join + an aggregation: readers get a precomputed (and
-- MAGIC automatically clustered) result instead of rerunning the query on every access.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_employee_360
CLUSTER BY AUTO
COMMENT "360° employee view — consolidation (current state) + year-to-date absence KPIs"
TBLPROPERTIES ('quality' = 'gold')
AS
-- Step 1 (CTE): year-to-date absence KPIs per employee
WITH absences_ytd AS (
  SELECT
    employee_id,
    SUM(days)  AS absence_days_ytd,
    COUNT(*)   AS absence_events_ytd
  FROM silver_absences
  WHERE YEAR(start_date) = YEAR(current_date())
  GROUP BY employee_id
)
-- Step 2: current state of each active employee, enriched with the KPIs
SELECT
  e.*,
  COALESCE(ab.absence_days_ytd, 0)   AS absence_days_ytd,
  COALESCE(ab.absence_events_ytd, 0) AS absence_events_ytd
FROM gold_employees_current e
LEFT JOIN absences_ytd ab
  ON e.employee_id = ab.employee_id
WHERE e.status = 'Active';
