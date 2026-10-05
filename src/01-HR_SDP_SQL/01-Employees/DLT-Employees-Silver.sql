-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 2 / Preparation & Quality — Employees (Streaming Table + Expectations)
-- MAGIC #### Preparation & quality: typing, derived columns, data quality checks (Expectations).
-- MAGIC **Expectations** (`CONSTRAINT ... EXPECT`) declare quality rules directly in the pipeline:
-- MAGIC non-compliant rows dropped (`DROP ROW`), blocking failure on a missing key (`FAIL UPDATE`),
-- MAGIC or a simple warning. We also build business **derived columns** (e.g. `age_bracket`,
-- MAGIC `seniority_years`) and **join the departments reference data** (master data).
-- MAGIC
-- MAGIC The query is split into **CTEs** (`WITH typed AS (…)`) rather than subqueries: each step has a
-- MAGIC name and reads top to bottom. Same execution plan, so same performance.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_employees
(
  CONSTRAINT valid_employee_id EXPECT (employee_id IS NOT NULL)                                  ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_birth_date  EXPECT (birth_date IS NOT NULL AND birth_date > DATE'1940-01-01') ON VIOLATION DROP ROW,
  CONSTRAINT valid_hire_date   EXPECT (hire_date IS NOT NULL AND hire_date >= birth_date)        ON VIOLATION DROP ROW,
  CONSTRAINT plausible_age     EXPECT (age BETWEEN 16 AND 75)                                    ON VIOLATION DROP ROW,
  CONSTRAINT known_department  EXPECT (department_name IS NOT NULL),
  CONSTRAINT valid_contract    EXPECT (contract_type IN ('Permanent','Fixed-term','Apprentice','Intern','Temp')),
  CONSTRAINT valid_entity      EXPECT (source_entity IN ('FR','BE'))                             ON VIOLATION DROP ROW
)
CLUSTER BY AUTO
COMMENT "Employees of the 2 subsidiaries, cleaned, typed, enriched (age_bracket, seniority) and joined to the departments reference — silver layer"
TBLPROPERTIES ('quality' = 'silver')
AS
-- Step 1 (CTE): typing and derived columns from the raw extracts
WITH typed AS (
  SELECT
    source_entity,                                                    -- originating subsidiary (FR / BE)
    employee_id,
    to_date(extract_date)                                             AS extract_date,
    to_timestamp(extract_ts)                                          AS extract_ts,  -- orders files
    to_timestamp(updated_at)                                          AS updated_at,  -- orders versions within a file
    _source_file,                                                     -- source file (see silver_employees_last_extract)
    initcap(first_name)                                               AS first_name,
    upper(last_name)                                                  AS last_name,
    gender,
    to_date(birth_date)                                               AS birth_date,
    CAST(floor(datediff(current_date(), to_date(birth_date)) / 365.25) AS INT) AS age,
    to_date(hire_date)                                                AS hire_date,
    ROUND(datediff(current_date(), to_date(hire_date)) / 365.25, 1)   AS seniority_years,
    job_title,
    contract_type,
    work_location,
    manager_id,
    lower(email)                                                      AS email,
    CAST(fte AS DOUBLE)                                               AS fte,
    status,
    department_id
  FROM STREAM(bronze_employees)
)
-- Step 2: join to the departments reference + derived brackets
SELECT
  t.*,
  d.department_name,
  d.business_unit,
  d.region,
  d.country,
  d.cost_center,
  -- Derived column: age bracket (CASE WHEN)
  CASE
    WHEN t.age < 20 THEN '<20'
    WHEN t.age < 35 THEN '20-34'
    WHEN t.age < 45 THEN '35-44'
    WHEN t.age < 55 THEN '45-54'
    ELSE '55+'
  END AS age_bracket,
  CASE
    WHEN t.seniority_years < 2  THEN '0-2 yrs'
    WHEN t.seniority_years < 5  THEN '2-5 yrs'
    WHEN t.seniority_years < 10 THEN '5-10 yrs'
    WHEN t.seniority_years < 20 THEN '10-20 yrs'
    ELSE '20+ yrs'
  END AS seniority_bracket
FROM typed t
LEFT JOIN ${master_data_schema}.departments d
  ON t.department_id = d.department_id;
