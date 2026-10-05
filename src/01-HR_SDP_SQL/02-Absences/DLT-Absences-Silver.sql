-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 5 / Preparation & Quality — Absences (Streaming Table + Expectations)
-- MAGIC #### Cleaning and quality checks on the absence event stream, reusing the same pattern.
-- MAGIC Consistent dates, plausible duration, known absence type. We derive the absence month for
-- MAGIC downstream aggregates, and keep the **free-text note** (`comment`).

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_absences
(
  CONSTRAINT valid_absence_id EXPECT (absence_id IS NOT NULL)                       ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_employee   EXPECT (employee_id IS NOT NULL)                     ON VIOLATION DROP ROW,
  CONSTRAINT valid_dates      EXPECT (start_date IS NOT NULL AND end_date >= start_date) ON VIOLATION DROP ROW,
  CONSTRAINT positive_days    EXPECT (days > 0 AND days <= 365)                     ON VIOLATION DROP ROW,
  CONSTRAINT known_type       EXPECT (absence_type IS NOT NULL)
)
CLUSTER BY AUTO
COMMENT "Cleaned and typed absence events — silver layer"
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT
  source_entity,                            -- originating subsidiary (FR / BE)
  absence_id,
  employee_id,
  absence_type,
  to_date(start_date)                       AS start_date,
  to_date(end_date)                         AS end_date,
  CAST(days AS INT)                         AS days,
  to_timestamp(event_timestamp)             AS event_timestamp,
  date_trunc('month', to_date(start_date))  AS absence_month,
  NULLIF(trim(comment), '')                 AS comment  -- free-text note ('' -> NULL)
FROM STREAM(bronze_absences);