# LDP pipeline — HR 360 (SQL)

Pipeline `HR_360_SQL_<target>` → schema `alp_demo_catalog.acme_hr_en_sql`. The same logic exists in
Python in [`../01-HR_SDP_Python/`](../01-HR_SDP_Python/README.md) (schema `acme_hr_en_python`).
Queries are written with **CTEs** (`WITH … AS`) rather than subqueries: one step = one name,
read top to bottom.

Graph order (the pipeline resolves dependencies automatically):

```
00-Data-Sources/
  DLT-Raw-Employees.sql   -> bronze_employees   (Streaming Table + Auto Loader, 2 Append Flows: landing_fr + landing_be)
  DLT-Raw-Absences.sql    -> bronze_absences    (Streaming Table + Auto Loader, 2 Append Flows: landing_fr + landing_be)

01-Employees/
  DLT-Employees-Silver.sql       -> silver_employees              (Streaming Table + Expectations + departments join)
  DLT-Employees-Last-Extract.sql -> silver_employees_last_extract (Materialized View + window functions ROW_NUMBER / LAG)
  DLT-Employees-Gold.sql         -> gold_employees_current        (Auto CDC — SCD Type 1, current state, SEQUENCE BY STRUCT(extract_ts, updated_at))
                                    gold_employees_history        (Auto CDC — SCD Type 2, mobility history)

02-Absences/
  DLT-Absences-Silver.sql   -> silver_absences    (Streaming Table + Expectations, free-text `comment`)
  DLT-Gold-Employee-360.sql -> gold_employee_360  (Materialized View)
```

Every persisted table uses `CLUSTER BY AUTO` (automatic liquid clustering).

`03-Explorations/` is **not** part of the pipeline (validation queries to run on a SQL Warehouse),
including the SQL vs Python comparison.

LDP features showcased: **Streaming Table**, **Auto Loader** (`STREAM read_files`), **Append Flows**
(multi-source concatenation), **Expectations**, **window functions** in a Materialized View,
**Auto CDC** (SCD Type 1 & 2), **Materialized View**, **CLUSTER BY AUTO**.
