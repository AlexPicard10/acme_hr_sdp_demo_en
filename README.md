# ACME HR 360 — Data + Spark Declarative Pipelines (SDP) demo

Demo asset to **upskill HR Data teams on Databricks and Spark Declarative Pipelines (SDP)**. It
builds, end to end and with **synthetic HR data**, a **360° employee view** (*HR 360*):

> HRIS extracts from **two subsidiaries**, each dropped into its own **Unity Catalog Volume** →
> **Auto Loader** ingestion and concatenation with **Append Flows**
> → **Spark Declarative Pipeline** (bronze → silver → gold), written **twice, in SQL and in Python**,
> to compare both languages → **Materialized Views**, all governed by **Unity Catalog** and deployed
> by a **Databricks Asset Bundle**.

Designed as **workshop / enablement material**: every step is annotated and showcases an SDP
feature in a realistic HR business scenario.

> **SDP** (Spark Declarative Pipelines) = **LDP** (Lakeflow Declarative Pipelines), formerly DLT —
> interchangeable terms.

## What the demo shows (SDP features)

| SDP feature | Where | Role in the pipeline (bronze→silver→gold) |
|---|---|---|
| **Streaming Table** + **Auto Loader** (`STREAM read_files`) | `bronze_employees`, `bronze_absences` | Raw data ingestion — bronze |
| **Append Flows** (`CREATE FLOW … INSERT INTO`) | `bronze_employees`, `bronze_absences` | Concatenating 2 sources with the same structure (FR and BE subsidiaries, 2 Volumes) — bronze |
| **Expectations** (declarative data quality) | `silver_employees`, `silver_absences` | Data quality checks — silver |
| Derived columns (`age_bracket`, `seniority_years`) + reference join | `silver_employees` | Preparation & enrichment — silver |
| **Window functions** (`ROW_NUMBER`, `COUNT OVER`, `LAG`) in a **Materialized View** | `silver_employees_last_extract` | Latest row per employee in the latest delivered file — silver |
| **Auto CDC — SCD Type 1** (latest version per employee, `SEQUENCE BY STRUCT(extract_ts, updated_at)`) | `gold_employees_current` | Incremental deduplication / latest value — gold |
| **Auto CDC — SCD Type 2** (mobility history) | `gold_employees_history` | History — gold |
| **Materialized View** (stored result, refreshed by the pipeline) | `gold_employee_360` | 360° employee view exposed to consumers — gold |
| **`CLUSTER BY AUTO`** (automatic liquid clustering) | every pipeline table | Performance without manual tuning |
| **SQL and Python**: same pipeline, two languages (`pyspark.pipelines`) | schemas `acme_hr_en_sql` and `acme_hr_en_python` | Compare the syntaxes; identical results, checked with `EXCEPT` |

Scenario: two subsidiaries, **ACME France** and **ACME Belgium**, each deliver their **Employees +
Absences** extracts (same structure) to their own storage; a shared **Departments** reference
(headcount, age pyramid, gender mix, absenteeism).

## Architecture

![HR 360 architecture — 2 UC Volumes → Auto Loader + Append Flows → SDP pipeline in SQL (acme_hr_en_sql) and in Python (acme_hr_en_python) → bronze → silver (Expectations, window functions) → gold (Auto CDC SCD1/SCD2, 360° Materialized View) → consumption governed by Unity Catalog](docs/hr360-architecture.png)

```
UC Volume landing_fr (ACME France)        UC Volume landing_be (ACME Belgium)
   ├── employees/  (JSONL)                    ├── employees/  (JSONL, same structure)
   └── absences/   (JSONL)                    └── absences/   (JSONL)
        │  Auto Loader · flow *_fr                  │  Auto Loader · flow *_be
        └──────────────────┬────────────────────────┘
                           ▼  Append Flows (concatenation, source_entity column)
BRONZE  bronze_employees · bronze_absences                     (streaming tables)
        │  2 pipelines, same logic:  HR_360_SQL    → schema acme_hr_en_sql
        │                            HR_360_Python → schema acme_hr_en_python
        ▼  cleaning, typing, derived columns, expectations, join acme_hr_en.departments
SILVER  silver_employees · silver_absences                     (streaming tables + expectations)
        silver_employees_last_extract   (materialized view · ROW_NUMBER / LAG: latest delivery)
        ▼
GOLD    gold_employees_current        (Auto CDC SCD1 — current state)
        gold_employees_history        (Auto CDC SCD2 — history)
        gold_employee_360             (consumption materialized view)
```

Catalog `alp_demo_catalog`, three schemas:

| Schema | Content |
|---|---|
| `acme_hr_en` | shared sources: Volumes `landing_fr` / `landing_be` (raw data), `departments` reference |
| `acme_hr_en_sql` | tables of the **SQL** pipeline (`src/01-HR_SDP_SQL`) |
| `acme_hr_en_python` | tables of the **Python** pipeline (`src/01-HR_SDP_Python`) — same names as in SQL |

Tables are prefixed by layer (`bronze_` / `silver_` / `gold_`).

### SQL or Python?

Both pipelines read the same Volumes and produce the same tables. The full equivalence table is in
[`src/01-HR_SDP_Python/README.md`](src/01-HR_SDP_Python/README.md). In short: **SQL** for
readability and query-oriented teams; **Python** for reuse (one shared ingestion function, flows
generated by a loop), unit testing, and Python-only features (sinks, Auto CDC from snapshots,
custom data sources).

### Window function or Auto CDC?

- **Auto CDC** for production: it maintains the latest version (SCD1) or the history (SCD2),
  incrementally — each run only processes the new rows.
- **Window function** (in a Materialized View) for analysis and checks: latest file only, top N,
  comparison with the previous version. It rereads its source, so it costs more on large volumes,
  and window functions are not supported on streaming tables.

## Prerequisites

Everything happens **in the Databricks workspace UI**: nothing to install locally.

- A Databricks workspace with Unity Catalog and **serverless** compute.
- `ALL_PRIVILEGES` (or CREATE SCHEMA / VOLUME) on the **`alp_demo_catalog`** catalog
  (adapt it to your catalog in `databricks.yml` and the notebooks).
- The repo cloned into a **Git folder**: **+ New → Git folder**, this repo's URL, branch `main`.

> Storage is **Unity Catalog Volumes** (one per subsidiary): no bucket, IAM role or credential to
> provision. In production, each Volume could be an external Volume on the subsidiary's bucket;
> the pipeline does not change.

## Walkthrough (step by step)

### 1. Unity Catalog setup (schemas + Volumes) & master data
In the Git folder, open the notebook `src/00-Utiles/00-MasterData-build` and click **Run all**. It
creates the schemas `acme_hr_en`, `acme_hr_en_sql` and `acme_hr_en_python`, the **Volumes**
`landing_fr` and `landing_be` (one per subsidiary, with `employees/` and `absences/` subfolders)
and the `alp_demo_catalog.acme_hr_en.departments` reference table.

### 2. Generate and load the raw data into the Volumes
Open the notebook `src/00-Utiles/01-Generate-HR-Data` and click **Run all** **twice**, once per
subsidiary. Widgets:
- `entity` = **`FR`** on the first run, then **`BE`** on the second (e.g. 50,000 then 10,000 employees);
- `mode` = **`seed`**: initial load, 50,000 employees and 12 months of absences by default (`employees` widget);
- `bad_records` = **`yes`** (default): adds a few deliberately non-compliant rows to see the
  Expectations in action.

The notebook writes the JSONL extracts **directly to the subsidiary's Volume**
(`.../landing_fr/…` or `.../landing_be/…`) and keeps one roster per subsidiary in
`_state/roster.json`, so identifiers (`EMPFR…`, `EMPBE…`) stay stable across runs. Each extract
also contains ~2% of employees with several versions (intra-day corrections, `updated_at`): this
feeds the window function example. Check: **Catalog Explorer → `alp_demo_catalog` → `acme_hr_en`
→ Volumes**.

### 3. Deploy and run the pipelines (bundle UI)
Both pipelines are described by the **bundle** (`databricks.yml`, `resources/hr_pipeline_sql.pipeline.yml`
and `resources/hr_pipeline_python.pipeline.yml`); deploy them from the UI, no CLI needed.

1. In the Git folder, open `databricks.yml`, then the **Deployments** panel (🚀 icon in the editor
   side bar).
2. Pick the **`dev`** target and click **Deploy**. The panel lists the created resources: the
   pipelines `[dev <your_name>] HR_360_SQL_dev` and `[dev <your_name>] HR_360_Python_dev`.
3. In the same panel, click **Run** on `hr_pipeline_sql`, then on `hr_pipeline_python` (they can
   run in parallel), or open each pipeline and click **Start**.
4. Follow the graphs: bronze → silver → gold. They are identical across languages. The **Data
   quality** tab shows the rows rejected by the Expectations (same names, same counters).

> On the `dev` target, the deployment is **source-linked**: the pipelines read the SQL and Python
> files directly from the Git folder. A code change is picked up on the next run, without redeploying.

> **CLI alternative** (optional): `databricks bundle deploy -t dev`, then
> `databricks bundle run hr_pipeline_sql -t dev` and `databricks bundle run hr_pipeline_python -t dev`,
> with an authenticated CLI profile.

### 4. Explore the results
Run the queries of `src/01-HR_SDP_SQL/03-Explorations/hr_exploration.sql` on a SQL Warehouse
(streaming tables, Append Flows, window functions, SCD1/SCD2, 360° employee view). The
**"SQL vs Python"** section checks that both pipelines produce the same results.

### 5. Showcase incremental processing + CDC
Rerun the `01-Generate-HR-Data` notebook with the `mode` widget = **`increment`**: it produces a
new employee extract (transfers / hires / leavers) with a more recent `extract_ts`. Then rerun the
pipelines with **Run** in the Deployments panel, or **Start** on each pipeline. Use a **normal**
run, not a full refresh.

Only the new files are ingested (Auto Loader, directory listing on the Volume).
`gold_employees_current` reflects the up-to-date state (SCD1); `gold_employees_history`
accumulates the mobility history (SCD2, ordered by `extract_ts`).

## Going further (optional)
- **AI/BI dashboard** on `gold_employee_360` and `gold_employees_history` (headcount per BU, age
  pyramid, absenteeism per month / type).
- **Schedule** the pipelines (job + notifications).
- Add other HR sources (payroll, training) by reusing the same bronze→silver→gold skeleton.

## Structure
```
acme_hr_sdp_demo_en/
├── databricks.yml                     # DAB bundle (variables, dev/prod targets)
├── docs/                              # architecture diagram (PNG + HTML source)
├── resources/
│   ├── hr_pipeline_sql.pipeline.yml     # serverless SQL SDP pipeline    → schema acme_hr_en_sql
│   └── hr_pipeline_python.pipeline.yml  # serverless Python SDP pipeline → schema acme_hr_en_python
└── src/
    ├── 00-Utiles/
    │   ├── 00-MasterData-build.py     # setup: 3 schemas + 2 Volumes + departments reference
    │   └── 01-Generate-HR-Data.py     # synthetic HR data generator NOTEBOOK (widgets, writes to the Volumes)
    ├── 01-HR_SDP_SQL/                 # pipeline in SQL
    │   ├── 00-Data-Sources/           # bronze (Auto Loader + Append Flows)
    │   ├── 01-Employees/              # silver + window functions + gold (SCD1/SCD2)
    │   ├── 02-Absences/               # silver + 360° view MV
    │   └── 03-Explorations/           # validation queries, incl. SQL vs Python (not in the pipeline)
    └── 01-HR_SDP_Python/              # the same pipeline in Python (same layout)
        └── utilities/                 # shared ingestion function (imported, not a pipeline source)
```
