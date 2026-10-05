# Databricks notebook source
# MAGIC %md
# MAGIC # HR 360 — Setup (schemas, Volumes) + master data build (Departments reference)
# MAGIC
# MAGIC Run **once** before the LDP pipelines. Creates the **three schemas**, the **two UC Volumes** that
# MAGIC receive the raw HR data (written by the `01-Generate-HR-Data.py` notebook) and the
# MAGIC `departments` reference table, shared by the group and joined in the silver layer.
# MAGIC
# MAGIC | Schema | Content |
# MAGIC |---|---|
# MAGIC | `acme_hr_en` | shared raw data: Volumes `landing_fr` / `landing_be`, `departments` reference table |
# MAGIC | `acme_hr_en_sql` | tables of the SDP pipeline written in **SQL** (`src/01-HR_SDP_SQL`) |
# MAGIC | `acme_hr_en_python` | tables of the SDP pipeline written in **Python** (`src/01-HR_SDP_Python`), same names |
# MAGIC
# MAGIC **Two subsidiaries, two storage locations.** ACME France and ACME Belgium each run their own HRIS
# MAGIC and deliver their extracts (same structure) to their own Volume: `landing_fr` and `landing_be`.
# MAGIC Each Volume plays the role of a subsidiary's "bucket"; in production these would be two external
# MAGIC Volumes on two buckets. Each pipeline concatenates both sources with Append Flows.
# MAGIC
# MAGIC > The `alp_demo_catalog` catalog already exists (adapt it to your workspace).
# MAGIC > The `department_id` values here must match those generated in the employee extracts
# MAGIC > (`01-Generate-HR-Data.py`).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Shared raw data (Volumes + master data)
# MAGIC CREATE SCHEMA IF NOT EXISTS alp_demo_catalog.acme_hr_en;
# MAGIC -- One target schema per pipeline: the same logic written in SQL and in Python
# MAGIC CREATE SCHEMA IF NOT EXISTS alp_demo_catalog.acme_hr_en_sql    COMMENT 'HR 360 — SQL SDP pipeline outputs';
# MAGIC CREATE SCHEMA IF NOT EXISTS alp_demo_catalog.acme_hr_en_python COMMENT 'HR 360 — Python SDP pipeline outputs';
# MAGIC -- One managed Volume per subsidiary (one folder per dataset: employees/, absences/)
# MAGIC CREATE VOLUME IF NOT EXISTS alp_demo_catalog.acme_hr_en.landing_fr COMMENT 'HRIS extracts — ACME France';
# MAGIC CREATE VOLUME IF NOT EXISTS alp_demo_catalog.acme_hr_en.landing_be COMMENT 'HRIS extracts — ACME Belgium';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Departments reference
# MAGIC 12 HR departments spread across Business Units and regions (fictional data).

# COMMAND ----------

departments = [
    # (id,     name,                       business_unit,           region,          country,   cost_center)
    ("D001",  "Executive Management",      "Corporate",             "Île-de-France", "France",  "CC-1000"),
    ("D002",  "Human Resources",           "Corporate",             "Île-de-France", "France",  "CC-1100"),
    ("D003",  "Finance & Controlling",     "Corporate",             "Île-de-France", "France",  "CC-1200"),
    ("D004",  "Information Systems",       "Corporate",             "Hauts-de-France","France", "CC-1300"),
    ("D005",  "Network Operations",        "Networks",              "Auvergne-Rhône-Alpes", "France", "CC-2000"),
    ("D006",  "Industrial Maintenance",    "Networks",              "Grand Est",     "France",  "CC-2100"),
    ("D007",  "Renewable Energy",          "Renewables",            "Occitanie",     "France",  "CC-3000"),
    ("D008",  "Trading & Markets",         "Global Energy Mgmt",    "Île-de-France", "France",  "CC-4000"),
    ("D009",  "B2B Sales",                 "Retail",                "Provence-Alpes-Côte d'Azur", "France", "CC-5000"),
    ("D010",  "B2C Customer Care",         "Retail",                "Nouvelle-Aquitaine", "France", "CC-5100"),
    ("D011",  "Research & Innovation",     "Research",              "Île-de-France", "France",  "CC-6000"),
    ("D012",  "Benelux Operations",        "International",         "Brussels",      "Belgium", "CC-7000"),
]

columns = ["department_id", "department_name", "business_unit", "region", "country", "cost_center"]

df = spark.createDataFrame(departments, columns)

(df.write
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable("alp_demo_catalog.acme_hr_en.departments"))

spark.sql("ALTER TABLE alp_demo_catalog.acme_hr_en.departments "
          "SET TBLPROPERTIES ('comment' = 'HR departments reference (master data)')")

display(spark.table("alp_demo_catalog.acme_hr_en.departments"))
