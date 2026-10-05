# 2 / Preparation & Quality — Employees (Streaming Table + Expectations)
# Twin SQL file: 01-HR_SDP_SQL/01-Employees/DLT-Employees-Silver.sql
#
#   SQL                                              Python
#   CONSTRAINT c EXPECT (…) ON VIOLATION FAIL UPDATE @dp.expect_or_fail("c", "…")
#   CONSTRAINT c EXPECT (…) ON VIOLATION DROP ROW    @dp.expect_all_or_drop({"c": "…", …})
#   CONSTRAINT c EXPECT (…)                          @dp.expect_all({"c": "…", …})   (warn)
#   WITH typed AS (… FROM STREAM(bronze_employees))  typed = spark.readStream.table("bronze_employees")…
#   CASE WHEN … END                                  F.when(…).when(…).otherwise(…)
#
# Expectation conditions stay SQL expressions (strings), with the same names as in SQL:
# the data quality metrics of both pipelines can be compared directly.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

MASTER_DATA_SCHEMA = spark.conf.get("master_data_schema")
# Same literal as in SQL (365.25 is a DECIMAL(5,2)): identical types and rounding.
DAYS_PER_YEAR = F.lit(365.25).cast("decimal(5,2)")


@dp.table(
    name="silver_employees",
    comment="Employees of the 2 subsidiaries, cleaned, typed, enriched (age_bracket, seniority) and joined to the departments reference — silver layer",
    table_properties={"quality": "silver"},
    cluster_by_auto=True,
)
@dp.expect_or_fail("valid_employee_id", "employee_id IS NOT NULL")
@dp.expect_all_or_drop({
    "valid_birth_date": "birth_date IS NOT NULL AND birth_date > DATE'1940-01-01'",
    "valid_hire_date": "hire_date IS NOT NULL AND hire_date >= birth_date",
    "plausible_age": "age BETWEEN 16 AND 75",
    "valid_entity": "source_entity IN ('FR','BE')",
})
@dp.expect_all({
    "known_department": "department_name IS NOT NULL",
    "valid_contract": "contract_type IN ('Permanent','Fixed-term','Apprentice','Intern','Temp')",
})
def silver_employees():
    # Step 1: typing and derived columns from the raw extracts (= `typed` CTE in SQL)
    typed = spark.readStream.table("bronze_employees").select(
        "source_entity",                                                # originating subsidiary (FR / BE)
        "employee_id",
        F.to_date("extract_date").alias("extract_date"),
        F.to_timestamp("extract_ts").alias("extract_ts"),               # orders files
        F.to_timestamp("updated_at").alias("updated_at"),               # orders versions within a file
        "_source_file",                                                 # source file
        F.initcap("first_name").alias("first_name"),
        F.upper("last_name").alias("last_name"),
        "gender",
        F.to_date("birth_date").alias("birth_date"),
        F.floor(F.datediff(F.current_date(), F.to_date("birth_date")) / DAYS_PER_YEAR).cast("int").alias("age"),
        F.to_date("hire_date").alias("hire_date"),
        F.round(F.datediff(F.current_date(), F.to_date("hire_date")) / DAYS_PER_YEAR, 1).alias("seniority_years"),
        "job_title",
        "contract_type",
        "work_location",
        "manager_id",
        F.lower("email").alias("email"),
        F.col("fte").cast("double").alias("fte"),
        "status",
        "department_id",
    )

    # Departments reference (master data): batch read, stream-static join
    departments = spark.read.table(f"{MASTER_DATA_SCHEMA}.departments")

    # Step 2: join to the reference + derived brackets
    return (
        typed.alias("t")
        .join(departments.alias("d"), F.col("t.department_id") == F.col("d.department_id"), "left")
        .select(
            "t.*",
            "d.department_name",
            "d.business_unit",
            "d.region",
            "d.country",
            "d.cost_center",
            F.when(F.col("t.age") < 20, "<20")
            .when(F.col("t.age") < 35, "20-34")
            .when(F.col("t.age") < 45, "35-44")
            .when(F.col("t.age") < 55, "45-54")
            .otherwise("55+")
            .alias("age_bracket"),
            F.when(F.col("t.seniority_years") < 2, "0-2 yrs")
            .when(F.col("t.seniority_years") < 5, "2-5 yrs")
            .when(F.col("t.seniority_years") < 10, "5-10 yrs")
            .when(F.col("t.seniority_years") < 20, "10-20 yrs")
            .otherwise("20+ yrs")
            .alias("seniority_bracket"),
        )
    )
