# 6 / 360° employee view — consumption Materialized View (Unity Catalog)
# Twin SQL file: 01-HR_SDP_SQL/02-Absences/DLT-Gold-Employee-360.sql
#
# spark.read.table(...) (batch read) inside a @dp.materialized_view: the result is stored and
# refreshed on every pipeline update.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_employee_360",
    comment="360° employee view — consolidation (current state) + year-to-date absence KPIs",
    table_properties={"quality": "gold"},
    cluster_by_auto=True,
)
def gold_employee_360():
    # Step 1: year-to-date absence KPIs per employee (= `absences_ytd` CTE in SQL)
    absences_ytd = (
        spark.read.table("silver_absences")
        .filter(F.year("start_date") == F.year(F.current_date()))
        .groupBy("employee_id")
        .agg(
            F.sum("days").alias("absence_days_ytd"),
            F.count("*").alias("absence_events_ytd"),
        )
    )

    # Step 2: current state of each active employee, enriched with the KPIs
    employees = spark.read.table("gold_employees_current")
    return (
        employees.alias("e")
        .join(absences_ytd.alias("ab"), F.col("e.employee_id") == F.col("ab.employee_id"), "left")
        .filter(F.col("e.status") == "Active")
        .select(
            "e.*",
            F.coalesce(F.col("ab.absence_days_ytd"), F.lit(0)).alias("absence_days_ytd"),
            F.coalesce(F.col("ab.absence_events_ytd"), F.lit(0)).alias("absence_events_ytd"),
        )
    )
