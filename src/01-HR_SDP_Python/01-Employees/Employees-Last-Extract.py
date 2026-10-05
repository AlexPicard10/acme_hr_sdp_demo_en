# 2b / Checking the latest delivery — Window functions (Materialized View)
# Twin SQL file: 01-HR_SDP_SQL/01-Employees/DLT-Employees-Last-Extract.sql
# (detailed explanations and the "Auto CDC or window function" table are in the SQL file)
#
#   SQL                                                   Python
#   CREATE OR REFRESH MATERIALIZED VIEW                   @dp.materialized_view(...)
#   … OVER (PARTITION BY a, b ORDER BY c DESC)            .over(Window.partitionBy("a", "b").orderBy(F.col("c").desc()))
#   ROW_NUMBER() / COUNT(*) / LAG(x)                      F.row_number() / F.count("*") / F.lag("x")
#   max_by(_source_file, extract_ts)                      F.max_by("_source_file", "extract_ts")
#
# In Python, the window is a reusable object: declare it once and apply it to several columns.
# Window functions are still not allowed on a streaming table: we read silver_employees in batch
# (spark.read), hence the Materialized View.

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.materialized_view(
    name="silver_employees_last_extract",
    comment="Latest file delivered by each subsidiary, one row per employee (window functions ROW_NUMBER, COUNT, LAG)",
    table_properties={"quality": "silver"},
    cluster_by_auto=True,
)
def silver_employees_last_extract():
    employees = spark.read.table("silver_employees")

    # The most recent file of each subsidiary (= `last_file` CTE in SQL)
    last_file = employees.groupBy("source_entity").agg(
        F.max_by("_source_file", "extract_ts").alias("last_source_file")
    )

    # The windows: all versions of an employee within a subsidiary
    per_employee = Window.partitionBy("source_entity", "employee_id")
    per_employee_oldest_first = per_employee.orderBy("updated_at")
    per_employee_latest_first = per_employee.orderBy(F.col("updated_at").desc())

    # Rows of the latest file (keep only the silver_employees columns: after the join,
    # `source_entity` exists on both sides and the window would be ambiguous)
    in_last_file = (
        employees.alias("e")
        .join(
            last_file.alias("l"),
            (F.col("e.source_entity") == F.col("l.source_entity"))
            & (F.col("e._source_file") == F.col("l.last_source_file")),
            "inner",
        )
        .select("e.*")
    )

    # = `ranked` CTE in SQL
    ranked = in_last_file.select(
        "*",
        F.count("*").over(per_employee).alias("versions_in_file"),
        F.lag("department_id").over(per_employee_oldest_first).alias("previous_department_id"),
        F.lag("job_title").over(per_employee_oldest_first).alias("previous_job_title"),
        F.row_number().over(per_employee_latest_first).alias("rn"),
    )

    # Rank 1 = the most recent version
    return ranked.filter(F.col("rn") == 1).drop("rn")
