# 5 / Preparation & Quality — Absences (Streaming Table + Expectations)
# Twin SQL file: 01-HR_SDP_SQL/02-Absences/DLT-Absences-Silver.sql

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_absences",
    comment="Cleaned and typed absence events — silver layer",
    table_properties={"quality": "silver"},
    cluster_by_auto=True,
)
@dp.expect_or_fail("valid_absence_id", "absence_id IS NOT NULL")
@dp.expect_all_or_drop({
    "valid_employee": "employee_id IS NOT NULL",
    "valid_dates": "start_date IS NOT NULL AND end_date >= start_date",
    "positive_days": "days > 0 AND days <= 365",
})
@dp.expect("known_type", "absence_type IS NOT NULL")
def silver_absences():
    return spark.readStream.table("bronze_absences").select(
        "source_entity",                                                # originating subsidiary (FR / BE)
        "absence_id",
        "employee_id",
        "absence_type",
        F.to_date("start_date").alias("start_date"),
        F.to_date("end_date").alias("end_date"),
        F.col("days").cast("int").alias("days"),
        F.to_timestamp("event_timestamp").alias("event_timestamp"),
        F.date_trunc("month", F.to_date("start_date")).alias("absence_month"),
        # free-text note; '' -> NULL
        F.nullif(F.trim("comment"), F.lit("")).alias("comment"),
    )
