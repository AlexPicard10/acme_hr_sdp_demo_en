# 3 & 4 / Consolidation and history — Employees (Auto CDC, SCD Type 1 and 2)
# Twin SQL file: 01-HR_SDP_SQL/01-Employees/DLT-Employees-Gold.sql
#
#   SQL                                              Python
#   CREATE OR REFRESH STREAMING TABLE t;             dp.create_streaming_table("t", …)
#   CREATE FLOW f AS AUTO CDC INTO t                 dp.create_auto_cdc_flow(target="t", name="f",
#     FROM STREAM(silver_employees)                    source="silver_employees",
#     KEYS (employee_id)                               keys=["employee_id"],
#     SEQUENCE BY STRUCT(extract_ts, updated_at)       sequence_by=F.struct("extract_ts", "updated_at"),
#     STORED AS SCD TYPE 2                             stored_as_scd_type=2,
#     TRACK HISTORY ON a, b                            track_history_column_list=["a", "b"])
#
# No decorator here: the source is passed by NAME (not a DataFrame), Auto CDC reads it as a
# stream. SEQUENCE BY STRUCT(extract_ts, updated_at) = the streaming, incremental equivalent of
# ROW_NUMBER (see Employees-Last-Extract.py).

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SEQUENCE = F.struct("extract_ts", "updated_at")

# --- SCD Type 1: current state, latest known version of each employee ---
dp.create_streaming_table(
    name="gold_employees_current",
    comment="Employees — current state: latest known version of each employee (SCD1)",
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
    cluster_by_auto=True,
)

dp.create_auto_cdc_flow(
    name="gold_employees_current_flow",
    target="gold_employees_current",
    source="silver_employees",
    keys=["employee_id"],
    sequence_by=SEQUENCE,
    stored_as_scd_type=1,
)

# --- SCD Type 2: mobility history (department / contract / job title) ---
dp.create_streaming_table(
    name="gold_employees_history",
    comment="Employee history (SCD2) — department / contract / job title mobility over time",
    table_properties={"quality": "gold"},
    cluster_by_auto=True,
)

dp.create_auto_cdc_flow(
    name="gold_employees_history_flow",
    target="gold_employees_history",
    source="silver_employees",
    keys=["employee_id"],
    sequence_by=SEQUENCE,
    stored_as_scd_type=2,
    track_history_column_list=["department_id", "contract_type", "job_title"],
)
