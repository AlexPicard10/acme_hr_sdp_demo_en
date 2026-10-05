# 1b / Ingesting the Absence events of both subsidiaries (Auto Loader + Append Flows)
# Twin SQL file: 01-HR_SDP_SQL/00-Data-Sources/DLT-Raw-Absences.sql
#
# Same pattern as employees, and the same ingestion function (utilities/ingestion.py):
# one target table, one Append Flow per subsidiary.

from pyspark import pipelines as dp

from utilities.ingestion import read_hr_extract

SOURCES = {
    "FR": "absences_path_fr",
    "BE": "absences_path_be",
}

dp.create_streaming_table(
    name="bronze_absences",
    comment="Raw absence events of the 2 subsidiaries (FR, BE), concatenated by Append Flows from 2 UC Volumes — bronze layer",
    table_properties={"quality": "bronze"},
    cluster_by_auto=True,
)


def add_entity_flow(entity: str, path: str) -> None:
    @dp.append_flow(target="bronze_absences", name=f"bronze_absences_{entity.lower()}")
    def _flow():
        return read_hr_extract(spark, path, entity)


for entity, conf_key in SOURCES.items():
    add_entity_flow(entity, spark.conf.get(conf_key))
