"""Helpers shared by the bronze files of the Python pipeline.

This module is NOT a pipeline source (it is outside the `libraries` globs): it is plain Python code,
imported by the pipeline files thanks to `root_path` (the `01-HR_SDP_Python` folder is added to
`sys.path`). This is one of Python's strengths: the ingestion logic is written once and reused for
every dataset and every subsidiary.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_hr_extract(spark: SparkSession, path: str, entity: str) -> DataFrame:
    """Streams (Auto Loader) a subsidiary's JSON extracts and adds the technical columns.

    SQL equivalent: `SELECT *, '<entity>' AS source_entity, _metadata.file_path AS _source_file, …
    FROM STREAM read_files('<path>', format => 'json', inferColumnTypes => true,
    schemaEvolutionMode => 'rescue')`.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(path)
        .select(
            "*",
            F.lit(entity).alias("source_entity"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_source_file_ts"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
