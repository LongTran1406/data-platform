from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F


def _path_key(col):
    """Reduce any file path to the part starting at 'gtfs/'.
    Landing paths and the _source_file values stored in bronze can differ in
    prefix (abfss://... vs relative), so compare only the shared tail."""
    return F.regexp_extract(col, r"(gtfs/.*)$", 1)


def find_new_files(spark, source_glob: str, bronze_path: str) -> list[str]:
    """Landing files matching source_glob that bronze hasn't ingested yet."""
    try:
        # Selecting only `path` means the file contents are never read here
        landing_df = (
            spark.read.format("binaryFile")
            .load(source_glob)
            .select("path")
            .withColumn("key", _path_key(F.col("path")))
        )
    except AnalysisException:
        # Nothing has landed yet, so the glob matches no files
        return []

    # First run: bronze doesn't exist yet, so everything is new
    if DeltaTable.isDeltaTable(spark, bronze_path):
        ingested_df = (
            spark.read.format("delta")
            .load(bronze_path)
            .select(_path_key(F.col("_source_file")).alias("key"))
            .distinct()
        )
        landing_df = landing_df.join(ingested_df, on="key", how="left_anti")

    return [row["path"] for row in landing_df.select("path").collect()]
