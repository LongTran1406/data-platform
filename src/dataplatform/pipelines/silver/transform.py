from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def dedupe(df: DataFrame, keys: list[str] | None) -> DataFrame:
    """Keeps the most recently ingested copy of each unique key. Falls back
    to dropping exact duplicate rows for tables with no unique key."""
    if not keys:
        return df.dropDuplicates()

    window = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

def build_valid_condition(rules: dict):
    """One combined boolean Column expression: True for rows that pass every
    per-row rule. Range/allowed-value checks cast to double first, since
    bronze columns may be stored as strings or as inferred numeric types
    depending on how each table was ingested."""

    condition = F.lit(True)
    for column in rules.get("not_null", []):
        condition = condition & F.col(column).isNotNull()

    for column, (min_value, max_value) in rules.get("ranges", {}).items():
        numeric_col = F.col(column).cast("double")
        condition = condition & numeric_col.between(min_value, max_value)

    for column, allowed_values in rules.get("allowed_values", {}).items():
        numeric_col = F.col(column).cast("double")
        condition = condition & numeric_col.isin([float(v) for v in allowed_values])

    return condition

def clean_transform_table(spark, cfg, rules, table_name):
    bronze_path = f"{cfg['paths']['bronze']}gtfs/{table_name}/"
    silver_path = f"{cfg['paths']['silver']}gtfs/{table_name}/"
    quarantine_path = f"{cfg['paths']['quarantine']}gtfs/{table_name}/"

    bronze_df = spark.read.format("delta").load(bronze_path)
    deduped_df = dedupe(bronze_df, rules.get("unique")).cache()

    condition = build_valid_condition(rules)
    valid_df = deduped_df.filter(condition)
    invalid_df = deduped_df.filter(~condition)

    valid_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)

    invalid_count = invalid_df.count()
    if invalid_count > 0:
        invalid_df.write.format("delta").mode("append").option("mergeSchema", "true").save(quarantine_path)
        print(f"{table_name}: {invalid_count} rows quarantined")

    valid_count = valid_df.count()
    print(f"{table_name}: {valid_count} valid rows written to silver")

    deduped_df.unpersist()
    return valid_df, bronze_df