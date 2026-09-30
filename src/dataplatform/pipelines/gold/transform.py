from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

STOP_TIME_UPDATE_SCHEMA = ArrayType(StructType([
    StructField("stopId", StringType()),
    StructField("scheduleRelationship", StringType()),
    StructField("arrival", StructType([StructField("delay", IntegerType())])),
    StructField("departure", StructType([StructField("delay", IntegerType())])),
]))


def build_on_time_performance(spark, cfg: dict, on_time_threshold_seconds: int) -> tuple[DataFrame, DataFrame]:
    silver = cfg["paths"]["silver"]

    realtime_df = spark.read.format("delta").load(f"{silver}gtfs/realtime/")
    stop_time_df = spark.read.format("delta").load(f"{silver}gtfs/stop_times/")
    routes_df = spark.read.format("delta").load(f"{silver}gtfs/routes/")

    # One row per scheduled stop on the trip. explode_outer (not explode) keeps
    # entities whose update list is empty (e.g. RTTA_DEF deadhead trips), same
    # as pandas' .explode() does for an empty list.
    rt = realtime_df.withColumn(
        "stu", F.explode_outer(F.from_json(F.col("stop_time_updates_json"), STOP_TIME_UPDATE_SCHEMA))
    )
    rt = rt.withColumn("stop_id", F.col("stu.stopId"))
    rt = rt.withColumn("actual_delay_seconds", F.col("stu.arrival.delay"))

    # Join on (trip_id, stop_id) only - accepted limitation: the feed's JSON
    # never includes stopSequence, so loop/City-Circle trips revisiting the
    # same stop_id can double count that stop. Not fixable without that field.
    detail = rt.join(
        stop_time_df.select("trip_id", "stop_id", "stop_sequence", "arrival_time"),
        on=["trip_id", "stop_id"],
        how="left",
    )

    detail = detail.withColumn("service_date", F.to_date("_ingested_at"))
    detail = detail.withColumnRenamed("arrival_time", "scheduled_arrival_time")
    detail = detail.withColumn(
        "on_time", F.abs(F.col("actual_delay_seconds")) <= F.lit(on_time_threshold_seconds)
    )

    detail = detail.join(
        routes_df.select("route_id", "route_short_name"), on="route_id", how="left"
    )

    fact_df = detail.select(
        "service_date", "trip_id", "route_id", "route_short_name",
        "stop_id", "stop_sequence", "scheduled_arrival_time",
        "actual_delay_seconds", "on_time", "vehicle_id",
    )

    agg_df = (
        fact_df.groupBy("service_date", "route_short_name")
        .agg(
            F.count("stop_id").alias("total_stop_number"),
            F.sum(F.col("on_time").cast("int")).alias("stop_on_time"),
            (F.avg(F.col("on_time").cast("double")) * 100).alias("pct_on_time"),
            F.avg("actual_delay_seconds").alias("avg_delay_sec"),
            F.max("actual_delay_seconds").alias("max_delay_sec"),
        )
    )

    return fact_df, agg_df


def write_gold_tables(spark, schema: str, fact_df: DataFrame, agg_df: DataFrame):
    catalog = spark.sql("SELECT current_catalog()").first()[0]
    if catalog == "hive_metastore":
        raise RuntimeError(
            "The job's default catalog is hive_metastore. Gold tables must be "
            "written to a Unity Catalog catalog so the dashboard can read them."
        )

    # Unqualified schema/table names resolve inside the current (workspace) catalog
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    tables = {
        "fact_on_time_performance": fact_df,
        "agg_route_daily_performance": agg_df,
    }
    for name, df in tables.items():
        full_name = f"{schema}.{name}"
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
        print(f"Wrote {spark.table(full_name).count()} rows to {catalog}.{full_name}")
