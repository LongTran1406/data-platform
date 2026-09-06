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


def write_gold_tables(cfg: dict, fact_df: DataFrame, agg_df: DataFrame):
    gold = cfg["paths"]["gold"]
    fact_path = f"{gold}gtfs/fact_on_time_performance/"
    agg_path = f"{gold}gtfs/agg_route_daily_performance/"

    fact_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(fact_path)
    agg_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(agg_path)

    print(f"Wrote {fact_df.count()} rows to {fact_path}")
    print(f"Wrote {agg_df.count()} rows to {agg_path}")
