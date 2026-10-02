from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from dataplatform.pipelines.gold.transform import require_unity_catalog, write_table

def build_trip_delay_features(spark, schema: str, lookback_hours: int) -> DataFrame:
    fact_df = spark.table(f"{schema}.fact_on_time_performance")

    # GTFS arrival_time is an "HH:MM:SS" string that can exceed 24:00:00 for
    # after-midnight trips, so it is parsed by hand rather than cast to a timestamp.
    parts = F.split("scheduled_arrival_time", ":")
    arrival_seconds = (
        parts.getItem(0).cast("int") * 3600
        + parts.getItem(1).cast("int") * 60
        + parts.getItem(2).cast("int")
    )

    df = fact_df.withColumn("arrival_seconds", arrival_seconds)
    df = df.withColumn(
        "event_epoch",
        F.unix_timestamp(F.col("service_date").cast("timestamp")) + F.col("arrival_seconds"),
    )
    df = df.withColumn("hour_of_day", (F.floor(F.col("arrival_seconds") / 3600) % 24).cast("int"))
    df = df.withColumn("day_of_week", F.dayofweek("service_date"))

    # Trailing window ending 1s before the current row, so a row never sees
    # its own outcome.
    window = (
        Window.partitionBy("route_id")
        .orderBy("event_epoch")
        .rangeBetween(-lookback_hours * 3600, -1)
    )

    df = df.withColumn("route_avg_delay_recent", F.avg("actual_delay_seconds").over(window))
    df = df.withColumn("route_pct_on_time_recent", F.avg(F.col("on_time").cast("double")).over(window))

    return df.select(
        "trip_id", "route_id", "stop_id", "stop_sequence", "service_date",
        "scheduled_arrival_time", "event_epoch",
        "hour_of_day", "day_of_week",
        "route_avg_delay_recent", "route_pct_on_time_recent",
        "actual_delay_seconds", "on_time",
    )


def write_ml_features(spark, schema: str, df: DataFrame):
    catalog = require_unity_catalog(spark)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    write_table(spark, schema, "ml_features_trip_delay", df, catalog)
