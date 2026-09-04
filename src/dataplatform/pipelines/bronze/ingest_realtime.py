from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from google.transit import gtfs_realtime_pb2
from google.protobuf import json_format
import argparse
import json
import os
import sys
from pathlib import Path

try:
    _script_path = Path(__file__).resolve()
except NameError:
    _script_path = Path(sys.argv[0]).resolve()

PROJECT_ROOT = _script_path.parents[4]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dataplatform.config import load_config

def parse_vehiclepos(feed, source_file):
    """Turn a VehiclePositions feed into one row per vehicle."""
    rows = []
    for entity in feed.entity:
        v = entity.vehicle
        rows.append({
            "entity_id": entity.id,
            "trip_id": v.trip.trip_id,
            "route_id": v.trip.route_id,
            "vehicle_id": v.vehicle.id,
            "vehicle_label": v.vehicle.label,
            "latitude": v.position.latitude,
            "longitude": v.position.longitude,
            "bearing": v.position.bearing,
            "speed": v.position.speed,
            "current_stop_sequence": v.current_stop_sequence,
            "stop_id": v.stop_id,
            "vehicle_timestamp": v.timestamp,
            "_source_file": source_file,
        })
    return rows

def parse_realtime(feed, source_file):
    """Turn a TripUpdates feed into one row per trip.
    Per-stop delay details are kept as a JSON string, since each trip can
    have a different number of upcoming stops (doesn't fit neat columns)."""
    rows = []
    for entity in feed.entity:
        tu = entity.trip_update
        stop_time_updates = [json_format.MessageToDict(stu) for stu in tu.stop_time_update]
        rows.append({
            "entity_id": entity.id,
            "trip_id": tu.trip.trip_id,
            "route_id": tu.trip.route_id,
            "vehicle_id": tu.vehicle.id,
            "trip_timestamp": tu.timestamp,
            "delay": tu.delay,
            "stop_time_updates_json": json.dumps(stop_time_updates),
            "_source_file": source_file,
        })
    return rows

def parse_alerts(feed, source_file):
    """Turn an Alerts feed into one row per alert."""
    rows = []
    for entity in feed.entity:
        a = entity.alert
        header = a.header_text.translation[0].text if a.header_text.translation else None
        description = a.description_text.translation[0].text if a.description_text.translation else None
        informed_entities = [json_format.MessageToDict(ie) for ie in a.informed_entity]
        rows.append({
            "entity_id": entity.id,
            "cause": a.cause,
            "effect": a.effect,
            "header_text": header,
            "description_text": description,
            "informed_entities_json": json.dumps(informed_entities),
            "_source_file": source_file,
        })
    return rows

# Maps the --feed name to the function that knows how to read that feed's shape
FEED_PARSERS = {
    "vehiclepos": parse_vehiclepos,
    "realtime": parse_realtime,
    "alerts": parse_alerts,
}

def ingest_realtime_feed(spark, feed_type: str, source_glob: str, bronze_path: str):
    parse_fn = FEED_PARSERS[feed_type]

    # Just fetch the raw file bytes with Spark (fast, reuses existing ABFS auth) -
    # the actual protobuf decoding still happens below in plain Python.
    bin_df = spark.read.format("binaryFile").load(source_glob)

    rows = []
    for record in bin_df.select("path", "content").collect():
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(bytes(record["content"]))
        rows.extend(parse_fn(feed, record["path"]))

    if not rows:
        print(f"No entities found for feed '{feed_type}'")
        return

    out_df = spark.createDataFrame(rows).withColumn("_ingested_at", current_timestamp())
    out_df.write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)
    print(f"Wrote {out_df.count()} rows to {bronze_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="staging")
    parser.add_argument("--feeds", nargs="+", default=["vehiclepos", "realtime", "alerts"])
    args = parser.parse_args()

    cfg = load_config(args.env)
    spark = SparkSession.builder.getOrCreate()

    # Same ABFS OAuth setup as ingest_raw.py - each job task runs in its own
    # process, so this has to be repeated here rather than shared.
    storage_account = cfg["storage_account"]
    account_suffix = f"{storage_account}.dfs.core.windows.net"
    spark.conf.set(f"fs.azure.account.auth.type.{account_suffix}", "OAuth")
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{account_suffix}",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{account_suffix}", os.environ["AZURE_CLIENT_ID"])
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account_suffix}", os.environ["AZURE_CLIENT_SECRET"])
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{account_suffix}",
        f"https://login.microsoftonline.com/{os.environ['AZURE_TENANT_ID']}/oauth2/token",
    )

    for feed_type in args.feeds:
        source_glob = f"{cfg['paths']['landing']}gtfs/{feed_type}/*/*/data.pb"
        bronze_path = f"{cfg['paths']['bronze']}gtfs/{feed_type}/"
        ingest_realtime_feed(spark, feed_type, source_glob, bronze_path)


if __name__ == "__main__":
    main()