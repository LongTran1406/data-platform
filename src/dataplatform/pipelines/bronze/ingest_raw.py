from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dataplatform.config import load_config


def ingest_raw_txt(spark, source_path: str, bronze_path: str):
    df = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .load(source_path)) \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", input_file_name())
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)
    count = df.count()
    print(f"Wrote {count} rows to {bronze_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="staging")
    parser.add_argument("--table", required=True)
    args = parser.parse_args()
    cfg = load_config(args.env)
    spark = SparkSession.builder.getOrCreate()
    source_path = f"{cfg['paths']['landing']}gtfs/schedule/*/*/{args.table}.txt"
    bronze_path = f"{cfg['paths']['bronze']}gtfs/{args.table}/"

    ingest_raw_txt(spark, source_path, bronze_path)

if __name__ == "__main__":
    main()