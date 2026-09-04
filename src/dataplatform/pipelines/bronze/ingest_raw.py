from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
import argparse
import sys
from pathlib import Path
import os

try:
    _script_path = Path(__file__).resolve()
except NameError:
    _script_path = Path(sys.argv[0]).resolve()

PROJECT_ROOT = _script_path.parents[4]
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
    parser.add_argument("--tables", nargs="+", required=True)
    args = parser.parse_args()
    cfg = load_config(args.env)
    spark = SparkSession.builder.getOrCreate()

    storage_account = cfg["storage_account"]
    # Spark needs to know which storage account these login settings apply to,
    # so we build its full address here and reuse it below
    account_suffix = f"{storage_account}.dfs.core.windows.net"

    # Tell Spark to login using OAuth (client ID + secrete), not a plain storage key
    spark.conf.set(f"fs.azure.account.auth.type.{account_suffix}", "OAuth")

    # Login as an an app/service not a person
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{account_suffix}",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )

    # The username for the app that's allowed to read/write this storage account
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{account_suffix}", os.environ["AZURE_CLIENT_ID"])

    # The password for the same app
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account_suffix}", os.environ["AZURE_CLIENT_SECRET"])

    # The Microsoft login page address used to check the username/password above
    # and hand back an access token
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{account_suffix}",
        f"https://login.microsoftonline.com/{os.environ['AZURE_TENANT_ID']}/oauth2/token",
    )

    for table in args.tables:
        source_path = f"{cfg['paths']['landing']}gtfs/schedule/*/*/{table}.txt"
        bronze_path = f"{cfg['paths']['bronze']}gtfs/{table}/"
        ingest_raw_txt(spark, source_path, bronze_path)

if __name__ == "__main__":
    main()