import argparse
from pathlib import Path
import sys
from pyspark.sql import SparkSession
import os

try:
    _script_path = Path(__file__).resolve()
except NameError:
    _script_path = Path(sys.argv[0]).resolve()

PROJECT_ROOT = _script_path.parents[4]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dataplatform.config import load_config, load_gold_config, load_ml_config
from dataplatform.pipelines.gold.ml_features import build_trip_delay_features, write_ml_features

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="staging")
    args = parser.parse_args()

    cfg = load_config(args.env)
    gold_config = load_gold_config()
    ml_config = load_ml_config()
    spark = SparkSession.builder.getOrCreate()

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

    schema = gold_config["schema"]
    features_df = build_trip_delay_features(spark, schema, ml_config["feature_lookback_hours"])
    write_ml_features(spark, schema, features_df)
