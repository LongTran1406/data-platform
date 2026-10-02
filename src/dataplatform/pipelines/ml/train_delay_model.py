import argparse
from pathlib import Path
import sys
import mlflow
from pyspark.sql import SparkSession
import os

try:
    _script_path = Path(__file__).resolve()
except NameError:
    _script_path = Path(sys.argv[0]).resolve()

PROJECT_ROOT = _script_path.parents[4]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dataplatform.config import load_config, load_gold_config, load_ml_config
from dataplatform.pipelines.gold.transform import require_unity_catalog
from dataplatform.pipelines.ml.train import load_training_data, train_and_register, promote_version

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

    catalog = require_unity_catalog(spark)
    mlflow.set_registry_uri("databricks-uc")

    model_schema = ml_config["model_schema"]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {model_schema}")

    train_df, test_df = load_training_data(spark, gold_config["schema"], ml_config["test_fraction"])
    version, _ = train_and_register(train_df, test_df, catalog, model_schema, ml_config["model_name"])

    full_name = f"{catalog}.{model_schema}.{ml_config['model_name']}"
    promote_version(full_name, version, ml_config["champion_alias"])
