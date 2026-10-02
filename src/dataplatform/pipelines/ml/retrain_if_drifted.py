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
from dataplatform.pipelines.ml.drift import compute_drift, write_drift_metrics
from dataplatform.pipelines.ml.retrain import retrain_and_maybe_promote

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

    gold_schema = gold_config["schema"]
    model_schema = ml_config["model_schema"]
    model_name = ml_config["model_name"]
    champion_alias = ml_config["champion_alias"]

    drift_df = compute_drift(
        spark,
        catalog,
        gold_schema,
        model_schema,
        model_name,
        champion_alias,
        ml_config["drift_check_window_days"],
        ml_config["drift_threshold"],
    )

    drift_detected = bool(drift_df["drift_detected"].iloc[0])

    if not drift_detected:
        drift_df["retrained"] = False
        drift_df["challenger_version"] = None
        drift_df["challenger_accuracy"] = None
        drift_df["promoted"] = False
        print("No drift detected, skipping retrain.")
    else:
        print("Drift detected, retraining a challenger...")
        result = retrain_and_maybe_promote(
            spark, gold_schema, catalog, model_schema, model_name, champion_alias, ml_config["test_fraction"]
        )
        drift_df["retrained"] = True
        drift_df["challenger_version"] = result["challenger_version"]
        drift_df["challenger_accuracy"] = result["challenger_accuracy"]
        drift_df["promoted"] = result["promoted"]

    write_drift_metrics(spark, gold_schema, drift_df)
    print(drift_df.to_string(index=False))
