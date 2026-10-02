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
from dataplatform.pipelines.gold.transform import require_unity_catalog, write_table
from dataplatform.pipelines.ml.predict import champion_model_exists, load_scoring_data, score

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

    if not champion_model_exists(catalog, model_schema, model_name, champion_alias):
        print(
            f"No '{champion_alias}' alias registered yet for {catalog}.{model_schema}.{model_name} "
            "- skipping scoring. Run train_delay_model first."
        )
    else:
        scoring_pdf = load_scoring_data(spark, gold_schema)
        if scoring_pdf.empty:
            print("No rows with complete features to score - skipping.")
        else:
            predictions_pdf = score(scoring_pdf, catalog, model_schema, model_name, champion_alias)

            predictions_df = spark.createDataFrame(predictions_pdf)
            write_table(spark, gold_schema, "predicted_delay", predictions_df, catalog)
