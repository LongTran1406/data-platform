import argparse
from pathlib import Path
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import os

try:
    _script_path = Path(__file__).resolve()
except NameError:
    _script_path = Path(sys.argv[0]).resolve()

PROJECT_ROOT = _script_path.parents[4]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dataplatform.config import load_config, load_dq_rules
from dataplatform.pipelines.silver.transform import clean_transform_table


def check_not_null(df: DataFrame, columns: list[str]) -> dict:
    return {
        c: {"passed": df.filter(F.col(c).isNull()).count() == 0, "null_count": df.filter(F.col(c).isNull()).count()}
        for c in columns
    }


def check_unique(df: DataFrame, columns: list[str]) -> dict:
    total = df.count()
    distinct = df.select(*columns).distinct().count()
    return {"columns": columns, "passed": total == distinct, "duplicate_count": total - distinct}


def check_range(df: DataFrame, column: str, min_value, max_value) -> dict:
    numeric_col = F.col(column).cast("double")
    invalid_count = df.filter(~numeric_col.between(min_value, max_value)).count()
    return {"column": column, "passed": invalid_count == 0, "invalid_count": invalid_count}


def check_allowed_values(df: DataFrame, column: str, allowed_values: list) -> dict:
    numeric_col = F.col(column).cast("double")
    invalid_count = df.filter(~numeric_col.isin([float(v) for v in allowed_values])).count()
    return {"column": column, "passed": invalid_count == 0, "invalid_count": invalid_count}


def check_row_count(bronze_df: DataFrame, silver_df: DataFrame) -> dict:
    bronze_count = bronze_df.count()
    silver_count = silver_df.count()
    return {"bronze_count": bronze_count, "silver_count": silver_count, "passed": silver_count > 0}


def validate_table(rules: dict, silver_df: DataFrame, bronze_df: DataFrame = None) -> dict:
    results = {}

    if "not_null" in rules:
        results["not_null"] = check_not_null(silver_df, rules["not_null"])
    if "unique" in rules:
        results["unique"] = check_unique(silver_df, rules["unique"])
    if "ranges" in rules:
        results["ranges"] = {c: check_range(silver_df, c, lo, hi) for c, (lo, hi) in rules["ranges"].items()}
    if "allowed_values" in rules:
        results["allowed_values"] = {
            c: check_allowed_values(silver_df, c, v) for c, v in rules["allowed_values"].items()
        }
    if bronze_df is not None:
        results["row_count"] = check_row_count(bronze_df, silver_df)

    return results


if __name__ == "__main__":
    dq_rules = load_dq_rules()

    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="staging")
    parser.add_argument("--tables", nargs="+", default=list(dq_rules.keys()))
    args = parser.parse_args()

    cfg = load_config(args.env)
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

    for table_name in args.tables:
        print(f"=== {table_name} ===")
        try:
            rules = dq_rules[table_name]
            valid_df, bronze_df = clean_transform_table(spark, cfg, rules, table_name)
            results = validate_table(rules, silver_df=valid_df, bronze_df=bronze_df)
            print(results)
        except Exception as e:
            print(f"{table_name}: FAILED - {e}")