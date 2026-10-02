import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient

from dataplatform.pipelines.ml.train import FEATURE_COLUMNS, LABEL_COLUMN, evaluate_accuracy


def compute_drift(
    spark,
    catalog: str,
    gold_schema: str,
    model_schema: str,
    model_name: str,
    champion_alias: str,
    window_days: int,
    drift_threshold: float,
) -> pd.DataFrame:
    full_name = f"{catalog}.{model_schema}.{model_name}"
    client = MlflowClient()

    model_version = client.get_model_version_by_alias(full_name, champion_alias)
    baseline_accuracy = client.get_run(model_version.run_id).data.metrics["accuracy"]

    # Trailing window of FULLY COMPLETED days only, excluding today - unlike
    # predict_delay's "latest snapshot", drift-checking needs settled outcomes,
    # not a still-active trip's in-flight forecast.
    df = spark.table(f"{gold_schema}.ml_features_trip_delay")
    df = df.filter(
        (df["service_date"] >= spark.sql(f"SELECT date_sub(current_date(), {window_days})").first()[0])
        & (df["service_date"] < spark.sql("SELECT current_date()").first()[0])
    )
    for column in [*FEATURE_COLUMNS, LABEL_COLUMN]:
        df = df.filter(df[column].isNotNull())

    pdf = df.select(*FEATURE_COLUMNS, LABEL_COLUMN).toPandas()

    model = mlflow.sklearn.load_model(f"models:/{full_name}@{champion_alias}")
    current_accuracy = evaluate_accuracy(model, pdf) if len(pdf) > 0 else None

    accuracy_drop = (baseline_accuracy - current_accuracy) if current_accuracy is not None else None
    drift_detected = (accuracy_drop is not None) and (accuracy_drop > drift_threshold)

    return pd.DataFrame([{
        "checked_at": pd.Timestamp.utcnow(),
        "champion_version": model_version.version,
        "baseline_accuracy": baseline_accuracy,
        "current_accuracy": current_accuracy,
        "accuracy_drop": accuracy_drop,
        "drift_detected": drift_detected,
        "rows_evaluated": len(pdf),
    }])


def write_drift_metrics(spark, gold_schema: str, df: pd.DataFrame):
    spark.createDataFrame(df).write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(f"{gold_schema}.model_drift_metrics")
