import mlflow
import pandas as pd
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

from dataplatform.pipelines.ml.train import FEATURE_COLUMNS

ID_COLUMNS = ["trip_id", "route_id", "stop_id", "stop_sequence", "service_date"]


def champion_model_exists(catalog: str, model_schema: str, model_name: str, champion_alias: str) -> bool:
    full_name = f"{catalog}.{model_schema}.{model_name}"
    try:
        MlflowClient().get_model_version_by_alias(full_name, champion_alias)
        return True
    except MlflowException:
        return False


def load_scoring_data(spark, gold_schema: str) -> pd.DataFrame:
    df = spark.table(f"{gold_schema}.ml_features_trip_delay")

    # Score whatever the latest snapshot is - not filtered on whether the
    # feed's own actual_delay_seconds is null, since that field is itself a
    # forecast for upcoming stops, not a clean "resolved vs pending" flag.
    latest_date = df.selectExpr("max(service_date) as d").first()["d"]
    df = df.filter(df["service_date"] == latest_date)

    # The model can't score missing inputs (e.g. a route's first stop in the
    # lookback window has no history yet), so those rows get no prediction.
    for column in FEATURE_COLUMNS:
        df = df.filter(df[column].isNotNull())

    return df.select(*ID_COLUMNS, *FEATURE_COLUMNS).toPandas()


def score(pdf: pd.DataFrame, catalog: str, model_schema: str, model_name: str, champion_alias: str) -> pd.DataFrame:
    full_name = f"{catalog}.{model_schema}.{model_name}"
    model = mlflow.sklearn.load_model(f"models:/{full_name}@{champion_alias}")

    model_version = MlflowClient().get_model_version_by_alias(full_name, champion_alias).version

    X = pdf[FEATURE_COLUMNS]
    result = pdf[ID_COLUMNS].copy()
    result["predicted_on_time"] = model.predict(X)
    result["predicted_probability"] = model.predict_proba(X)[:, 1]
    result["model_version"] = model_version
    result["scored_at"] = pd.Timestamp.utcnow()

    return result
