import mlflow
import pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

FEATURE_COLUMNS = [
    "hour_of_day", "day_of_week", "route_avg_delay_recent", "route_pct_on_time_recent",
]
LABEL_COLUMN = "on_time"

# Columns that must be non-null for a row to be usable - no history yet
# (rolling features) or no resolved outcome yet (label) means the row can't
# be trained on or evaluated against.
_REQUIRED_COLUMNS = [*FEATURE_COLUMNS, "actual_delay_seconds", LABEL_COLUMN]


def load_training_data(spark, gold_schema: str, test_fraction: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = spark.table(f"{gold_schema}.ml_features_trip_delay")

    for column in _REQUIRED_COLUMNS:
        df = df.filter(df[column].isNotNull())

    pdf = df.toPandas()
    if len(pdf) < 2:
        raise ValueError(
            f"Only {len(pdf)} usable rows in {gold_schema}.ml_features_trip_delay - "
            "need more data (or a longer feature_lookback_hours) before training."
        )

    # Split by time, not randomly - the latest test_fraction of rows become
    # the test set, so evaluation is always on stops later than any trained on.
    pdf = pdf.sort_values("event_epoch").reset_index(drop=True)
    n_test = max(1, int(len(pdf) * test_fraction))
    train_df = pdf.iloc[:-n_test]
    test_df = pdf.iloc[-n_test:]

    return train_df, test_df


def evaluate_accuracy(model, pdf: pd.DataFrame) -> float:
    return accuracy_score(pdf[LABEL_COLUMN], model.predict(pdf[FEATURE_COLUMNS]))


def train_and_register(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    catalog: str,
    model_schema: str,
    model_name: str,
) -> tuple[str, float]:
    X_train, y_train = train_df[FEATURE_COLUMNS], train_df[LABEL_COLUMN]
    X_test, y_test = test_df[FEATURE_COLUMNS], test_df[LABEL_COLUMN]

    # A job run (spark_python_task) has no notebook context to infer an
    # experiment from, unlike an attached notebook run - set one explicitly.
    mlflow.set_experiment(f"/Users/tranthelong1406@gmail.com/{model_name}")

    with mlflow.start_run():
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)

        accuracy = accuracy_score(y_test, model.predict(X_test))

        mlflow.log_param("model_type", "RandomForestClassifier")
        mlflow.log_param("n_estimators", 100)
        mlflow.log_param("train_rows", len(X_train))
        mlflow.log_param("test_rows", len(X_test))
        mlflow.log_metric("accuracy", accuracy)

        signature = infer_signature(X_train, model.predict(X_train))
        full_name = f"{catalog}.{model_schema}.{model_name}"
        model_info = mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            signature=signature,
            registered_model_name=full_name,
            # Trained in this same run, so its tree storage is safe to trust.
            skops_trusted_types=["sklearn.tree._tree.Tree"],
        )

        print(f"Train rows: {len(X_train)}, test rows: {len(X_test)}, accuracy: {accuracy:.4f}")

    version = model_info.registered_model_version
    print(f"Registered {full_name} v{version} (not yet promoted)")

    return version, accuracy


def promote_version(full_name: str, version: str, champion_alias: str):
    MlflowClient().set_registered_model_alias(full_name, champion_alias, version)
    print(f"Promoted {full_name} v{version} to '{champion_alias}'")
