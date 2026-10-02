import mlflow

from dataplatform.pipelines.ml.train import (
    evaluate_accuracy,
    load_training_data,
    promote_version,
    train_and_register,
)


def retrain_and_maybe_promote(
    spark,
    gold_schema: str,
    catalog: str,
    model_schema: str,
    model_name: str,
    champion_alias: str,
    test_fraction: float,
) -> dict:
    full_name = f"{catalog}.{model_schema}.{model_name}"

    train_df, test_df = load_training_data(spark, gold_schema, test_fraction)
    challenger_version, _ = train_and_register(train_df, test_df, catalog, model_schema, model_name)

    # Evaluate both models on the SAME held-out split (the challenger's own
    # time-ordered test split) - a fair comparison, distinct from compute_drift's trailing
    # window, which answers a different question (has the live model decayed).
    champion_model = mlflow.sklearn.load_model(f"models:/{full_name}@{champion_alias}")
    challenger_model = mlflow.sklearn.load_model(f"models:/{full_name}/{challenger_version}")

    champion_accuracy = evaluate_accuracy(champion_model, test_df)
    challenger_accuracy = evaluate_accuracy(challenger_model, test_df)

    promoted = challenger_accuracy > champion_accuracy
    if promoted:
        promote_version(full_name, challenger_version, champion_alias)
    else:
        print(
            f"Challenger v{challenger_version} ({challenger_accuracy:.4f}) did not beat "
            f"champion ({champion_accuracy:.4f}) - champion unchanged"
        )

    return {
        "challenger_version": challenger_version,
        "champion_accuracy": champion_accuracy,
        "challenger_accuracy": challenger_accuracy,
        "promoted": promoted,
    }
