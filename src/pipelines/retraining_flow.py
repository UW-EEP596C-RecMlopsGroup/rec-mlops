"""
Prefect Flow for Recommendation Model Retraining (Phase 2 Updated)
"""

import structlog
from prefect import flow

from src.pipelines.tasks import task_register_and_promote  # <--- Added import
from src.pipelines.tasks import (
    task_evaluate_results,
    task_load_data,
    task_train_nmf,
    task_train_svd,
)

logger = structlog.get_logger()


@flow(name="Recommendation Model Retraining Flow", log_prints=True)
def retraining_flow(config_path: str = "config/config.yaml"):
    logger.info("🚀 Starting Retraining Flow...")

    # 1. Load Data
    user_item_matrix = task_load_data(config_path=config_path)

    # 2. Train Models
    # Note: we need the MLflow run_id to build the model_uri
    # The training tasks call mlflow.start_run internally,
    # so we assume each task returns a metrics dictionary that includes the run_id.
    # (We could tweak train_models.py or rely on the active MLflow run context,
    # but the simplest option is to return the run_id from every task.)

    # ⚠️ For simplicity we record the run_id inside task_train_svd/nmf,
    # though the most reliable approach is to include it in the task result at the flow layer.
    # We therefore assume task_train_* returns:
    # {'status': 'success', 'metrics': {...}, 'run_id': '...', 'artifact_path': '...'}

    logger.info("🤖 Training SVD Model...")
    svd_results = task_train_svd(user_item_matrix, config_path=config_path)

    logger.info("🤖 Training NMF Model...")
    nmf_results = task_train_nmf(user_item_matrix, config_path=config_path)

    # 3. Evaluate & Compare
    best_model_name = task_evaluate_results(svd_results, nmf_results)

    # 4. Register & Promote (new step)
    if best_model_name == "svd":
        best_run_info = svd_results
    else:
        best_run_info = nmf_results

    # Build model_uri: runs:/<run_id>/<artifact_path>
    # Note: train_models.py must return the run_id (see previous step)
    if "run_id" in best_run_info:
        run_id = best_run_info["run_id"]
        # train_models.py logs each model under f"{model_name}_model"
        artifact_path = f"{best_model_name}_model"
        model_uri = f"runs:/{run_id}/{artifact_path}"

        promotion_status = task_register_and_promote(
            best_model_name, best_run_info["metrics"], run_id, model_uri
        )
        logger.info(f"Model Promotion Status: {promotion_status}")
    else:
        logger.warning("Could not find run_id, skipping registration.")

    return best_model_name


if __name__ == "__main__":
    retraining_flow()
