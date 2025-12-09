"""
Prefect Flow for Recommendation Model Retraining (Phase 2 Updated)
"""

import structlog
from prefect import flow

from src.pipelines.tasks import task_register_and_promote  # <--- 新增导入
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
    # 注意：我们需要获取 mlflow run_id 来构建 model_uri
    # 这里我们在 tasks 内部使用了 mlflow.start_run，
    # 为了拿到 run_id，我们假设 train task 返回的 metrics 字典里包含了 run_id
    # (这需要微调 train_models.py，或者我们利用 MLflow 的 active run 上下文，
    # 但最简单的方法是让 task 返回 run_id)

    # ⚠️ 为了简化，我们直接在 task_train_svd/nmf 内部做记录，
    # 但在 Flow 层获取 Run ID 最稳妥的方式是在 Task 返回值里带上。
    # 让我们假设 task_train_* 返回结构为:
    # {'status': 'success', 'metrics': {...}, 'run_id': '...', 'artifact_path': '...'}

    logger.info("🤖 Training SVD Model...")
    svd_results = task_train_svd(user_item_matrix, config_path=config_path)

    logger.info("🤖 Training NMF Model...")
    nmf_results = task_train_nmf(user_item_matrix, config_path=config_path)

    # 3. Evaluate & Compare
    best_model_name = task_evaluate_results(svd_results, nmf_results)

    # 4. Register & Promote (新增步骤)
    if best_model_name == "svd":
        best_run_info = svd_results
    else:
        best_run_info = nmf_results

    # 构建 model_uri: runs:/<run_id>/<artifact_path>
    # 注意：我们需要修改 train_models.py 让其返回 run_id (见下一步)
    if "run_id" in best_run_info:
        run_id = best_run_info["run_id"]
        # train_models.py 里 log_model 的名字是 f"{model_name}_model"
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
