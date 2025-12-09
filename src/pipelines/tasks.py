"""
Prefect Tasks for Recommendation Engine Pipeline
Encapsulates model training logic from src.models.train_models into discrete tasks.
"""

from typing import Any, Dict

import mlflow
import numpy as np
import structlog
from mlflow.tracking import MlflowClient
from prefect import task

from src.models.train_models import ModelTrainer

# 初始化 structlog，保持与原项目一致的日志风格
logger = structlog.get_logger()


@task(name="Load Training Data", retries=3, retry_delay_seconds=10, log_prints=True)
def task_load_data(config_path: str = "config/config.yaml") -> np.ndarray:
    """
    Load interaction data and create user-item matrix.
    Uses Spark via ModelTrainer to handle data loading.
    """
    logger.info("Initializing ModelTrainer for data loading...")
    # 初始化 Trainer 以复用其数据加载逻辑 (Spark Session 等)
    trainer = ModelTrainer(config_path=config_path)

    logger.info("Loading training data...")
    # load_training_data 返回 (interactions_pd, user_item_matrix.values)
    # 我们这里主要需要矩阵用于训练
    _, user_item_matrix = trainer.load_training_data()

    row_count, col_count = user_item_matrix.shape
    logger.info(f"Data loaded successfully. Matrix shape: {row_count} users x {col_count} items")

    return user_item_matrix


@task(name="Train SVD Model", log_prints=True)
def task_train_svd(
    user_item_matrix: np.ndarray, config_path: str = "config/config.yaml"
) -> Dict[str, Any]:
    """
    Train SVD model using the provided user-item matrix.
    Returns dictionary with metrics, run_id, and status.
    """
    logger.info("Starting SVD training task...")
    trainer = ModelTrainer(config_path=config_path)

    try:
        # train_models.py 现在返回的是一个字典:
        # {'status': 'success', 'metrics': {...}, 'run_id': '...', 'model_type': 'svd'}
        result = trainer.train_svd_model(user_item_matrix)

        rmse = result.get("metrics", {}).get("rmse", "N/A")
        logger.info(f"SVD Training completed. RMSE: {rmse}")

        return result

    except Exception as e:
        logger.error(f"SVD Training failed: {str(e)}")
        return {"model_type": "svd", "status": "failed", "error": str(e)}


@task(name="Train NMF Model", log_prints=True)
def task_train_nmf(
    user_item_matrix: np.ndarray, config_path: str = "config/config.yaml"
) -> Dict[str, Any]:
    """
    Train NMF model using the provided user-item matrix.
    Returns dictionary with metrics, run_id, and status.
    """
    logger.info("Starting NMF training task...")
    trainer = ModelTrainer(config_path=config_path)

    try:
        # train_models.py 现在返回的是一个字典
        result = trainer.train_nmf_model(user_item_matrix)

        rmse = result.get("metrics", {}).get("rmse", "N/A")
        logger.info(f"NMF Training completed. RMSE: {rmse}")

        return result

    except Exception as e:
        logger.error(f"NMF Training failed: {str(e)}")
        return {"model_type": "nmf", "status": "failed", "error": str(e)}


@task(name="Evaluate & Compare Models", log_prints=True)
def task_evaluate_results(svd_result: Dict[str, Any], nmf_result: Dict[str, Any]) -> str:
    """
    Compare training results and determine the best model.
    """
    logger.info("Evaluating training results...")

    results = {}
    if svd_result["status"] == "success":
        results["svd"] = svd_result

    if nmf_result["status"] == "success":
        results["nmf"] = nmf_result

    if not results:
        raise ValueError("All model training tasks failed.")

    # 简单的比较逻辑：优先比较 RMSE
    best_model = None
    best_rmse = float("inf")

    for name, res in results.items():
        rmse = res["metrics"].get("rmse", float("inf"))
        logger.info(f"Model {name} RMSE: {rmse}")

        if rmse < best_rmse:
            best_rmse = rmse
            best_model = name

    logger.info(f"🏆 Best model determined: {best_model} with RMSE: {best_rmse}")
    return best_model


@task(name="Register & Promote Model", log_prints=True)
def task_register_and_promote(
    best_model_name: str, metrics: Dict[str, float], run_id: str, model_uri: str
) -> str:
    """
    Register the best model and promote to Production if it beats the current one.
    """
    # 定义注册表中的模型名称 (标准化命名)
    reg_model_name = f"Recommendation_{best_model_name.upper()}"
    logger.info(f"🚀 Registering model: {reg_model_name} from run {run_id}")

    client = MlflowClient()

    # 1. 注册模型版本
    # model_uri 格式通常为: runs:/<run_id>/<artifact_path>
    model_version = mlflow.register_model(model_uri, reg_model_name)
    logger.info(f"Registered version: {model_version.version}")

    # 2. 获取当前 Production 模型的指标 (如果有)
    promote_to_prod = False
    try:
        latest_prod = client.get_latest_versions(reg_model_name, stages=["Production"])
        if not latest_prod:
            logger.info("No Production model found. Promoting current model immediately.")
            promote_to_prod = True
        else:
            prod_version = latest_prod[0]
            prod_run_id = prod_version.run_id
            prod_run = client.get_run(prod_run_id)
            prod_rmse = prod_run.data.metrics.get("rmse", float("inf"))

            new_rmse = metrics.get("rmse", float("inf"))

            logger.info(f"Current Prod RMSE: {prod_rmse:.4f} vs New Model RMSE: {new_rmse:.4f}")

            if new_rmse < prod_rmse:
                logger.info("🎉 New model is better! Promoting to Production.")
                promote_to_prod = True
            else:
                logger.info("New model is not better. Keeping in Staging.")
                client.transition_model_version_stage(
                    name=reg_model_name, version=model_version.version, stage="Staging"
                )
    except Exception as e:
        logger.warning(f"Error comparing models: {e}. Defaulting to promotion.")
        promote_to_prod = True

    # 3. 执行晋升
    if promote_to_prod:
        client.transition_model_version_stage(
            name=reg_model_name,
            version=model_version.version,
            stage="Production",
            archive_existing_versions=True,
        )
        return "promoted"

    return "staging"
