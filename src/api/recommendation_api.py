"""Real-Time Recommendation API with caching helpers for unit tests."""

import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Literal, Optional

import structlog
import uvicorn
import yaml
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, Response
from pydantic import BaseModel, Field

from ..models.recommendation_engine import RecommendationEngine
from ..utils.cache import CacheManager

# 尝试导入prometheus metrics，如果不可用则忽略
try:
    from ..utils.prometheus_metrics import (
        request_count,
        response_time,
        recommendations_total,
        models_loaded,
        inflight_requests,
        recommendation_batch_size,
        get_metrics,
        PROMETHEUS_CONTENT_TYPE,
    )
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False
    PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

logger = structlog.get_logger()

# Load configuration
config_path = "config/config.yaml"
if os.path.exists(config_path):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
else:
    # Default config for container execution
    config = {
        "api": {"host": "0.0.0.0", "port": 8000, "cache_ttl": 300},
        "mlflow": {
            "tracking_uri": "http://mlflow:5000",
            "experiment_name": "recommendation_engine",
        },
        "database": {"redis": {"host": "redis", "port": 6379, "db": 0}},
        "streaming": {"spark": {"app_name": "RecEngine"}, "kafka": {}},
    }


# Global state trackers
SERVER_START_TIME = time.time()
AlgorithmLiteral = Literal["svd", "nmf", "hybrid"]


# Models
class RecommendationRequest(BaseModel):
    user_id: int
    num_recommendations: int = Field(default=10, ge=1, le=50)
    exclude_seen: bool = True
    algorithm: AlgorithmLiteral = "hybrid"


class RecommendationResponse(BaseModel):
    user_id: int
    recommendations: List[Dict[str, Any]]
    algorithm_used: str
    response_time_ms: float
    cache_hit: bool


class HealthResponse(BaseModel):
    status: str
    active_models: List[str]
    uptime_seconds: float


class InteractionRequest(BaseModel):
    user_id: int
    item_id: int
    rating: float = Field(ge=0, le=5)
    interaction_type: str = Field(default="event")


# Global state
recommendation_engine = None
_engine_lock = asyncio.Lock()
cache_settings = config.get("database", {}).get("redis", {}).copy()
cache_settings.setdefault("ttl", config.get("api", {}).get("cache_ttl", 300))
cache_manager = CacheManager(cache_settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""

    logger.info("Starting Recommendation API...")
    await _ensure_engine_initialized()

    yield

    logger.info("Shutting down...")


app = FastAPI(title="Recommendation API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def _ensure_engine_initialized() -> RecommendationEngine:
    """Lazily initialize the recommendation engine for environments without startup hooks."""

    global recommendation_engine

    if recommendation_engine is not None:
        return recommendation_engine

    async with _engine_lock:
        if recommendation_engine is None:
            try:
                recommendation_engine = RecommendationEngine(config)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("Failed to construct recommendation engine", exc_info=True)
                raise HTTPException(status_code=503, detail="Engine initialization failed") from exc

            try:
                await recommendation_engine.load_models()
                logger.info("Models loaded successfully")
            except Exception as exc:  # pragma: no cover - best-effort logging
                logger.error(f"Error loading models during startup: {exc}")

    return recommendation_engine


# ============================================================
# Metrics helpers
# ============================================================
def _record_request_metrics(
    *,
    endpoint: str,
    method: str,
    status_code: int,
    elapsed_seconds: float,
    algorithm: Optional[str] = None,
    outcome: str = "success",
    recommendation_count: Optional[int] = None,
):
    """Safely record Prometheus metrics for API calls."""
    if not HAS_PROMETHEUS:
        return

    safe_elapsed = max(elapsed_seconds, 0.0)
    request_count.labels(
        endpoint=endpoint,
        method=method,
        status=str(status_code),
    ).inc()
    response_time.labels(endpoint=endpoint).observe(safe_elapsed)

    if algorithm:
        recommendations_total.labels(
            algorithm=algorithm,
            status=outcome,
        ).inc()
        if recommendation_count is not None and recommendation_count > 0:
            recommendation_batch_size.labels(algorithm=algorithm).observe(
                recommendation_count
            )


@app.get("/metrics")
def metrics_endpoint():
    """Expose Prometheus metrics or a minimal uptime metric if unavailable."""
    if HAS_PROMETHEUS:
        # PlainTextResponse avoids duplicating charset values in the header
        return PlainTextResponse(content=get_metrics())

    uptime = _uptime_seconds()
    body = f"rec_api_uptime_seconds {uptime:.2f}\n"
    return PlainTextResponse(content=body)


async def get_recommendation_engine() -> RecommendationEngine:
    return await _ensure_engine_initialized()


def _cache_key(user_id: int, algorithm: str, num_recs: int, exclude_seen: bool) -> str:
    return f"rec:{user_id}:{algorithm}:{num_recs}:{int(exclude_seen)}"


def _uptime_seconds() -> float:
    return time.time() - SERVER_START_TIME


async def _build_recommendation_response(
    request: RecommendationRequest, engine: RecommendationEngine
) -> RecommendationResponse:
    cache_hit = False
    cache_key = _cache_key(
        request.user_id, request.algorithm, request.num_recommendations, request.exclude_seen
    )

    cached = cache_manager.get(cache_key) if cache_manager else None
    if cached is not None:
        cache_hit = True
        recommendations = cached
    else:
        try:
            recommendations = await engine.get_recommendations(
                user_id=request.user_id,
                num_recommendations=request.num_recommendations,
                algorithm=request.algorithm,
                exclude_seen=request.exclude_seen,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("Recommendation generation failed", exc_info=True)
            raise HTTPException(status_code=500, detail="Internal server error") from exc

        if cache_manager:
            cache_manager.set(cache_key, recommendations, cache_settings.get("ttl"))

    return RecommendationResponse(
        user_id=request.user_id,
        recommendations=recommendations,
        algorithm_used=request.algorithm,
        response_time_ms=0.0,
        cache_hit=cache_hit,
    )


@app.get("/health", response_model=HealthResponse)
async def health_check(engine: RecommendationEngine = Depends(get_recommendation_engine)):
    active_models = await engine.get_active_models()
    if HAS_PROMETHEUS:
        models_loaded.set(len(active_models))
    return HealthResponse(
        status="healthy",
        active_models=active_models,
        uptime_seconds=_uptime_seconds(),
    )


async def _handle_recommendation_request(
    request: RecommendationRequest,
    engine: RecommendationEngine,
    *,
    endpoint_label: str,
    method: str,
) -> RecommendationResponse:
    """Execute recommendation flow with consistent metrics tracking."""

    start_time = time.time()
    inflight_label = None
    if HAS_PROMETHEUS:
        inflight_label = inflight_requests.labels(endpoint=endpoint_label)
        inflight_label.inc()

    try:
        response = await _build_recommendation_response(request, engine)
    except HTTPException as http_exc:
        elapsed = time.time() - start_time
        _record_request_metrics(
            endpoint=endpoint_label,
            method=method,
            status_code=http_exc.status_code,
            elapsed_seconds=elapsed,
            algorithm=request.algorithm,
            outcome="error",
        )
        raise
    except Exception as exc:
        elapsed = time.time() - start_time
        _record_request_metrics(
            endpoint=endpoint_label,
            method=method,
            status_code=500,
            elapsed_seconds=elapsed,
            algorithm=request.algorithm,
            outcome="error",
        )
        raise HTTPException(status_code=500, detail="Failed to generate recommendations") from exc
    finally:
        if inflight_label is not None:
            inflight_label.dec()

    elapsed = time.time() - start_time
    _record_request_metrics(
        endpoint=endpoint_label,
        method=method,
        status_code=200,
        elapsed_seconds=elapsed,
        algorithm=request.algorithm,
        outcome="success",
        recommendation_count=len(response.recommendations),
    )

    response.response_time_ms = elapsed * 1000
    return response


@app.post("/recommendations", response_model=RecommendationResponse)
async def get_recommendations(
    request: RecommendationRequest,
    engine: RecommendationEngine = Depends(get_recommendation_engine),
):
    return await _handle_recommendation_request(
        request,
        engine,
        endpoint_label="/recommendations",
        method="POST",
    )


@app.get("/users/{user_id}/recommendations", response_model=RecommendationResponse)
async def get_user_recommendations(
    user_id: int,
    num_recommendations: int = Query(10, ge=1, le=50),
    algorithm: AlgorithmLiteral = Query("hybrid"),
    exclude_seen: bool = Query(True),
    engine: RecommendationEngine = Depends(get_recommendation_engine),
):
    request = RecommendationRequest(
        user_id=user_id,
        num_recommendations=num_recommendations,
        algorithm=algorithm,
        exclude_seen=exclude_seen,
    )
    return await _handle_recommendation_request(
        request,
        engine,
        endpoint_label="/users/{user_id}/recommendations",
        method="GET",
    )


@app.post("/interactions")
async def record_interaction(
    interaction: InteractionRequest, engine: RecommendationEngine = Depends(get_recommendation_engine)
):
    try:
        await engine.record_interaction(interaction.dict())
    except Exception as exc:
        logger.error("Failed to record interaction", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to record interaction") from exc

    return {"status": "success", "message": "Interaction recorded"}


@app.get("/stats")
async def get_system_stats(engine: RecommendationEngine = Depends(get_recommendation_engine)):
    stats = await engine.get_model_stats()
    return {"status": "success", "stats": stats, "uptime_seconds": _uptime_seconds()}


@app.post("/models/retrain")
async def trigger_model_retrain(engine: RecommendationEngine = Depends(get_recommendation_engine)):
    try:
        await engine.retrain_models()
    except Exception as exc:
        logger.error("Model retraining failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Retraining failed") from exc

    return {"status": "success", "message": "Model retraining started"}


# --- 新增：Admin Endpoint 实现热加载 ---
@app.post("/admin/reload-models")
async def reload_models(
    background_tasks: BackgroundTasks,
    engine: RecommendationEngine = Depends(get_recommendation_engine),
):
    """
    Force reload models from MLflow Registry (Production stage).
    This allows updating the model without restarting the container.
    """
    try:
        logger.info("Received request to reload models...")
        # Await the reload (this might take a few seconds)
        await engine.load_models()

        # Get new status
        stats = await engine.get_model_stats()
        return {
            "status": "success",
            "message": "Models reloaded from Production",
            "current_state": stats,
        }
    except Exception as e:
        logger.error(f"Failed to reload models: {e}")
        raise HTTPException(status_code=500, detail=f"Reload failed: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
