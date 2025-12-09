"""
Prometheus metrics for recommendation API
轻量级metrics导出，避免重复注册
"""

from prometheus_client import (
    Histogram,
    Gauge,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST,
    Counter,
)

# 创建独立的registry以避免重复注册问题
_registry = CollectorRegistry()

# 请求计数器
request_count = Counter(
    'recommendation_api_requests_total',
    'Total API requests',
    ['endpoint', 'method', 'status'],
    registry=_registry
)

# 响应时间
response_time = Histogram(
    'recommendation_api_response_time_seconds',
    'Response time in seconds',
    ['endpoint'],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5),
    registry=_registry
)

# 推荐生成
recommendations_total = Counter(
    'recommendation_api_recommendations_total',
    'Total recommendations generated',
    ['algorithm', 'status'],
    registry=_registry
)

# 并发请求
inflight_requests = Gauge(
    'recommendation_api_inflight_requests',
    'Concurrent in-flight requests',
    ['endpoint'],
    registry=_registry
)

# 推荐数量分布
recommendation_batch_size = Histogram(
    'recommendation_api_recommendation_count',
    'Number of recommendations returned per request',
    ['algorithm'],
    buckets=(1, 5, 10, 20, 30, 50),
    registry=_registry
)

# 模型加载状态
models_loaded = Gauge(
    'recommendation_api_models_loaded',
    'Number of loaded models',
    registry=_registry
)

# 缓存命中率
cache_hits = Counter(
    'recommendation_api_cache_hits_total',
    'Cache hits',
    registry=_registry
)

cache_misses = Counter(
    'recommendation_api_cache_misses_total',
    'Cache misses',
    registry=_registry
)


PROMETHEUS_CONTENT_TYPE = CONTENT_TYPE_LATEST


def get_metrics() -> bytes:
    """返回Prometheus格式的metrics"""
    return generate_latest(_registry)
