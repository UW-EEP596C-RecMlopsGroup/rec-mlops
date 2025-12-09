"""
Prometheus metrics for the recommendation API.
Lightweight exports to prevent duplicate registration.
"""

from prometheus_client import (
    Histogram,
    Gauge,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST,
    Counter,
)

# Use an isolated registry to prevent duplicate registration
_registry = CollectorRegistry()

# Request counter
request_count = Counter(
    'recommendation_api_requests_total',
    'Total API requests',
    ['endpoint', 'method', 'status'],
    registry=_registry
)

# Response time
response_time = Histogram(
    'recommendation_api_response_time_seconds',
    'Response time in seconds',
    ['endpoint'],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5),
    registry=_registry
)

# Recommendation generation tracker
recommendations_total = Counter(
    'recommendation_api_recommendations_total',
    'Total recommendations generated',
    ['algorithm', 'status'],
    registry=_registry
)

# Concurrent requests
inflight_requests = Gauge(
    'recommendation_api_inflight_requests',
    'Concurrent in-flight requests',
    ['endpoint'],
    registry=_registry
)

# Distribution of recommendation counts
recommendation_batch_size = Histogram(
    'recommendation_api_recommendation_count',
    'Number of recommendations returned per request',
    ['algorithm'],
    buckets=(1, 5, 10, 20, 30, 50),
    registry=_registry
)

# Model load status
models_loaded = Gauge(
    'recommendation_api_models_loaded',
    'Number of loaded models',
    registry=_registry
)

# Cache hit ratio
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
    """Return metrics formatted for Prometheus."""
    return generate_latest(_registry)
