"""Caching utilities used by the recommendation API and engines."""

import hashlib
import importlib
import json
import pickle
import threading
import time
from typing import Any, Dict, List, Optional

import structlog

try:  # pragma: no cover - redis is optional in tests
    import redis  # type: ignore
except Exception:  # pragma: no cover - CI fallback
    redis = None


logger = structlog.get_logger()
JSON_PREFIX = b"J"
PICKLE_PREFIX = b"P"


class _InMemoryBackend:
    """Simple in-memory fallback used when Redis is unavailable."""

    def __init__(self):
        self._store: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def set(self, key: str, value: bytes, ttl: Optional[int]) -> None:
        expire_at = time.time() + ttl if ttl else None
        with self._lock:
            self._store[key] = (value, expire_at)

    def get(self, key: str) -> Optional[bytes]:
        with self._lock:
            payload = self._store.get(key)
            if not payload:
                return None
            value, expire_at = payload
            if expire_at and expire_at < time.time():
                self._store.pop(key, None)
                return None
            return value

    def delete(self, key: str) -> bool:
        with self._lock:
            return self._store.pop(key, None) is not None

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def keys(self) -> List[str]:
        with self._lock:
            return list(self._store.keys())


class CacheManager:
    """Synchronous cache wrapper with Redis and in-memory backends."""

    def __init__(
        self,
        redis_config: Optional[Dict[str, Any]] = None,
        *,
        client: Optional[Any] = None,
        **kwargs: Any,
    ):
        config = dict(redis_config or {})
        config.update(kwargs)

        self.host = config.get("host", "localhost")
        self.port = config.get("port", 6379)
        self.db = config.get("db", 0)
        self.password = config.get("password")
        self.ttl = config.get("ttl", 300)
        self.namespace = config.get("namespace", "rec")

        self.client = client or self._create_client()
        self._fallback = _InMemoryBackend()

    def _get_redis_module(self):
        """Load redis module lazily so tests can inject mocks."""

        global redis
        if redis is not None:
            return redis
        try:
            redis = importlib.import_module("redis")
            return redis
        except Exception as exc:  # pragma: no cover - optional dependency
            logger.warning("Redis module unavailable, using in-memory cache: %s", exc)
            return None

    def _create_client(self):
        redis_module = self._get_redis_module()
        if not redis_module:
            return None

        try:
            return redis_module.Redis(host=self.host, port=self.port, db=self.db, password=self.password)
        except Exception as exc:  # pragma: no cover
            logger.warning("Falling back to in-memory cache: %s", exc)
            return None

    def _serialize(self, value: Any) -> bytes:
        try:
            return JSON_PREFIX + json.dumps(value).encode("utf-8")
        except (TypeError, ValueError):
            return PICKLE_PREFIX + pickle.dumps(value)

    def _deserialize(self, value: bytes) -> Any:
        if not value:
            return None
        prefix, payload = value[:1], value[1:]
        if prefix == JSON_PREFIX:
            return json.loads(payload.decode("utf-8"))
        if prefix == PICKLE_PREFIX:
            return pickle.loads(payload)
        return payload

    def _store(self, key: str, payload: bytes, ttl: Optional[int]) -> None:
        if self.client:
            if ttl:
                self.client.setex(key, ttl, payload)
            else:
                self.client.set(key, payload)
            return
        self._fallback.set(key, payload, ttl)

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        payload = self._serialize(value)
        self._store(key, payload, ttl or self.ttl)
        return True

    def get(self, key: str, default: Any = None) -> Any:
        if self.client:
            raw = self.client.get(key)
        else:
            raw = self._fallback.get(key)
        if raw is None:
            return default
        return self._deserialize(raw)

    def delete(self, key: str) -> bool:
        if self.client:
            return bool(self.client.delete(key))
        return self._fallback.delete(key)

    def clear(self) -> None:
        if self.client:
            try:
                keys = self.client.keys("*")
                if keys:
                    self.client.delete(*keys)
                else:
                    self.client.flushdb()
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to clear Redis cache: %s", exc)
        self._fallback.clear()

    def exists(self, key: str) -> bool:
        if self.client:
            return bool(self.client.exists(key))
        return self._fallback.get(key) is not None

    def increment(self, key: str, amount: int = 1) -> int:
        if self.client:
            return int(self.client.incrby(key, amount))

        raw = self._fallback.get(key)
        current = int(self._deserialize(raw)) if raw else 0
        current += amount
        self._store(key, JSON_PREFIX + json.dumps(current).encode("utf-8"), None)
        return current

    def get_ttl(self, key: str) -> Optional[int]:
        if self.client:
            ttl = self.client.ttl(key)
            return ttl if ttl >= 0 else None
        # In-memory backend uses per-key expirations, so approximate
        return None

    def get_stats(self) -> Dict[str, Any]:
        if self.client:
            info = self.client.info()
            hits = info.get("keyspace_hits", 0)
            misses = info.get("keyspace_misses", 0)
            total = hits + misses
            return {
                "connected_clients": info.get("connected_clients", 0),
                "used_memory": info.get("used_memory", 0),
                "hit_rate": hits / total if total else 0.0,
            }

        return {
            "connected_clients": 1,
            "used_memory": len(self._fallback.keys()),
            "hit_rate": 1.0,
        }

    def close(self) -> None:
        if self.client and hasattr(self.client, "close"):
            try:
                self.client.close()
            except Exception:  # pragma: no cover
                pass

    def _generate_key(self, namespace: str, identifier: Any) -> str:
        raw = f"{namespace}:{identifier}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class RecommendationCache:
    """Domain-specific helper built on top of CacheManager."""

    def __init__(self, cache_manager: CacheManager):
        self.cache = cache_manager
        self.default_ttl = cache_manager.ttl or 1800

    def _user_rec_key(self, user_id: int, algorithm: str, count: int) -> str:
        return f"rec:{user_id}:{algorithm}:{count}"

    def get_recommendations(self, user_id: int, algorithm: str, num_recommendations: int):
        key = self._user_rec_key(user_id, algorithm, num_recommendations)
        return self.cache.get(key)

    def set_recommendations(
        self,
        user_id: int,
        algorithm: str,
        num_recommendations: int,
        recommendations: List[Dict[str, Any]],
        ttl: Optional[int] = None,
    ) -> bool:
        key = self._user_rec_key(user_id, algorithm, num_recommendations)
        return self.cache.set(key, recommendations, ttl or self.default_ttl)


def cache_performance_monitor(func):
    """Decorator that logs slow cache operations."""

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        duration_ms = (time.time() - start_time) * 1000
        if duration_ms > 10:
            logger.warning("Slow cache op %s took %.2f ms", func.__name__, duration_ms)
        return result

    return wrapper


def example_usage():  # pragma: no cover - documentation helper
    cache = CacheManager({"ttl": 10})
    cache.set("demo", {"items": [1, 2, 3]})
    print(cache.get("demo"))
    cache.clear()


if __name__ == "__main__":  # pragma: no cover
    example_usage()
