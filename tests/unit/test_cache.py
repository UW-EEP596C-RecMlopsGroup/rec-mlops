"""
Unit tests for cache module
Testing caching functionality and performance
"""

import sys
from unittest.mock import MagicMock

import pytest

# Mock Redis before importing cache
sys.modules["redis"] = MagicMock()

from src.utils.cache import CacheManager, JSON_PREFIX  # noqa: E402


@pytest.fixture
def cache_builder():
    """Helper to create cache managers with injected Redis clients."""

    def _build(**options):
        mock_client = MagicMock()
        manager = CacheManager({"host": "localhost", "port": 6379, "db": 0}, client=mock_client, **options)
        return manager, mock_client

    return _build


class TestCacheManager:
    """Test cases for CacheManager"""

    def test_cache_initialization(self, cache_builder):
        cache, _ = cache_builder()
        assert cache is not None
        assert cache.ttl > 0

    def test_cache_set_and_get(self, cache_builder):
        cache, mock_client = cache_builder()
        mock_client.get.return_value = JSON_PREFIX + b'{"items": [1, 2, 3]}'

        cache.set("user_123", {"items": [1, 2, 3]})
        result = cache.get("user_123")

        assert mock_client.setex.called
        mock_client.get.assert_called_with("user_123")
        assert result == {"items": [1, 2, 3]}

    def test_cache_expiration(self, cache_builder):
        cache, mock_client = cache_builder(ttl=1)
        cache.set("temp_key", {"data": "value"})

        assert mock_client.setex.called
        call_args = mock_client.setex.call_args
        assert call_args[0][1] == 1

    def test_cache_delete(self, cache_builder):
        cache, mock_client = cache_builder()
        cache.delete("user_123")

        mock_client.delete.assert_called_with("user_123")

    def test_cache_clear_all(self, cache_builder):
        cache, mock_client = cache_builder()
        mock_client.keys.return_value = [b"key1", b"key2", b"key3"]

        cache.clear()

        assert mock_client.delete.called or mock_client.flushdb.called

    def test_cache_key_generation(self, cache_builder):
        cache, _ = cache_builder()
        key1 = cache._generate_key("user", 123)
        key2 = cache._generate_key("user", 456)

        assert key1 != key2
        assert key1 == cache._generate_key("user", 123)


class TestCacheEdgeCases:
    """Test edge cases and error conditions"""

    def test_cache_with_none_value(self, cache_builder):
        cache, mock_client = cache_builder()
        cache.set("none_key", None)

        assert mock_client.setex.called

    def test_cache_with_large_value(self, cache_builder):
        cache, mock_client = cache_builder()
        large_data = {"data": "x" * (1024 * 1024)}
        cache.set("large_key", large_data)

        assert mock_client.setex.called

    def test_cache_connection_failure_handling(self, cache_builder):
        cache, mock_client = cache_builder()
        mock_client.get.side_effect = Exception("Connection failed")

        with pytest.raises(Exception):
            cache.get("key")

        assert mock_client.get.called
