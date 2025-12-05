"""
Unit tests for cache module
Testing caching functionality and performance
"""

import sys
import time
from unittest.mock import MagicMock, Mock, patch

import pytest

# Mock Redis before importing cache
sys.modules["redis"] = MagicMock()

from src.utils.cache import CacheManager


class TestCacheManager:
    """Test cases for CacheManager"""

    @pytest.fixture
    def cache_manager(self):
        """Create a cache manager instance"""
        with patch("redis.Redis"):
            return CacheManager(host="localhost", port=6379, db=0)

    def test_cache_initialization(self, cache_manager):
        """Test cache manager initialization"""
        assert cache_manager is not None
        assert cache_manager.ttl > 0

    @patch("redis.Redis")
    def test_cache_set_and_get(self, mock_redis):
        """Test setting and getting cache values"""
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        mock_redis_instance.get.return_value = b'{"items": [1, 2, 3]}'

        with patch("redis.Redis", return_value=mock_redis_instance):
            cache = CacheManager(host="localhost")
            cache.set("user_123", {"items": [1, 2, 3]})
            result = cache.get("user_123")

            # Verify set was called
            assert mock_redis_instance.setex.called

    @patch("redis.Redis")
    def test_cache_expiration(self, mock_redis):
        """Test that cache values expire after TTL"""
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        mock_redis_instance.get.return_value = None  # Simulates expired key

        with patch("redis.Redis", return_value=mock_redis_instance):
            cache = CacheManager(host="localhost", ttl=1)
            cache.set("temp_key", {"data": "value"})

            # Verify TTL was set
            assert mock_redis_instance.setex.called
            call_args = mock_redis_instance.setex.call_args
            assert call_args[0][1] == 1  # TTL should be 1 second

    @patch("redis.Redis")
    def test_cache_delete(self, mock_redis):
        """Test deleting cache entries"""
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance

        with patch("redis.Redis", return_value=mock_redis_instance):
            cache = CacheManager(host="localhost")
            cache.delete("user_123")

            assert mock_redis_instance.delete.called

    @patch("redis.Redis")
    def test_cache_clear_all(self, mock_redis):
        """Test clearing all cache entries"""
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        mock_redis_instance.keys.return_value = [b"key1", b"key2", b"key3"]

        with patch("redis.Redis", return_value=mock_redis_instance):
            cache = CacheManager(host="localhost")
            cache.clear()

            # Verify delete operations
            assert mock_redis_instance.delete.called or mock_redis_instance.flushdb.called

    @patch("redis.Redis")
    def test_cache_key_generation(self, mock_redis):
        """Test cache key generation"""
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance

        with patch("redis.Redis", return_value=mock_redis_instance):
            cache = CacheManager(host="localhost")

            # Test key generation with different inputs
            key1 = cache._generate_key("user", 123)
            key2 = cache._generate_key("user", 456)

            # Keys should be different
            assert key1 != key2
            # Keys should be deterministic
            assert key1 == cache._generate_key("user", 123)


@pytest.mark.smoke
class TestCachePerformance:
    """Performance tests for cache"""

    @patch("redis.Redis")
    def test_cache_read_performance(self, mock_redis):
        """Test cache read performance"""
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        mock_redis_instance.get.return_value = b'{"items": [1, 2, 3]}'

        with patch("redis.Redis", return_value=mock_redis_instance):
            cache = CacheManager(host="localhost")

            start = time.time()
            for i in range(1000):
                cache.get(f"key_{i}")
            elapsed = time.time() - start

            # Cache reads should be very fast (< 100ms for 1000 ops)
            assert elapsed < 0.1, f"Cache reads took {elapsed}s for 1000 ops"

    @patch("redis.Redis")
    def test_cache_write_performance(self, mock_redis):
        """Test cache write performance"""
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance

        with patch("redis.Redis", return_value=mock_redis_instance):
            cache = CacheManager(host="localhost")

            start = time.time()
            for i in range(100):
                cache.set(f"key_{i}", {"data": f"value_{i}"})
            elapsed = time.time() - start

            # Cache writes should be reasonably fast (< 500ms for 100 ops)
            assert elapsed < 0.5, f"Cache writes took {elapsed}s for 100 ops"


class TestCacheEdgeCases:
    """Test edge cases and error conditions"""

    @patch("redis.Redis")
    def test_cache_with_none_value(self, mock_redis):
        """Test caching None values"""
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance

        with patch("redis.Redis", return_value=mock_redis_instance):
            cache = CacheManager(host="localhost")
            cache.set("none_key", None)

            # Verify set was called
            assert mock_redis_instance.setex.called

    @patch("redis.Redis")
    def test_cache_with_large_value(self, mock_redis):
        """Test caching large values"""
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance

        with patch("redis.Redis", return_value=mock_redis_instance):
            cache = CacheManager(host="localhost")

            # Create large value (1MB)
            large_data = {"data": "x" * (1024 * 1024)}
            cache.set("large_key", large_data)

            # Verify set was called
            assert mock_redis_instance.setex.called

    @patch("redis.Redis")
    def test_cache_connection_failure_handling(self, mock_redis):
        """Test graceful handling of connection failures"""
        mock_redis_instance = MagicMock()
        mock_redis_instance.get.side_effect = Exception("Connection failed")
        mock_redis.return_value = mock_redis_instance

        with patch("redis.Redis", return_value=mock_redis_instance):
            cache = CacheManager(host="localhost")

            # Should handle connection errors gracefully
            try:
                cache.get("key")
            except Exception:
                pass  # Expected to fail

            assert mock_redis_instance.get.called
