"""
Locust load testing script for Recommendation API
Tests the /recommendations endpoint under concurrent load
Performance targets: response time < 100ms (p95)
"""

import random
import time
from datetime import datetime

from locust import HttpUser, between, events, task


class RecommendationUser(HttpUser):
    """Simulates a user making recommendation requests"""

    wait_time = between(1, 3)  # Wait 1-3 seconds between requests

    def on_start(self):
        """Called when a user starts"""
        self.user_ids = list(range(1, 101))  # Simulate 100 different users
        self.performance_metrics = {
            "requests": 0,
            "failures": 0,
            "slow_requests": 0,  # response time > 100ms
            "response_times": [],
        }

    @task(3)
    def get_recommendations(self):
        """Task: Get recommendations (weighted 3)"""
        user_id = random.choice(self.user_ids)

        payload = {
            "user_id": user_id,
            "num_recommendations": 10,
            "exclude_seen": True,
            "algorithm": random.choice(["svd", "nmf", "hybrid"]),
        }

        start_time = time.time()

        with self.client.post("/recommendations", json=payload, catch_response=True) as response:
            elapsed_time = (time.time() - start_time) * 1000  # Convert to ms

            self.performance_metrics["requests"] += 1
            self.performance_metrics["response_times"].append(elapsed_time)

            if elapsed_time > 100:
                self.performance_metrics["slow_requests"] += 1

            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
                self.performance_metrics["failures"] += 1

    @task(1)
    def get_recommendations_with_filters(self):
        """Task: Get recommendations with filters (weighted 1)"""
        user_id = random.choice(self.user_ids)

        payload = {
            "user_id": user_id,
            "num_recommendations": 20,
            "exclude_seen": True,
            "algorithm": "hybrid",
        }

        start_time = time.time()

        with self.client.post("/recommendations", json=payload, catch_response=True) as response:
            elapsed_time = (time.time() - start_time) * 1000

            self.performance_metrics["requests"] += 1
            self.performance_metrics["response_times"].append(elapsed_time)

            if elapsed_time > 100:
                self.performance_metrics["slow_requests"] += 1

            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
                self.performance_metrics["failures"] += 1

    @task(1)
    def check_health(self):
        """Task: Check API health (weighted 1)"""
        with self.client.get("/health", catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Health check failed: {response.status_code}")


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Called when load test starts"""
    print("\n" + "=" * 60)
    print(f"Load test started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print(f"Target URL: {environment.host}")
    print(f"Target response time: < 100ms (p95)")
    print("=" * 60 + "\n")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Called when load test stops - generate final report"""
    print("\n" + "=" * 60)
    print("Load Test Summary Report")
    print("=" * 60)

    stats = environment.stats

    # Overall statistics
    total_requests = stats.total.num_requests
    total_failures = stats.total.num_failures
    total_success = total_requests - total_failures

    print(f"\nTotal Requests: {total_requests}")
    print(f"Successful: {total_success} ({total_success/total_requests*100:.1f}%)")
    print(f"Failed: {total_failures} ({total_failures/total_requests*100:.1f}%)")

    # Response time statistics
    print(f"\nResponse Time Statistics (ms):")
    print(f"  Min: {stats.total.min_response_time:.2f}")
    print(f"  Max: {stats.total.max_response_time:.2f}")
    print(f"  Mean: {stats.total.avg_response_time:.2f}")
    print(f"  Median: {stats.total.median_response_time:.2f}")

    # Percentile response times
    print(f"\nPercentile Response Times (ms):")
    for percentile in [50, 75, 90, 95, 99]:
        resp_time = stats.total.get_response_time_percentile(percentile / 100)
        print(f"  p{percentile}: {resp_time:.2f}")

    # Check performance targets
    p95_response_time = stats.total.get_response_time_percentile(0.95)
    success_rate = (total_success / total_requests * 100) if total_requests > 0 else 0

    print("\n" + "=" * 60)
    print("Performance Assessment:")
    print("=" * 60)

    target_p95 = 100  # ms
    if p95_response_time <= target_p95:
        print(
            f"✅ Response Time Target Met: p95={p95_response_time:.2f}ms (target: <{target_p95}ms)"
        )
    else:
        print(
            f"❌ Response Time Target NOT Met: p95={p95_response_time:.2f}ms (target: <{target_p95}ms)"
        )

    target_success_rate = 99.5
    if success_rate >= target_success_rate:
        print(f"✅ Success Rate Target Met: {success_rate:.2f}% (target: >{target_success_rate}%)")
    else:
        print(
            f"❌ Success Rate Target NOT Met: {success_rate:.2f}% (target: >{target_success_rate}%)"
        )

    # RPS statistics
    print("\nThroughput:")
    print(f"  Requests/sec: {stats.total.total_rps:.2f}")

    print("=" * 60 + "\n")


@events.request.add_listener
def log_request(request_type, name, response_time, response_length, response, context, **kwargs):
    """Log individual requests for debugging"""
    # Only log errors or very slow requests
    if response and response.status_code != 200 or response_time > 150:
        print(
            f"[{request_type}] {name}: {response_time:.2f}ms, Status: {response.status_code if response else 'N/A'}"
        )
