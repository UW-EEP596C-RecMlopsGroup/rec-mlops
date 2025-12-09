# Changelog

All significant changes to this project are documented here.


## [Unreleased] - MLOps Transformation Phases 1 & 2

### 🚀 Major Features
Converted the standalone recommendation scripts into a fully containerized, orchestrated, production-grade MLOps platform.

- **Orchestration layer**: Integrated **Prefect** to manage model-training workflows, providing visualization and automatic retries.
- **Model registry**: Added **MLflow Model Registry** so we can version-control models and manage their lifecycle across Staging and Production stages.
- **Model CI/CD**: Implemented metric-based "auto-promotion" plus API hot reload, enabling zero-downtime updates.
- **Containerization**: Moved Spark, the API, and MLflow tracking into Docker containers to eliminate local dependency drift.

### 🏗️ Infrastructure Changes
- **`docker-compose.yml`**:
    - Added a `prefect` service for workflow orchestration.
    - Swapped the `kafka` and `zookeeper` images to `bitnamilegacy` to recover from upstream pull-policy changes.
    - Configured `mlflow` with artifact serving (proxy mode) bound to `0.0.0.0`, fixing cross-container permissions and access issues.
    - Updated every volume mount so the repo root maps to `/app` inside each container for instant code sync.

### 💻 Code Modifications

#### 1. Pipeline & Orchestration
- **New `src/pipelines/tasks.py`**:
    - Broke the original training logic into atomic Prefect tasks: `task_load_data`, `task_train_svd`, and `task_train_nmf`.
    - Added `task_register_and_promote`, which promotes a model only when its RMSE beats the current Production version.
- **New `src/pipelines/retraining_flow.py`**:
    - Describes the end-to-end flow: load data → train in parallel → evaluate → register → promote.

#### 2. Model Training (`src/models/train_models.py`)
- **Delta Lake support**: Introduced `configure_spark_with_delta_pip` and set the Ivy cache to `/tmp/.ivy2`, solving the container `ClassNotFoundException` for Delta dependencies.
- **Better data handling**: Added `drop_duplicates` during loading to avoid pivot failures caused by duplicate samples.
- **MLflow integration**:
    - Uses `infer_signature` to capture model input/output schema.
    - Refactored return values so each training call provides a `run_id` for downstream registration.

#### 3. Serving Engine (`src/models/recommendation_engine.py`)
- **Spark configuration parity**: Mirrored the training Spark settings so inference can read the same Delta tables.
- **Model loading**:
    - Prefer loading from the MLflow Registry `Production` stage.
    - Added a fallback path that retrains locally if no Production model exists.
- **Environment awareness**: Prioritizes the `MLFLOW_TRACKING_URI` env var to fix container networking issues.

#### 4. API Service (`src/api/recommendation_api.py`)
- **Hot reload endpoint**: Added `POST /admin/reload-models` so the API can pull the latest Production model without a restart.
- **Startup resilience**: Hardened the `lifespan` logic with better exception handling when MLflow is temporarily unavailable.

#### 5. Utility Scripts
- **New `src/init_delta_tables.py`**: Provides a container-friendly data initializer so we can avoid local Windows Hadoop/Java setup pain.

### 🐛 Bug Fixes
- **SVD dimension mismatch**: Increased the sample dataset item count (5 → 50) to avoid failing when `n_components=10` exceeds available features.
- **Connection refused**: Replaced `localhost` with Docker service names (`mlflow`, `spark-master`) for inter-container traffic.
- **YAML parsing**: Fixed multi-line commands in `docker-compose.yml` so API and MLflow bind to `0.0.0.0` correctly.

## [Unreleased] - Phase 3: Quality Assurance & Automation

### 🚀 Major Features

This phase focuses on code quality, automated testing, and CI/CD to keep the platform reliable and maintainable.

- **Load testing**: Integrated **Locust** for high-concurrency benchmarking; target p95 latency < 100 ms for the recommendation API.
- **Code quality enforcement**: Added **pre-commit hooks** to mandate Black, isort, Flake8, and MyPy before commits land.
- **Stronger test coverage**: Added unit tests for cache, metrics, and other critical modules to reach 70%+ coverage.

### 🏗️ Infrastructure Changes

- **Docker Compose additions**:
    - Added a `locust-master` service for containerized load testing.
    - Wired Locust into the API network so concurrent user simulations work end-to-end.

- **Pre-commit configuration** (`.pre-commit-config.yaml`):
    - Black: formatting (line-length 100)
    - isort: import sorting (profile=black)
    - Flake8: linting (max-line-length 100)
    - MyPy: static typing
    - Bandit: security scanning (test modules excluded)
    - pydocstyle: docstring validation

- **Project config files** (`pyproject.toml`, `.flake8`):
    - Centralize all tool settings to avoid scattered configs.
    - Provide a single source for Black, isort, MyPy, pytest, and more.

### 💻 Code Modifications

#### 1. Load Testing (`tests/locustfile.py`)
- **Locust user script**:
    - Adds `RecommendationUser` to emulate real clients.
    - Implements three weighted tasks:
      - `get_recommendations()` weight 3
      - `get_recommendations_with_filters()` weight 1
      - `check_health()` weight 1
    - Captures latency, success rate, and p95 metrics.
    - Auto-generates a report with:
      - Success/failure counts
      - Latency percentiles (p50, p75, p90, p95, p99)
      - Throughput (RPS)
      - Goal evaluation (p95 < 100 ms, success > 99.5%)

#### 2. Docker Orchestration (`docker-compose.yml`)
- **Locust integration**:
    - Runs in master mode on port 8089
    - Installs Locust + requests on container startup
    - Shares the network with Spark Master for API access

#### 3. Makefile Enhancements (`Makefile`)
- **New targets**:
    - `make lint`: Flake8 + Bandit
    - `make format`: Black + isort
    - `make type-check`: MyPy
    - `make pre-commit`: run every hook
    - `make pre-commit-install`: install the hooks
    - `make test-unit`: unit tests only
    - `make test-integration`: integration tests only
    - `make test-smoke`: smoke tests
    - `make load-test`: start the Locust UI
    - `make load-test-headless`: headless Locust run (5 min, 100 users)
    - `make ci`: lint + type-check + unit tests
    - `make ci-full`: CI plus load testing

#### 4. Test Enhancements (`tests/unit/test_cache.py`)
- **CacheManager unit tests** cover initialization, get/set, TTL expiry, delete/clear, key generation, and more.
- **Performance check**: validates 1,000 ops finish in < 100 ms.
- **Edge cases**: handles `None` values, large payloads, and simulated connection failures.

#### 5. Test Configuration (`tests/conftest.py`)
- **Pytest markers**:
    - `@pytest.mark.smoke`
    - `@pytest.mark.unit`
    - `@pytest.mark.integration`
    - `@pytest.mark.slow`
    - `@pytest.mark.performance`

- **Dependency mocking**: Pre-mocks heavyweight services (PySpark, MLflow, Redis, Kafka) to keep test startup fast.

### 📊 Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Response time p95 | < 100 ms | 95% of requests should finish within 100 ms |
| Success rate | > 99.5% | Production-grade availability |
| Code coverage | ≥ 70% | Unit tests cover critical modules |
| Throughput | 100+ RPS | Sustains 100 concurrent users |

### 🐛 Known Issues & Future Work

- **Docker Compose Locust topology**: Currently master-only; add workers for distributed load.
- **CI/CD deployment**: Still need concrete Kubernetes manifests or another target environment.
- **Test databases**: Integration tests rely on temporary Postgres/Redis containers; consider testcontainers.
- **Performance baselining**: Validate targets in a production-like environment.

---
