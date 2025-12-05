# 变更日志 (Changelog)

本项目的所有重要更改都将记录在此文件中。


## [Unreleased] - MLOps 转型阶段 1 & 2

### 🚀 主要特性 (Major Features)
将原本独立的推荐系统脚本改造为全容器化、可编排的生产级 MLOps 平台。

- **编排层 (Orchestration)**: 集成 **Prefect** 管理模型训练工作流，实现任务的可视化与自动化重试。
- **模型注册中心 (Model Registry)**: 集成 **MLflow Model Registry**，实现模型版本控制和生命周期管理（Staging/Production 阶段流转）。
- **持续交付 (CI/CD for Models)**: 实现了基于指标对比的“模型自动晋升”逻辑，以及 API 服务的“热加载”机制，达成零停机更新。
- **全容器化 (Containerization)**: 将 Spark 计算、API 服务、MLflow 追踪全部迁移至 Docker 容器内运行，彻底解决本地环境依赖问题。

### 🏗️ 基础设施变更 (Infrastructure)
- **`docker-compose.yml`**:
    - 新增 `prefect` 服务用于工作流编排。
    - 将 `kafka` 和 `zookeeper` 镜像源切换至 `bitnamilegacy`，修复了上游镜像策略变更导致的拉取失败问题。
    - 配置 `mlflow` 开启 **Artifact Serving** (代理模式) 并监听 `0.0.0.0`，解决了容器间文件写入权限和访问问题。
    - 更新所有服务的 Volume 挂载配置，将项目根目录映射到容器内的 `/app`，保证代码实时同步。

### 💻 代码修改详情 (Code Modifications)

#### 1. 流水线与编排 (Pipeline & Orchestration)
- **新增 `src/pipelines/tasks.py`**: 
    - 将原始训练逻辑拆解为原子的 Prefect 任务：`task_load_data`（数据加载）、`task_train_svd`（SVD训练）、`task_train_nmf`（NMF训练）。
    - 新增 `task_register_and_promote` 任务，实现了自动评估逻辑：仅当新模型 RMSE 优于当前 Production 模型时才执行晋升。
- **新增 `src/pipelines/retraining_flow.py`**: 
    - 定义了端到端的训练流水线逻辑：加载数据 -> 并行训练 -> 评估 -> 注册 -> 晋升。

#### 2. 模型训练 (`src/models/train_models.py`)
- **Delta Lake 支持**: 引入 `configure_spark_with_delta_pip` 并配置 Ivy 缓存路径 (`/tmp/.ivy2`)，解决了容器内 Spark 无法加载 Delta Lake 依赖的 `ClassNotFoundException`。
- **数据处理增强**: 在数据加载阶段增加了去重逻辑 (`drop_duplicates`)，修复了因样本数据重复导致矩阵转换失败的问题。
- **MLflow 集成**: 
    - 增加了 `infer_signature`，自动记录模型的输入/输出 Schema。
    - 重构了函数返回值，使其包含 `run_id`，以便下游任务进行模型注册。

#### 3. 服务引擎 (`src/models/recommendation_engine.py`)
- **Spark 配置同步**: 同步了训练脚本中的 Spark 配置，确保推理阶段也能正确读取 Delta Lake 表。
- **模型加载逻辑**: 
    - 修改为优先从 MLflow Registry 的 **"Production"** 阶段拉取模型。
    - 增加了 **Fallback (兜底)** 机制：如果未找到 Production 模型，自动触发本地训练以保证服务可用。
- **环境感知**: 优化了配置读取逻辑，优先使用 `MLFLOW_TRACKING_URI` 环境变量，解决了容器网络连接问题。

#### 4. API 服务 (`src/api/recommendation_api.py`)
- **热加载接口**: 新增 `POST /admin/reload-models` 端点。允许外部触发 API 重新从 MLflow 拉取最新的 Production 模型，无需重启容器。
- **启动健壮性**: 优化了 `lifespan` 启动逻辑，增加了对初始化失败的异常处理，防止因 MLflow 暂时不可用导致 API 崩溃。

#### 5. 工具脚本
- **新增 `src/init_delta_tables.py`**: 创建了容器原生的数据初始化脚本，避开了 Windows 本地 Hadoop/Java 环境配置的复杂性。

### 🐛 Bug 修复 (Bug Fixes)
- **SVD 维度错误**: 增加了样本数据中的物品数量（从 5 增加到 50），修复了因特征数少于主成分数 (`n_components=10`) 导致的 SVD 训练失败。
- **连接被拒绝**: 修正了容器间通信地址，将代码中默认的 `localhost` 全部替换为 Docker 服务名 (`mlflow`, `spark-master`)。
- **YAML 解析错误**: 修复了 `docker-compose.yml` 中多行命令的语法问题，确保 API 和 MLflow 正确监听 `0.0.0.0`。

## [Unreleased] - 第三阶段：质量保障与自动化测试 (QA & Automation)

### 🚀 主要特性 (Major Features)

本阶段聚焦于代码质量、自动化测试和持续集成/部署，确保系统的可靠性和可维护性。

- **负载测试框架**: 集成 **Locust** 框架进行高并发性能测试，验证推荐 API 在高负载下的性能（目标 p95 响应时间 < 100ms）。
- **代码质量保证**: 引入 **pre-commit 钩子**，强制执行代码格式化 (Black)、导入排序 (isort)、代码检查 (Flake8) 和类型检查 (MyPy)。
- **增强的测试覆盖**: 补充单元测试，提高测试覆盖率至 70%+，包括缓存、指标收集等关键模块。

### 🏗️ 基础设施变更 (Infrastructure)

- **Docker Compose 扩展**:
    - 新增 `locust-master` 服务，用于容器化负载测试。
    - 配置 Locust 与 API 服务的网络连接，允许并发用户模拟。

- **预提交钩子配置** (`.pre-commit-config.yaml`):
    - Black: 代码格式化 (line-length=100)
    - isort: 导入排序（profile=black）
    - Flake8: 代码风格检查（max-line-length=100）
    - MyPy: Python 类型检查
    - Bandit: 安全性检查（排除测试代码）
    - pydocstyle: 文档字符串验证

- **项目配置文件** (`pyproject.toml`, `.flake8`):
    - 集中管理所有工具配置，避免多个配置文件混乱。
    - Black、isort、MyPy、pytest 等工具的统一配置。

### 💻 代码修改详情 (Code Modifications)

#### 1. 负载测试 (`tests/locustfile.py`)
- **创建 Locust 测试脚本**:
    - 定义 `RecommendationUser` 类模拟真实用户行为。
    - 实现三个主要任务：
      - `get_recommendations()`: 获取推荐（权重 3）
      - `get_recommendations_with_filters()`: 带过滤条件的推荐（权重 1）
      - `check_health()`: 健康检查（权重 1）
    - 配置性能指标收集：响应时间、成功率、p95 延迟等。
    - 自动生成测试报告，包括：
      - 成功率、失败率统计
      - 响应时间百分位数 (p50, p75, p90, p95, p99)
      - 吞吐量 (RPS)
      - 性能目标评估（p95 < 100ms, 成功率 > 99.5%）

#### 2. Docker 编排 (`docker-compose.yml`)
- **Locust 容器集成**:
    - Master 模式配置，监听 8089 端口
    - 自动安装 Locust 和 requests 依赖
    - 与 Spark Master 容器网络连接

#### 3. Makefile 增强 (`Makefile`)
- **新增命令**:
    - `make lint`: 运行 Flake8 和 Bandit 检查
    - `make format`: 代码格式化 (Black + isort)
    - `make type-check`: MyPy 类型检查
    - `make pre-commit`: 运行所有 pre-commit 钩子
    - `make pre-commit-install`: 安装 pre-commit 钩子
    - `make test-unit`: 仅运行单元测试
    - `make test-integration`: 仅运行集成测试
    - `make test-smoke`: 运行冒烟测试
    - `make load-test`: 启动 Locust Web UI 进行交互式测试
    - `make load-test-headless`: 无头模式负载测试（5分钟，100用户）
    - `make ci`: 完整 CI 流水线 (lint + type-check + test)
    - `make ci-full`: 完整 CI + 负载测试

#### 4. 测试增强 (`tests/unit/test_cache.py`)
- **CacheManager 单元测试**:
    - 初始化测试
    - Set/Get 操作测试
    - TTL 过期测试
    - 删除和清空测试
    - 键生成测试
    - **性能测试**: 读写性能验证 (< 100ms for 1000 ops)
    - **边界情况**: None 值、大值、连接失败处理

#### 5. 测试配置 (`tests/conftest.py`)
- **测试标记注册**:
    - `@pytest.mark.smoke`: 冒烟测试
    - `@pytest.mark.unit`: 单元测试
    - `@pytest.mark.integration`: 集成测试
    - `@pytest.mark.slow`: 慢速测试
    - `@pytest.mark.performance`: 性能测试

- **依赖 Mocking**:
    - 预 mock 重型依赖 (PySpark, MLflow, Redis, Kafka) 以加快测试启动

### 📊 性能目标 (Performance Targets)

| 指标 | 目标 | 说明 |
|------|------|------|
| 响应时间 p95 | < 100ms | 95% 的请求应在 100ms 内完成 |
| 成功率 | > 99.5% | 生产级可用性 |
| 代码覆盖率 | ≥ 70% | 单元测试覆盖关键模块 |
| 吞吐量 | 100+ RPS | 支持 100 并发用户 |

### 🐛 已知问题与后续工作 (Known Issues & Future Work)

- **Docker Compose Locust 配置**: 目前使用 master 模式，可扩展至 worker 模式以支持分布式负载测试
- **CI/CD 部署**: 需配置实际的 Kubernetes manifests 或其他部署目标
- **测试数据库**: 集成测试使用临时 PostgreSQL/Redis 容器，可优化为使用 testcontainers
- **性能基准**: 应在实际生产环境中验证性能目标

---
