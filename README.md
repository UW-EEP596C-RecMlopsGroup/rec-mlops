# 项目快速启动指南

以下命令请**严格按顺序逐条复制执行**（已测试可直接运行）。

## 操作步骤

1. **启动所有服务**

    ```bash
    # 1. 启动所有服务
    docker-compose up -d
    ```

2. **获取 spark-master 容器 ID**

    ```bash
    # 2. 获取 spark-master 容器 ID
    CONTAINER=$(docker-compose ps -q spark-master)
    ```

3. **在容器内安装基础依赖**

    ```bash
    # 3. 在容器内安装基础依赖
    docker exec -it $CONTAINER pip install \
        prefect \
        mlflow \
        structlog \
        pandas \
        scikit-learn \
        pyyaml \
        delta-spark==2.4.0 \
        kafka-python==2.0.2
    ```

4. **安装 Prefect 2.x 版本（必须 <3.0）**

    ```bash
    # 4. 安装 Prefect 2.x 版本（必须 <3.0）
    docker exec -it $CONTAINER pip install "prefect>=2.0.0,<3.0.0"
    ```

5. **初始化 Delta Lake 表结构**

    ```bash
    # 5. 初始化 Delta Lake 表结构（在 MinIO 中创建所需表）
    docker exec -w /app -it $CONTAINER python src/init_delta_tables.py
    ```

6. **运行模型重训练 Flow（关键步骤）**

    ```bash
    # 6. 运行模型重训练 Flow（关键步骤）
    docker exec \
      -e PREFECT_HOME=/tmp/.prefect \
      -e MLFLOW_TRACKING_URI=http://mlflow:5000 \
      -e PREFECT_API_URL=http://prefect:4200/api \
      -w /app \
      -it $CONTAINER python -m src.pipelines.retraining_flow
    ```

## 预期结果

- Prefect UI（http://localhost:4200）显示 Flow 运行成功（绿色）
- MLflow UI（http://localhost:5000） → Models 页面出现注册模型：Recommendation_SVD，最新版本被标记为 Production

## 模型热加载验证

```bash
# 7. 测试模型热加载 API（验证 Production 模型是否成功加载到推荐服务）
docker exec -w /app -it $CONTAINER python -c \
  "import requests; print(requests.post('http://localhost:8000/admin/reload-models').json())"
```

预期返回（示例）：

```json
{
  "status": "success",
  "message": "Models reloaded from Production",
  "current_state": {
    "Recommendation_SVD": {
      "version": "1",
      "stage": "Production",
      "loaded_at": "2025-12-04T08:88:88.888888"
    }
  }
}
```

## 开发和贡献

### Phase 3: 质量保障与自动化测试

**一键安装开发工具：**

```bash
make install-dev
```

自动创建虚拟环境、安装所有工具、配置 Git 钩子。

**激活虚拟环境：**

```bash
source venv_py313/bin/activate
```

**代码质量检查（推荐用于日常开发）：**

```bash
make ci              # lint + type-check 代码质量检查
make format          # 自动格式化代码 (Black + isort)
make lint            # Flake8 + Bandit 检查
make type-check      # MyPy 类型检查
make pre-commit      # 所有预提交钩子
```

**负载测试：**

```bash
make load-test              # 启动 Locust UI (http://localhost:8089)
make load-test-headless     # 无界面测试 (5min, 100 users)
```

**单元测试（需要完整依赖环境）：**

```bash
make ci-test         # 完整 CI + 单元测试
make test            # 仅单元测试
make test-smoke      # 冒烟测试
```

### Phase 3 工具

| 工具 | 用途 | 配置文件 |
|------|------|--------|
| Black | 代码格式化 | pyproject.toml |
| isort | 导入排序 | pyproject.toml |
| Flake8 | 代码检查 | .flake8 |
| MyPy | 类型检查 | pyproject.toml |
| Bandit | 安全检查 | pyproject.toml |
| pytest | 单元测试 | pyproject.toml |
| pre-commit | Git 钩子 | .pre-commit-config.yaml |
| Locust | 负载测试 | tests/locustfile.py |


