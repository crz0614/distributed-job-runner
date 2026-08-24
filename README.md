# Distributed Job Runner

[![CI](https://github.com/crz0614/distributed-job-runner/actions/workflows/ci.yml/badge.svg)](https://github.com/crz0614/distributed-job-runner/actions/workflows/ci.yml)

A dependency-free Go service for bounded concurrent execution. It demonstrates worker pools, backpressure, idempotent submission, per-attempt deadlines, exponential retry delay, cancellation, graceful shutdown and operational metrics.

## Run

```bash
go test -race ./...
go run ./cmd/server
```

Submit a job:

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'content-type: application/json' \
  -d '{"id":"demo-1","kind":"collect","payload":{"url":"https://example.com"}}'
```

Use the same ID twice to observe idempotency. Set `payload.simulate` to `failure` to exercise retry and terminal failure behavior.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Idempotently enqueue work |
| `GET` | `/jobs` | List current jobs |
| `GET` | `/jobs/{id}` | Inspect status and attempts |
| `DELETE` | `/jobs/{id}` | Cancel queued/running work |
| `GET` | `/metrics` | Queue and outcome counters |
| `GET` | `/healthz` | Readiness probe |

## PostgreSQL persistence foundation

A versioned PostgreSQL schema now defines durable `jobs` and `job_attempts` records, status constraints, idempotent job IDs, attempt history, indexes and automatic `updated_at` maintenance.

```bash
docker compose up -d postgres
docker compose exec postgres psql -U runner -d runner -c '\dt'
```

CI applies the migration to a real PostgreSQL 17 service and verifies inserts, JSONB payloads, attempt records and status updates. The current Go runner still uses its in-memory store; wiring the runtime to this schema is the next implementation milestone and is intentionally not claimed as complete.

## 中文说明

这是一个纯 Go 高并发任务执行服务，展示工作池、队列背压、幂等提交、超时控制、自动重试、任务取消、优雅退出和指标监控。它适用于网页采集、API 集成、自动化任务及数据管道执行器。

仓库现已加入可实际启动的 PostgreSQL 17 服务及版本化数据结构，覆盖任务、执行尝试、JSONB 载荷、状态约束、索引和更新时间维护。CI 会连接真实 PostgreSQL 执行迁移与数据断言。当前 Go 运行时仍使用内存存储，下一阶段才会接入数据库，因此 README 不会把尚未完成的持久化接线描述成生产能力。

## Architecture

```mermaid
flowchart LR
  A["HTTP API"] --> B["Bounded queue"]
  B --> C["Worker pool"]
  C --> D["Timeout + retry"]
  D --> E["Job state"]
  E --> F["Metrics API"]
```

The in-memory store keeps the project easy to evaluate. The PostgreSQL schema and CI-backed migration provide the durable storage contract; the application adapter remains pending.

## Safety

- Request bodies are capped at 1 MiB.
- The queue is bounded and returns `queue full` instead of consuming unlimited memory.
- Attempt deadlines prevent stuck upstream calls.
- Shutdown is bounded by a context deadline.
- The repository contains no credentials or real workload payloads.
