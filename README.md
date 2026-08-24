# Distributed Job Runner

[![CI](https://github.com/crz0614/distributed-job-runner/actions/workflows/ci.yml/badge.svg)](https://github.com/crz0614/distributed-job-runner/actions/workflows/ci.yml)

A Go service for durable, bounded concurrent execution. It combines worker pools, backpressure, idempotent submission, per-attempt deadlines, exponential retry delay, cancellation, graceful shutdown, PostgreSQL recovery and operational metrics.

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

## Durable PostgreSQL mode

A versioned PostgreSQL schema now defines durable `jobs` and `job_attempts` records, status constraints, idempotent job IDs, attempt history, indexes and automatic `updated_at` maintenance.

```bash
docker compose up --build
curl http://localhost:8080/healthz
```

The Compose stack starts PostgreSQL 17, applies the versioned schema and gives the runner a `DATABASE_URL`. Jobs, JSONB payloads, status changes and per-attempt outcomes are persisted. On restart, queued jobs and jobs interrupted while running are recovered and executed again; terminal jobs are not replayed. Duplicate job IDs return the original record instead of creating duplicate work.

Without `DATABASE_URL`, the service deliberately falls back to an in-memory store for local evaluation and logs that the mode is non-durable. `/healthz` reports the active storage mode and returns `503` if PostgreSQL becomes unavailable.

CI applies the migration to a real PostgreSQL service, runs the store integration tests, then verifies the Go race tests, vet and production build.

## 中文说明

这是一个纯 Go 高并发任务执行服务，展示工作池、队列背压、幂等提交、超时控制、自动重试、任务取消、优雅退出和指标监控。它适用于网页采集、API 集成、自动化任务及数据管道执行器。

仓库已将 Go 运行时接入 PostgreSQL 17：任务、JSONB 载荷、状态变化及每次执行结果都会持久化；服务重启后会恢复排队中及意外中断的任务，不会重放已成功、失败或取消的终态任务。同一任务 ID 重复提交时返回原记录，避免重复执行。未配置 `DATABASE_URL` 时才会明确降级为仅供本地评估的内存模式。

## Architecture

```mermaid
flowchart LR
  A["HTTP API"] --> B["Bounded queue"]
  B --> C["Worker pool"]
  C --> D["Timeout + retry"]
  D --> E["Job state"]
  E --> F["Metrics API"]
```

The Store boundary keeps the execution engine independent from persistence. Production mode uses PostgreSQL; the in-memory adapter remains available for hermetic tests and local evaluation.

## Safety

- Request bodies are capped at 1 MiB.
- The queue is bounded and returns `queue full` instead of consuming unlimited memory.
- Attempt deadlines prevent stuck upstream calls.
- Shutdown is bounded by a context deadline.
- The repository contains no credentials or real workload payloads.
