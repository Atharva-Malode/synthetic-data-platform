# Synthetic Data Platform

An implementation plan and working Go starter for a deterministic, JSON-driven synthetic data platform. The current slice generates correlated users and activity events, validates bounded specifications, tracks jobs in memory, and writes CSV, JSONL, and ZIP artifacts.

Kafka, PostgreSQL, Redis, Parquet, and agent-assisted planning remain planned phases. The specifications describe how those pieces should be added without claiming that they are implemented yet.

## Current Go implementation

```text
JSON specification
      |
      v
HTTP API (:8080)
      |
      +--> validation and row estimate
      +--> deterministic seeded generator
      +--> in-memory job store
      +--> CSV users and activity_events
      +--> JSONL event stream and optional ZIP
```

The implementation uses only the Go standard library. User and event IDs are stable hashes of the seed, entity type, and row index. Activity rows always reference a generated user ID, and a one-million-row limit prevents accidental oversized jobs.

Run it with:

```sh
go run . --port 8080
```

Available endpoints:

```text
GET  /api/health
POST /api/specifications/validate
POST /api/jobs/estimate
POST /api/jobs
GET  /api/jobs/{jobId}
POST /api/jobs/{jobId}/cancel
GET  /api/jobs/{jobId}/exports
```

Submit the example specification from the specification document to `/api/jobs`. Generated files are written below `data/{jobId}`. Jobs are intentionally in memory for this first slice, so a process restart loses job status but not already-written files.

## MCP server

The same generator can run as a local MCP server over stdio. MCP clients start the binary with the `--mcp` flag; no HTTP server is required in this mode.

```sh
go run . --mcp
```

Exposed MCP tools:

- `validate_generation_spec`
- `estimate_generation`
- `create_generation_job`
- `get_generation_status`
- `cancel_generation_job`

Example client configuration:

```json
{
      "mcpServers": {
            "synthetic-data": {
                  "command": "go",
                  "args": ["run", ".", "--mcp"],
                  "cwd": "/Users/naman/Desktop/go"
            }
      }
}
```

For a deployed binary, replace `go run .` with the compiled executable path. MCP requests are newline-delimited JSON-RPC messages on stdin and responses are written to stdout.

## Why This Project

Synthetic data is most useful when it preserves relationships and behavior rather than producing unrelated random columns. This platform treats generation as a reproducible data pipeline:

1. A user submits a versioned JSON specification.
2. Validation resolves entities, expressions, relationships, limits, and assumptions.
3. An estimate reports rows, storage, event volume, and execution risk.
4. The user confirms the plan when required.
5. A seeded generator creates parent entities before children.
6. Events flow through Kafka while PostgreSQL remains the durable source of truth.
7. Redis tracks short-lived coordination state such as locks and progress.
8. Export workers produce CSV, Parquet, and ZIP artifacts.

## Architecture

```text
CLI or Web UI
      |
      v
Orchestrator API (.NET)
      |
      +--> Specification Validator
      +--> Relationship Graph Builder
      +--> Size and Cost Estimator
      +--> Job Coordinator
                    |
                    v
             Generation Worker
              /       |       \
             v        v        v
          Kafka    PostgreSQL  Export Worker
             |        ^        /        \
             v        |       v          v
       Ingestion  Consumer  CSV       Parquet
       Consumer             +----------+
             |
             +------> PostgreSQL

Redis: job status, progress, locks, idempotency keys, and short-lived agent state
ZIP: CSV, Parquet, schema, manifest, checksums, and validation report
```

### Component responsibilities

- **Orchestrator API:** accepts specifications, validates plans, estimates cost, and manages job state transitions.
- **Generator:** creates deterministic records in dependency order and preserves foreign-key relationships.
- **Kafka:** transports entity events through a durable, replayable boundary.
- **PostgreSQL:** stores canonical job state and durable relational data with constraints and indexes.
- **Redis:** provides short-lived locks, progress updates, cancellation signals, and idempotency coordination.
- **Export worker:** streams bounded batches into CSV and Parquet, then packages the selected artifacts.
- **Agent/MCP layer:** asks clarifying questions and proposes plans, but cannot bypass typed validation or confirmation.

## Why Kafka Instead of Only an In-Process Worker Pool?

An in-process worker pool is useful for parallelizing a single generation run, and the generator can use one internally. It is not a replacement for an event-streaming boundary.

Kafka is used because it provides:

- **Durability:** events remain available if a consumer or application instance dies.
- **Replay:** a new consumer or repaired projection can read the event history again.
- **Independent scaling:** producers, ingestion consumers, and downstream processors can scale separately.
- **Backpressure:** consumers can catch up at their own rate instead of forcing generation to wait synchronously.
- **Ordering by key:** using `user_id` as the message key preserves per-user ordering within a partition.
- **Operational visibility:** consumer lag makes downstream health measurable.

The tradeoff is real complexity: topic management, partitioning, retries, dead-letter topics, offsets, and idempotent consumers. For a tiny one-process job, an in-process pool is simpler. Kafka becomes valuable when the platform needs replayable ingestion, independent consumers, failure recovery, and a boundary between generation and persistence.

## Reproducibility Through Seeding

The generator treats the specification and seed as part of the job's identity. A deterministic run records:

- specification version and checksum
- generator and application versions
- random seed
- resolved plan checksum
- environment and relevant dependency versions

Randomness is derived from stable inputs rather than from wall-clock time or thread scheduling. Parent identifiers are generated before child records, and child relationships are selected from the already-generated parent set. This means the same specification, generator version, configuration, and seed produce equivalent logical records.

Exact byte-for-byte files are not assumed automatically: serialization order, library versions, and metadata timestamps can change bytes without changing the logical dataset. The platform should document those boundaries and validate row counts, keys, relationships, and checksums explicitly.

## Idempotency When a Worker Dies Mid-Batch

Kafka delivery is at-least-once, so a consumer must assume that a message may be delivered again. A worker dying after a database write but before its Kafka offset is committed is the normal failure case, not an exceptional shortcut around correctness.

The ingestion path handles this with a stable event identity:

1. Every generated event receives a deterministic `event_id` and carries its schema version, entity ID, timestamp, and correlation ID.
2. PostgreSQL stores `event_id` under a unique constraint, alongside the durable entity record or ingestion ledger.
3. The consumer writes the record in a transaction. A duplicate event becomes a no-op or an expected conflict rather than a second durable row.
4. The Kafka offset is committed only after the transaction succeeds.
5. If the worker dies mid-batch, Kafka redelivers uncommitted messages; the unique constraint makes the retry safe.
6. Poison messages are retried with bounded attempts and then sent to a dead-letter topic with diagnostic metadata.

Batching still matters for throughput. The batch transaction must remain bounded, and the system should checkpoint progress at a granularity that makes retries affordable. Redis can coordinate locks and progress, but PostgreSQL constraints and transactions are the final protection against duplicate durable data.

## Initial Vertical Slice

The first usable slice intentionally supports only two entities:

```text
JSON specification
  -> typed validation
  -> relationship graph and estimate
  -> seeded users and activity_events
  -> Kafka publication
  -> idempotent PostgreSQL ingestion
  -> Redis progress tracking
  -> CSV, Parquet, and ZIP export
```

The initial relationship is one-to-many: one user can own many activity events. More complex entities, anomaly injection, learned distributions, Kubernetes deployment, and AWS infrastructure follow only after this path is reliable.

## Planned Technology Stack

- C# / .NET 8 or newer supported release
- ASP.NET Core Minimal API and Worker Services
- PostgreSQL with Npgsql and Dapper
- Apache Kafka with Confluent.Kafka
- Redis with StackExchange.Redis
- CsvHelper and Parquet.Net
- Docker Compose, then Kubernetes
- xUnit and Testcontainers
- OpenTelemetry and Swagger/OpenAPI

