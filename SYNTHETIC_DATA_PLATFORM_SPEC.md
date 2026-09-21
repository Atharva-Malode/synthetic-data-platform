# Synthetic Data Platform Specification

## 1. Purpose

Build a C#/.NET platform that creates realistic, correlated synthetic datasets for configurable users, activities, and time ranges. The platform accepts a JSON generation specification, validates relationships, asks for missing information through an agent-assisted workflow, generates deterministic data, publishes events through Kafka, persists data to PostgreSQL, tracks jobs with Redis, and exports CSV, Parquet, and ZIP files.

The project is intentionally phased so that every phase produces a usable result and teaches one or more target technologies:

- C# and .NET service design
- PostgreSQL relational modeling and ingestion
- Apache Kafka event streaming
- Redis job coordination and idempotency
- Kubernetes deployment and batch jobs
- AWS managed infrastructure and operations
- Agentic planning and MCP tool integration

## 2. Design Principles

1. The generation engine is deterministic. The same specification and seed produce the same logical data.
2. The agent plans and validates; ordinary application code generates and stores data.
3. PostgreSQL is the source of truth for durable relational data.
4. Kafka carries events and enables replayable ingestion; it is not the only data store.
5. Redis stores short-lived coordination state, not the canonical dataset.
6. Every expensive generation request is estimated and confirmed before execution.
7. The first version favors correctness, observability, and explainability over extreme scale.
8. All generated data must be clearly synthetic and must not copy real personal data.

## 3. Proposed C# Technology Stack

### Application

- .NET 8 or a newer supported .NET release
- ASP.NET Core Minimal API for the orchestration API
- BackgroundService or .NET Worker Service for consumers and workers
- System.Text.Json for specification parsing
- FluentValidation for request and specification validation

### Data and infrastructure clients

- Npgsql for PostgreSQL
- Dapper for explicit SQL and bulk-oriented operations
- Confluent.Kafka for Kafka producers and consumers
- StackExchange.Redis for Redis
- Bogus for realistic primitive values
- Parquet.Net for Parquet output
- CsvHelper for CSV output
- Docker Compose for the initial local environment
- Kubernetes Jobs and Deployments for later local cluster execution

### Testing and quality

- xUnit
- Testcontainers for PostgreSQL, Kafka, and Redis integration tests
- OpenTelemetry for traces, metrics, and logs
- Swagger/OpenAPI for the orchestration API

The agent or MCP server can also be written in C# using the MCP SDK available at implementation time. It should call typed application tools rather than directly modifying the database.

## 4. High-Level Architecture

```text
CLI or Web UI
      |
      v
Orchestrator API (.NET)
      |
      +--> Specification Validator
      +--> Relationship Graph Builder
      +--> Size and Cost Estimator
      +--> Generation Job Coordinator
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
             |                         |
             +------> PostgreSQL <-----+

Redis: job status, locks, idempotency, short-lived agent state
ZIP: CSV and Parquet artifacts stored locally first, S3 later
```

## 5. Core Domain Objects

### GenerationJob

- `Id`
- `DatasetName`
- `SpecificationVersion`
- `Seed`
- `Status`: Draft, Validated, AwaitingConfirmation, Queued, Running, Completed, Failed, Cancelled
- `RequestedAt`
- `StartedAt`
- `CompletedAt`
- `EstimatedRows`
- `ActualRows`
- `OutputLocation`
- `ErrorMessage`

### DatasetSpecification

- Dataset metadata
- Date range
- User count
- Activity configuration
- Entity definitions
- Relationships
- Output configuration
- Kafka configuration
- PostgreSQL persistence configuration

### EntityDefinition

- Entity name
- Count expression
- Field definitions
- Parent entity, if any
- Generation order
- Distribution rules
- Relationship definitions

### GenerationPlan

The validated, executable representation of the user specification:

- Ordered entity graph
- Resolved counts
- Resolved dates
- Generated database schema plan
- Kafka topic plan
- Estimated storage and processing time
- Validation warnings

## 6. Example JSON Specification

```json
{
  "dataset": {
    "name": "subscription-platform",
    "seed": 20260911,
    "startDate": "2026-01-01",
    "endDate": "2026-03-31",
    "userCount": 10000,
    "activityMultiplier": 12
  },
  "entities": [
    {
      "name": "users",
      "count": "$dataset.userCount",
      "fields": {
        "id": { "type": "uuid", "unique": true },
        "email": { "type": "email", "unique": true },
        "country": {
          "type": "choice",
          "values": { "US": 0.5, "GB": 0.2, "IN": 0.2, "CA": 0.1 }
        },
        "created_at": {
          "type": "datetime",
          "between": ["$dataset.startDate", "$dataset.endDate"]
        }
      }
    },
    {
      "name": "activity_events",
      "count": "$dataset.userCount * $dataset.activityMultiplier",
      "relationships": {
        "user_id": {
          "references": "users.id",
          "cardinality": "many_to_one"
        }
      },
      "fields": {
        "event_type": {
          "type": "choice",
          "values": ["login", "page_view", "purchase", "logout"]
        },
        "occurred_at": {
          "type": "datetime",
          "between": ["$dataset.startDate", "$dataset.endDate"]
        }
      }
    }
  ],
  "outputs": {
    "formats": ["csv", "parquet"],
    "zip": true,
    "publishToKafka": true,
    "persistToPostgres": true
  }
}
```

## 7. Functional Requirements

### Specification and planning

- Accept a JSON specification through API and CLI.
- Validate syntax, types, expressions, duplicate entity names, cycles, and missing references.
- Build and display an entity relationship graph.
- Resolve generation order from relationships.
- Estimate row counts, approximate file size, and processing time.
- Report ambiguous or missing requirements.
- Require user confirmation before large jobs.

### Data generation

- Support seeded deterministic generation.
- Generate users, entities, and child records in dependency order.
- Preserve foreign-key relationships.
- Support weighted choices, null rates, ranges, expressions, and date distributions.
- Support weekday and seasonal activity patterns.
- Support configurable anomalies such as duplicates, missing events, late events, and invalid records.
- Support resumable generation by entity and partition where practical.

### Event pipeline

- Publish entity events to Kafka.
- Use a stable entity key, such as `user_id`, when per-user ordering matters.
- Include event ID, event type, schema version, entity ID, timestamp, and correlation ID.
- Consume events into PostgreSQL.
- Make consumers idempotent.
- Add retry and dead-letter topics.
- Expose consumer lag and failed-message metrics.

### Persistence and export

- Create PostgreSQL tables from the validated model or use a controlled schema template.
- Apply primary keys, foreign keys, unique constraints, and indexes.
- Support efficient bulk ingestion.
- Write CSV and Parquet files.
- Package selected outputs and metadata into a ZIP archive.
- Store export checksums and row counts.

### Job management

- Track job state in PostgreSQL.
- Use Redis for progress updates, short-lived locks, idempotency keys, and cancellation signals.
- Expose job status and progress through the API.
- Prevent duplicate execution of the same job request.
- Retain failure details sufficient for retry or diagnosis.

## 8. Phased Delivery Plan

### Phase 0: Repository and engineering baseline

**Duration:** 1-2 days

- Create a separate solution named `SyntheticDataPlatform`.
- Add API, domain, application, infrastructure, worker, and test projects.
- Add configuration, logging, health checks, OpenAPI, and Docker support.
- Add a basic CI build and test workflow.

**Exit criteria:** The solution builds, tests run, and the API exposes a health endpoint.

### Phase 1: Deterministic generator and file exports

**Duration:** 5-8 days

- Implement the JSON specification model.
- Validate basic entity and field definitions.
- Build the relationship graph and generation order.
- Generate users and activity events using a seed.
- Export CSV, Parquet, and ZIP files.
- Add row-count and referential-integrity checks.

**Exit criteria:** The same input and seed generate equivalent output twice, and a sample dataset can be inspected without any infrastructure dependency.

### Phase 2: PostgreSQL persistence

**Duration:** 4-6 days

- Add PostgreSQL schema migrations.
- Persist generation jobs and generated entities.
- Add foreign keys, indexes, and unique constraints.
- Implement bulk ingestion.
- Add integration tests using Testcontainers.

**Exit criteria:** Generated users and activities are queryable in PostgreSQL and all relationship checks pass.

### Phase 3: Kafka event streaming

**Duration:** 7-10 days

- Add a Kafka producer to publish generated events.
- Add topics, keys, headers, and schema versions.
- Implement a consumer that writes events to PostgreSQL.
- Add consumer groups, retries, dead-letter topics, and idempotency.
- Add replay tests and basic lag metrics.

**Exit criteria:** A dataset can be generated as an event stream, consumed into PostgreSQL, replayed, and safely processed without duplicate durable records.

### Phase 4: Redis-backed orchestration

**Duration:** 3-5 days

- Add Redis job progress records.
- Add distributed job locks.
- Add idempotency keys.
- Add cancellation and retry signals.
- Add job status polling to the API.

**Exit criteria:** Concurrent requests cannot run the same job twice, progress is visible, and a failed job can be retried safely.

### Phase 5: Agent-assisted planning and MCP

**Duration:** 5-8 days

- Add typed planning tools for validation, graph inspection, estimates, and job status.
- Ask clarifying questions for missing dates, counts, relationships, and outputs.
- Generate a validated specification from user answers.
- Show the plan and require confirmation before execution.
- Keep the final specification available for reproducibility.

**Exit criteria:** A user can describe a dataset, answer questions, review a plan, confirm it, and start a job without manually writing every field.

### Phase 6: Kubernetes locally

**Duration:** 7-12 days

- Create Docker images for the API, workers, and consumers.
- Run PostgreSQL, Kafka, and Redis locally with Docker Compose first.
- Deploy application workloads to kind or minikube.
- Use Kubernetes Jobs for generation and export work.
- Add ConfigMaps, Secrets, health probes, resource limits, and persistent volumes.
- Add Helm or Kustomize configuration.

**Exit criteria:** The complete platform can be started locally in Kubernetes and a dataset job completes successfully.

### Hands-on Docker and Kubernetes Lab Sequence

Use the project as a progressively harder local lab. Each lab should be completed and documented before moving to the next one.

#### Docker Lab 1: Containerize the Go services

- Create a multi-stage Dockerfile for the API.
- Create separate images for the generation worker and Kafka consumer only when their runtime needs differ.
- Run the application as a non-root user.
- Add a small runtime image containing only the compiled binary and required certificates.
- Configure the service through environment variables rather than image changes.
- Add `/health/live` and `/health/ready` endpoints.
- Add `.dockerignore` and verify that source, secrets, and generated exports are not copied into images.

Practice commands:

```text
docker build -t synthetic-api:dev .
docker run --rm -p 8080:8080 synthetic-api:dev
docker image inspect synthetic-api:dev
docker logs <container>
```

#### Docker Lab 2: Run the local dependency stack

Create a Docker Compose environment containing:

- Go API
- Generation worker
- Kafka
- PostgreSQL
- Redis
- Kafka consumer

Use named volumes for PostgreSQL and Kafka data. Use a user-defined network and health checks. Keep credentials in a local `.env` file that is excluded from source control.

Practice:

- Start and stop the complete stack.
- Inspect service logs.
- Verify dependency health.
- Submit a small generation job.
- Restart the Kafka consumer and confirm it resumes from its offset.
- Restart Redis and observe which state is recoverable from PostgreSQL.
- Confirm the generated ZIP is available outside the container through a mounted output directory.

#### Kubernetes Lab 1: Deploy the API and dependencies

Create a local `kind` cluster and deploy the application with Kubernetes manifests.

Use:

- `Deployment` for the API.
- `Deployment` for the Kafka consumer.
- `Job` for one-off generation.
- `Job` for export packaging.
- `Service` for internal API and database access.
- `ConfigMap` for non-secret configuration.
- `Secret` for credentials.
- Persistent volumes for local PostgreSQL, Kafka, and generated artifacts.

Practice commands:

```text
kind create cluster --name synthetic-data
kubectl apply -f deployments/kubernetes/
kubectl get pods,services,jobs
kubectl logs deployment/synthetic-api
kubectl describe pod <pod-name>
kubectl port-forward service/synthetic-api 8080:8080
```

#### Kubernetes Lab 2: Run a generation job

- Submit a bounded specification through the API.
- Create a Kubernetes Job with the generation plan ID.
- Watch the Job complete.
- Read progress from Redis through the API.
- Verify records in PostgreSQL.
- Verify Kafka offsets and consumer lag.
- Verify CSV, Parquet, and ZIP output.
- Delete the Job while running and verify cancellation behavior.

#### Kubernetes Lab 3: Practice failure and recovery

Intentionally introduce controlled failures:

- Stop the Kafka consumer.
- Restart the API pod.
- Make PostgreSQL temporarily unavailable.
- Cause a generated record to fail validation.
- Run two requests with the same idempotency key.
- Exceed the configured row limit.

For each exercise, document:

- What failed.
- Which component detected it.
- Whether the job was retried.
- Whether duplicate records were avoided.
- What state was durable.
- How the user can recover.

#### Kubernetes Lab 4: Scaling and resource behavior

- Scale the Kafka consumer from one replica to two or more.
- Observe consumer-group partition assignment.
- Set CPU and memory requests and limits.
- Compare generation speed at different worker counts.
- Use a bounded Go worker pool so increased concurrency does not exhaust memory.
- Inspect pod resource usage.
- Verify that PostgreSQL and Redis remain protected from uncontrolled request rates.

Do not claim that replicas improve throughput unless the Kafka topic has enough partitions and the consumer is safe for concurrent processing.

#### Kubernetes Lab 5: Operational packaging

- Add startup, readiness, and liveness probes.
- Add structured logs containing job IDs and correlation IDs.
- Add graceful shutdown for workers and consumers.
- Add a Kubernetes resource quota for local development.
- Add a NetworkPolicy after the basic stack is working.
- Package the manifests with Helm or Kustomize.
- Add a one-command setup and teardown process.

**Hands-on completion criteria:** A fresh local cluster can be created from documentation, the stack can be deployed without manual database edits, a bounded job can complete end to end, a consumer can be scaled, and at least three failure scenarios can be demonstrated and recovered from.

### Phase 7: AWS deployment

**Duration:** 10-20 days

- Push images to ECR.
- Store exports in S3.
- Use RDS PostgreSQL.
- Use ElastiCache for Redis.
- Use MSK for Kafka.
- Deploy stateless application workloads to EKS.
- Add IAM, Secrets Manager, CloudWatch, logging, alarms, and cost controls.
- Add a documented teardown process.

**Exit criteria:** A small end-to-end job runs in AWS, exports are downloadable from S3, metrics and logs are visible, and resources can be shut down safely.

## 9. Testing Strategy

### Unit tests

- Expression evaluation
- Weighted distributions
- Seeded random behavior
- Relationship graph ordering
- Specification validation
- Job state transitions

### Integration tests

- PostgreSQL constraints and bulk ingestion
- Kafka publish, consume, retry, and replay
- Redis locks and idempotency
- CSV, Parquet, and ZIP validation

### End-to-end tests

- Submit specification
- Validate and confirm plan
- Generate dataset
- Publish and consume events
- Verify PostgreSQL row counts
- Verify exports and checksums

### Load tests

Start with modest targets:

- 10,000 users
- 1,000,000 activity events
- Generation completion within a documented target
- No duplicate primary keys
- No orphaned foreign keys
- Consumer lag returning to zero after generation

Increase scale only after correctness is established.

## 10. Initial API Surface

```text
POST   /api/specifications/validate
POST   /api/jobs/estimate
POST   /api/jobs
GET    /api/jobs/{jobId}
POST   /api/jobs/{jobId}/cancel
GET    /api/jobs/{jobId}/events
GET    /api/jobs/{jobId}/exports
GET    /api/health
GET    /api/metrics
```

Potential MCP tools:

```text
validate_generation_spec
build_relationship_graph
estimate_generation
create_generation_job
get_generation_status
cancel_generation_job
list_exports
```

## 11. Recommended First Vertical Slice

Do not begin with the agent or AWS. Build this path first:

```text
JSON file
  -> C# validation
  -> seeded users and activity generation
  -> Kafka publication
  -> Kafka consumer
  -> PostgreSQL ingestion
  -> Redis progress tracking
  -> CSV and Parquet export
  -> ZIP archive
```

The first vertical slice should support only two entities: `users` and `activity_events`. Add subscriptions, orders, payments, and anomaly injection after the pipeline is reliable.

## 12. Resume-Ready Milestones

After Phase 2, it is accurate to describe hands-on PostgreSQL experience.

After Phase 3, it is accurate to describe hands-on Kafka experience involving producers, consumers, replay, retries, and idempotency.

After Phase 4, it is accurate to describe hands-on Redis experience involving job coordination and idempotency.

Example project description:

```text
Synthetic Data Platform | Personal Project
C#, .NET, PostgreSQL, Apache Kafka, Redis, Docker

- Built a JSON-driven C# platform that generates deterministic, correlated synthetic users and activity events.
- Used PostgreSQL for relational persistence, foreign-key integrity, indexing, and bulk ingestion.
- Published activity events through Kafka and implemented idempotent consumers with retries and replay.
- Used Redis for job progress, distributed locks, and duplicate-job prevention.
- Exported validated datasets to CSV, Parquet, and ZIP archives.
```

Only add Kubernetes and AWS after the corresponding deployment is actually working and documented.

## 13. Risks and Controls

| Risk | Control |
|---|---|
| Agent produces invalid schemas | Validate every plan with typed C# rules before execution |
| Large accidental jobs | Estimate rows and storage, require confirmation, enforce limits |
| Duplicate Kafka delivery | Use stable event IDs and database idempotency constraints |
| Redis data loss | Keep durable job state in PostgreSQL |
| Unbounded memory usage | Stream rows and write in batches |
| Broken relationships | Generate parent entities first and run integrity checks |
| AWS cost surprises | Add quotas, budgets, lifecycle policies, and teardown scripts |
| Misleading resume claims | Label experience as personal-project or hands-on practice |

## 14. Definition of Success

The project is successful when a new user can submit a JSON specification for a bounded dataset, review the generated relationship plan and estimate, confirm the job, and receive a ZIP containing CSV and Parquet outputs. The same job must also demonstrate Kafka event flow, PostgreSQL persistence, Redis coordination, automated validation, and reproducible results from a fixed seed.
