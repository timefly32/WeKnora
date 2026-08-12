# WeKnora `docker compose up -d` Startup Flow

This document traces the complete startup sequence from `docker compose up -d` through every layer of the WeKnora platform — Docker Compose orchestration, image build, container entrypoint, Go application bootstrap, and graceful shutdown.

---

## 1. Docker Compose Service Topology

### 1.1 Default Services (no `--profile`)

| Service | Image | Depends On | Health Check | Exposed Ports |
|---------|-------|------------|--------------|---------------|
| **redis** | `redis:7.0-alpine` | — | — | internal only |
| **postgres** | `paradedb/paradedb:v0.22.2-pg17` | — | `pg_isready` (10s interval, 30s start period) | internal only |
| **docreader** | `wechatopenai/weknora-docreader` | — | `grpc_health_probe` (30s interval, 60s start period) | 50051 (internal) |
| **app** | `wechatopenai/weknora-app` | redis (started), postgres (healthy), docreader (healthy) | `curl -f http://localhost:8080/health` (30s interval, 60s start period) | `${APP_PORT:-8080}:8080` |
| **frontend** | `wechatopenai/weknora-ui` | app (healthy) | — | `${FRONTEND_PORT:-80}:80` |

### 1.2 Startup Order

Docker Compose resolves the dependency graph and starts services in topological order:

```
redis ──┐
postgres ──┼──► app ──► frontend
docreader ─┘
```

1. **redis**, **postgres**, **docreader** start in parallel (no inter-dependencies).
2. **app** waits until:
   - `redis` is `service_started`
   - `postgres` passes its health check (`pg_isready`)
   - `docreader` passes its health check (`grpc_health_probe`)
3. **frontend** waits until `app` passes its health check (`curl /health`).

### 1.3 Opt-in Services (via `--profile`)

| Profile | Services |
|---------|----------|
| `minio` / `full` | minio |
| `neo4j` / `full` | neo4j |
| `qdrant` / `full` | qdrant |
| `milvus` | milvus |
| `weaviate` | weaviate |
| `doris` | doris-fe, doris-be |
| `searxng` / `full` | searxng-init, searxng |
| `langfuse` / `full` | langfuse-db-init, langfuse-clickhouse, langfuse-minio, langfuse-worker, langfuse-web |
| `odl-hybrid` | odl-hybrid |
| `full` | mcp, sandbox, dex, and all of the above |
| `sandbox` | sandbox (build/pull only, not a long-running service) |

### 1.4 Network & Volumes

- All services share the `WeKnora-network` bridge network.
- Named volumes: `postgres-data`, `data-files`, `docreader-tmp`, `minio_data`, `neo4j-data`, `qdrant_data`, `milvus_data`, `weaviate_data`, `doris_fe_meta`, `doris_fe_log`, `doris_be_storage`, `doris_be_log`, `langfuse_clickhouse_data`, `langfuse_clickhouse_logs`, `langfuse_minio_data`, `searxng_config`.

---

## 2. Environment Variable Injection

### 2.1 `.env` File

The `app` service uses `env_file: .env` to load **all** variables from the `.env` file into the container environment. This is the primary configuration mechanism — any variable referenced in `config.yaml` or read via `os.Getenv()` is available without being explicitly listed under `environment:`.

Other services (frontend, docreader, mcp, etc.) only consume variables explicitly listed in their `environment:` blocks.

### 2.2 `start_all.sh` Pre-flight

The orchestration script (`scripts/start_all.sh`) performs these checks before invoking `docker compose up`:

1. **Detect Compose command**: Prefers `docker compose` (v2 plugin), falls back to `docker-compose` (v1).
2. **Ensure `.env` exists**: Copies `.env.example` → `.env` if missing.
3. **Validate critical vars**: Checks `DB_DRIVER` and `STORAGE_TYPE` are set.
4. **Detect platform**: Sets `PLATFORM` to `linux/amd64` or `linux/arm64`.
5. **Optionally start Ollama**: If `--ollama` or `--all` is passed, starts local or verifies remote Ollama.
6. **Run `docker compose up`**: With `--pull always` (default) or `--build` (if `--no-pull`).
7. **Pre-pull sandbox image**: Background pull of `wechatopenai/weknora-sandbox` for Agent Skills.

### 2.3 Variable Categories in `app` Service

The `app` service's `environment:` block explicitly passes ~100 variables (with defaults) covering:

| Category | Key Variables |
|----------|---------------|
| Logging | `LOG_LEVEL`, `LOG_PATH`, `LLM_DEBUG_LOG` |
| Runtime | `GIN_MODE`, `TZ`, `AUTO_MIGRATE`, `WEKNORA_LANGUAGE` |
| Database | `DB_DRIVER`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` |
| Auth/RBAC | `DISABLE_REGISTRATION`, `JWT_SECRET`, `WEKNORA_TENANT_*` |
| Object Storage | `STORAGE_TYPE`, `MINIO_*`, `COS_*`, `S3_*`, `OBS_*`, `OSS_*`, `TOS_*` |
| Vector Store | `RETRIEVE_DRIVER`, `QDRANT_*`, `MILVUS_*`, `ELASTICSEARCH_*`, `OPENSEARCH_*` |
| DocReader | `DOCREADER_ADDR`, `GRPC_TLS_*`, `GRPC_AUTH_TOKEN` |
| LLM | `OLLAMA_BASE_URL`, `BATCH_EMBED_SIZE`, `VLM_HTTP_TIMEOUT_SECONDS` |
| Redis | `REDIS_ADDR`, `REDIS_PASSWORD`, `REDIS_DB`, `REDIS_PREFIX` |
| Knowledge Graph | `NEO4J_ENABLE`, `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD` |
| Security | `SYSTEM_AES_KEY`, `SSRF_WHITELIST` |
| Agent/Sandbox | `WEKNORA_SANDBOX_*`, `WEKNORA_AGENT_*` |
| OIDC | `OIDC_AUTH_ENABLE`, `OIDC_AUTH_*` |
| Langfuse | `LANGFUSE_ENABLED`, `LANGFUSE_*` |
| Bootstrap | `WEKNORA_BOOTSTRAP_SYSTEM_ADMIN_EMAIL` |

---

## 3. Dockerfile.app: Multi-Stage Build

### 3.1 Build Stage (`golang:1.26-bookworm`)

1. **Install system deps**: `git`, `build-essential`, `libsqlite3-dev` (with optional APK mirror).
2. **Install `migrate` tool**: `go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest`.
3. **Download Go modules**: `go mod download` (with build cache mount).
4. **Download DuckDB**: `go run cmd/download/duckdb/duckdb.go`.
5. **Copy source**: Full project copied into the build context.
6. **Inject build metadata**: `VERSION`, `COMMIT_ID`, `BUILD_TIME`, `GO_VERSION` via build args.
7. **Compile**: `make build-prod` produces the `WeKnora` binary.
8. **Cache C++ deps**: Copy `yanyiwu` segmenter modules for runtime.

### 3.2 Final Stage (`debian:12.12-slim`)

1. **Create non-root user**: `useradd -m -s /bin/bash appuser`.
2. **Install runtime deps**:
   - `ca-certificates` (first, for HTTPS)
   - `build-essential`, `postgresql-client`, `default-mysql-client`, `tzdata`, `sed`, `curl`, `bash`, `vim`, `wget`
   - `libsqlite3-0`
   - `python3`, `python3-pip`, `python3-dev`, `libffi-dev`, `libssl-dev`
   - `nodejs`, `npm`
   - `gosu` (privilege-dropping tool)
   - `ffmpeg`
   - `uv` (Python package manager via astral.sh installer)
3. **Create data directories**: `/data/files` owned by `appuser`.
4. **Copy artifacts from builder**:
   - `migrate` binary → `/usr/local/bin/`
   - `yanyiwu` C++ segmenter data → `/go/pkg/mod/github.com/yanyiwu/`
   - `config/`, `scripts/`, `migrations/`, `dataset/samples/`
   - `skills/preloaded/` → also backed up to `skills/_builtin/` (read-only copy)
   - DuckDB data → `/home/appuser/.duckdb`
   - `WeKnora` binary
   - `scripts/docker-entrypoint.sh`
5. **Set entrypoint**: `./scripts/docker-entrypoint.sh`
6. **Default command**: `./WeKnora`

---

## 4. Container Entrypoint: `docker-entrypoint.sh`

The entrypoint runs as **root** and performs three phases:

### 4.1 Fix Bind-Mount Ownership

```bash
MOUNT_DIRS=(/app/skills/preloaded /data/files)
for dir in "${MOUNT_DIRS[@]}"; do
    chown -R appuser:appuser "$dir" 2>/dev/null || true
done
```

Bind-mounted host directories may have different UID/GID than the container's `appuser`. The `chown` ensures the application can read/write these directories.

### 4.2 Merge Built-in Skills

```bash
BUILTIN_DIR="/app/skills/_builtin"
PRELOADED_DIR="/app/skills/preloaded"
# Copy any missing built-in skill from _builtin → preloaded
```

When a bind-mount replaces `/app/skills/preloaded`, built-in skills that were in the image may disappear. This step copies back any missing built-in skills without overwriting user-provided ones.

### 4.3 Drop Privileges

```bash
exec gosu appuser "$@"
```

Drops from root to `appuser` and executes the main process (`./WeKnora`). This follows the same pattern as official PostgreSQL and Redis images.

---

## 5. Go Application Startup (`cmd/server/main.go`)

### 5.1 Phase 1: Gin Mode & Startup Banner

```
main() →
  gin.SetMode()           # release or debug based on GIN_MODE
  SilenceGinRouteSpam()   # suppress ~150 per-route debug lines
  LogStartupEnv()         # print curated env var banner
  MarkServerStarted()     # record boot timestamp for uptime tracking
```

**`SilenceGinRouteSpam()`** (`internal/runtime/startup.go`):
- Replaces Gin's per-route `DebugPrintRouteFunc` with an atomic counter.
- Non-route debug lines are routed through the structured logger.
- A single summary line ("registered N routes") is printed later via `LogGinRouteCount()`.

**`LogStartupEnv()`** (`internal/runtime/startup.go`):
- Prints a curated list of ~25 environment variables.
- Sensitive values (passwords, keys) show only `set (N chars)`.
- Emits targeted warnings for common misconfigurations (e.g., `SYSTEM_AES_KEY` not 32 bytes).

**`MarkServerStarted()`** (`internal/runtime/server.go`):
- Records `time.Now().UTC()` for `ServerUptime()` calculations.

### 5.2 Phase 2: Dependency Injection Container

```
main() →
  runtime.GetContainer()  # returns the global dig.Container (init'd in runtime/container.go)
  container.BuildContainer(c)
```

**`runtime.GetContainer()`** (`internal/runtime/container.go`):
- Returns the global `*dig.Container` created in `init()`.
- The `init()` function runs before `main()` and creates a fresh `dig.New()` container.

**`BuildContainer()`** (`internal/container/container.go`):
- Registers all application dependencies in order:
  1. **Resource cleaner** — for graceful shutdown cleanup
  2. **Core infrastructure** — `Config`, Langfuse, `*gorm.DB`, `FileService`, `*redis.Client`, goroutine pool
  3. **Retrieval engine registry** — vector store engine factory
  4. **External service clients** — DocReader gRPC, Ollama, Neo4j, StreamManager, DuckDB
  5. **Repositories** (~30) — data access layer for all domain entities
  6. **MCP manager** — MCP client connection management
  7. **Business services** (~30) — domain logic layer
  8. **Chat pipeline plugins** (~15) — search, rerank, web fetch, merge, completion, etc.
  9. **HTTP handlers** (~30) — API endpoint handlers
  10. **IM integration** — WeChat, Feishu, DingTalk, Slack, Telegram, etc.
  11. **Router** — Gin engine with all routes registered
  12. **Task queue** — Asynq servers (Redis mode) or sync executor (Lite mode)

- **Database initialization** (`initDatabase`):
  - Supports `postgres` and `sqlite` drivers (via `DB_DRIVER` env var).
  - Runs auto-migrations unless `AUTO_MIGRATE=false`.
  - Resolves `__pending_env__` storage provider markers.
  - Migrates legacy storage backends.
  - Syncs PostgreSQL sequences.
  - Configures connection pool (SQLite: 1 max open conn; PostgreSQL: 10 max idle).

- **Redis initialization** (`initRedisClient`):
  - If `REDIS_ADDR` is empty → returns `nil` (Lite mode, no Redis).
  - Otherwise connects and pings.

- **Task queue**:
  - **Redis mode**: 6 Asynq servers (core, post-process, enrichment, maintenance, shared, wiki) + distributed model concurrency limiter.
  - **Lite mode**: `SyncTaskExecutor` (inline goroutines) + in-process concurrency limiter.

### 5.3 Phase 3: Bootstrap Hooks

```
main() →
  runStartupBootstrap(c)
```

**`runStartupBootstrap()`** (`cmd/server/bootstrap.go`):
- **Best-effort**: failures only warn, never abort startup.
- **API key hash backfill**: Repairs legacy `tenants.api_key` rows missing a hash. Short-circuits with a cheap `EXISTS` query once all rows are backfilled.
- **System admin promotion**: If `WEKNORA_BOOTSTRAP_SYSTEM_ADMIN_EMAIL` is set and the deployment has zero system admins, the user with that email is promoted to system admin. Idempotent — once any system admin exists, the env var stops granting privileges.

### 5.4 Phase 4: HTTP Server Start

```
main() →
  c.Invoke(func(cfg, router, resourceCleaner, systemSettingSvc) {
    server := &http.Server{Handler: router}
    listener := listenWithRetry(addr, 10, 300ms)
    systemSettingSvc.SubscribeRedis(ctx)  # best-effort
    signal.Notify(signals, shutdownSignals...)
    # ... graceful shutdown goroutine ...
    server.Serve(listener)
  })
```

**`listenWithRetry()`** (`cmd/server/listen.go`):
- Retries `net.Listen("tcp", addr)` up to 10 times.
- Exponential backoff: `300ms * 2^i`, capped at 3 seconds.
- Solves transient port-unavailability issues (e.g., previous process still draining).

**`systemSettingSvc.SubscribeRedis(ctx)`**:
- Starts a goroutine that subscribes to Redis pub/sub for system setting changes.
- Best-effort: warns on failure (Redis may be disabled in Lite mode).

### 5.5 Phase 5: Route Registration Summary

After the router is built, `LogGinRouteCount()` prints a single summary line:

```
[gin] registered 150 routes
```

---

## 6. Graceful Shutdown

### 6.1 Signal Handling

**Unix** (`cmd/server/signals_unix.go`): `SIGINT`, `SIGTERM`, `SIGHUP`

**Windows** (`cmd/server/signals_windows.go`): `SIGINT`, `SIGTERM`

### 6.2 Shutdown Sequence

1. **First signal received**:
   - Close the TCP listener immediately (releases port for next process).
   - Start graceful drain with `server.Shutdown(ctx)` (default timeout: 30s, configurable via `cfg.Server.ShutdownTimeout`).
2. **Second signal received**:
   - Force-close all connections immediately via `server.Close()`.
3. **After drain completes**:
   - Run `resourceCleaner.Cleanup(ctx)` to release DI-managed resources (DB connections, Redis client, goroutine pools, etc.).
   - Cancel the context, unblocking `main()`.

### 6.3 Health Check Interaction

The `app` container's health check (`curl -f http://localhost:8080/health`) will begin failing once the listener is closed. Docker Compose will mark the container as unhealthy, but since `restart: unless-stopped` is set, it will not auto-restart unless the process exits.

---

## 7. Configuration Loading

### 7.1 `config/config.yaml`

Mounted as a bind mount at `/app/config/config.yaml`. Contains structured configuration for:

- `server` — host, port, shutdown timeout
- `conversation` — chat defaults
- `knowledge_base` — indexing, chunking parameters
- `storage` — storage backend defaults
- `retriever` — search engine parameters
- `graph` — knowledge graph settings
- `llm` — LLM provider defaults
- `document` — document processing settings
- `agent` — agent configuration

### 7.2 Environment Variable Override

Most runtime behavior is controlled by environment variables (loaded from `.env`), which take precedence over `config.yaml` values. The `config.LoadConfig()` function reads the YAML file first, then overlays environment variable values.

### 7.3 Optional `builtin_models.yaml`

If `config/builtin_models.yaml` is mounted, it declares LLM models that are seeded into the database on startup. This allows declarative model configuration without UI interaction.

---

## 8. Complete Startup Timeline

```
T+0s   docker compose up -d
       ├── redis starts
       ├── postgres starts (pg_isready polling begins)
       └── docreader starts (grpc_health_probe polling begins)

T+~5s  postgres healthy
T+~10s docreader healthy

T+~10s app container starts
       ├── docker-entrypoint.sh runs as root
       │   ├── chown bind-mount directories
       │   ├── merge built-in skills
       │   └── exec gosu appuser ./WeKnora
       └── Go binary starts
           ├── gin.SetMode()
           ├── SilenceGinRouteSpam()
           ├── LogStartupEnv() — print env banner
           ├── MarkServerStarted()
           ├── BuildContainer()
           │   ├── config.LoadConfig()
           │   ├── initDatabase() — connect + migrate
           │   ├── initRedisClient()
           │   ├── initFileService()
           │   ├── initRetrieveEngineRegistry()
           │   ├── initDocReaderClient()
           │   ├── register all repositories
           │   ├── register all services
           │   ├── register all handlers
           │   ├── router.NewRouter() — wire ~150 routes
           │   └── start Asynq servers (or sync executor)
           ├── runStartupBootstrap()
           │   ├── backfill API key hashes
           │   └── promote system admin (if env var set)
           ├── listenWithRetry() — bind :8080
           ├── SubscribeRedis() — system settings pub/sub
           └── server.Serve() — ready

T+~30s app health check passes (curl /health)

T+~30s frontend starts
       └── nginx proxies to app:8080

T+~35s All services up and serving traffic
```

---

## 9. Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| `env_file: .env` for app | Avoids enumerating every variable in `environment:`; new env vars automatically reach the app. |
| `gosu` privilege drop | Container starts as root (for `chown`), then drops to `appuser` for the application — same pattern as official postgres/redis images. |
| Built-in skills backup (`_builtin/`) | Bind-mounting `skills/preloaded` replaces the directory; `_builtin/` preserves image-bundled skills for merge-back. |
| Best-effort bootstrap | `runStartupBootstrap()` never aborts startup — a typo in `WEKNORA_BOOTSTRAP_SYSTEM_ADMIN_EMAIL` should not brick the deployment. |
| `listenWithRetry()` | Handles transient port conflicts from previous process drain; exponential backoff with 3s cap. |
| Lite mode (no Redis) | When `REDIS_ADDR` is empty, the app runs without Redis: sync task execution, in-process concurrency limiter, no pub/sub. |
| Health check with `start_period: 60s` | Gives the app time for DB migration + container build before Docker marks it unhealthy. |
| `AUTO_MIGRATE=true` by default | Zero-friction first-run experience; operators can disable for externally-managed migrations. |
