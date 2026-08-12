# KnowledgeBaseService Initialization Flow

## Overview

`knowledgeBaseService` is initialized via the **uber-go/dig** dependency injection container in `internal/container/container.go`. The container resolves all dependencies by type and constructs the service automatically in topological order.

## Struct Definition

Defined in `internal/application/service/knowledgebase.go:31`:

```go
type knowledgeBaseService struct {
    repo            interfaces.KnowledgeBaseRepository
    kgRepo          interfaces.KnowledgeRepository
    chunkRepo       interfaces.ChunkRepository
    shareRepo       interfaces.KBShareRepository
    kbShareService  interfaces.KBShareService
    modelService    interfaces.ModelService
    retrieveEngine  interfaces.RetrieveEngineRegistry
    ownership       retriever.TenantStoreOwnership
    tenantRepo      interfaces.TenantRepository
    fileSvc         interfaces.FileService
    storageResolver interfaces.StorageBackendResolver
    graphEngine     interfaces.RetrieveGraphRepository
    asynqClient     interfaces.TaskEnqueuer
    taskInspector   interfaces.TaskInspector
    taskPendingRepo interfaces.TaskPendingOpsRepository
    dsRepo          interfaces.DataSourceRepository
    syncLogRepo     interfaces.SyncLogRepository
    dsScheduler     *datasource.Scheduler
    audit           interfaces.AuditLogService
}
```

## Constructor

Defined in `internal/application/service/knowledgebase.go:54`:

```go
func NewKnowledgeBaseService(
    repo interfaces.KnowledgeBaseRepository,
    kgRepo interfaces.KnowledgeRepository,
    chunkRepo interfaces.ChunkRepository,
    shareRepo interfaces.KBShareRepository,
    kbShareService interfaces.KBShareService,
    modelService interfaces.ModelService,
    retrieveEngine interfaces.RetrieveEngineRegistry,
    ownership retriever.TenantStoreOwnership,
    tenantRepo interfaces.TenantRepository,
    fileSvc interfaces.FileService,
    storageResolver interfaces.StorageBackendResolver,
    graphEngine interfaces.RetrieveGraphRepository,
    asynqClient interfaces.TaskEnqueuer,
    taskInspector interfaces.TaskInspector,
    taskPendingRepo interfaces.TaskPendingOpsRepository,
    dsRepo interfaces.DataSourceRepository,
    syncLogRepo interfaces.SyncLogRepository,
    dsScheduler *datasource.Scheduler,
    audit interfaces.AuditLogService,
) interfaces.KnowledgeBaseService
```

## Container Registration Order

All registrations happen in `internal/container/container.go` within `BuildContainer()`.

### 1. Infrastructure Layer (L114-139)

| Line | Registration | Purpose |
|------|-------------|---------|
| 114 | `config.LoadConfig` | Application configuration |
| 115 | `initLangfuse` | Observability / tracing |
| 116 | `initDatabase` | GORM database connection |
| 117 | `initFileService` | File storage service |
| 118 | `initRedisClient` | Redis client |
| 119 | `initAntsPool` | Goroutine pool |
| 128 | `initRetrieveEngineRegistry` | Retrieval engine registry |
| 132 | `initDocReaderClient` | Document reader client |
| 135 | `initNeo4jClient` | Neo4j graph DB client |

### 2. Repository Layer (L142-176)

| Line | Registration | Maps to Field |
|------|-------------|---------------|
| 143 | `repository.NewTenantRepository` | `tenantRepo` |
| 148 | `repository.NewKnowledgeBaseRepository` | `repo` |
| 149 | `repository.NewKnowledgeRepository` | `kgRepo` |
| 151 | `repository.NewChunkRepository` | `chunkRepo` |
| 166 | `repository.NewKBShareRepository` | `shareRepo` |
| 172 | `repository.NewDataSourceRepository` | `dsRepo` |
| 173 | `repository.NewSyncLogRepository` | `syncLogRepo` |
| 175 | `repository.NewTaskPendingOpsRepository` | `taskPendingRepo` |
| 160 | `neo4jRepo.NewNeo4jRepository` | `graphEngine` |

### 3. Service Layer (L184-250)

| Line | Registration | Maps to Field |
|------|-------------|---------------|
| 185 | `service.NewTenantService` | (indirect, provides `tenantRepo`) |
| 189 | `service.NewAuditLogService` | `audit` |
| **191** | **`service.NewKnowledgeBaseService`** | **Target service** |
| 192 | `service.NewOrganizationService` | (downstream dependency) |
| 193 | `service.NewKBShareService` | `kbShareService` |
| 200 | `service.NewModelService` | `modelService` |
| 236 | `retriever.NewVectorStoreRepoOwnership` | `ownership` |
| 249 | `service.NewVectorStoreService` | (indirect) |
| 250 | `service.NewStorageBackendServiceWithResources` | `storageResolver` (via adapter at L252) |

### 4. Task Infrastructure (L270-302)

Conditionally registered based on Redis availability:

| Condition | `asynqClient` | `taskInspector` |
|-----------|--------------|-----------------|
| Redis available | `router.NewAsyncqClient` (L273) | `router.NewAsynqTaskInspector` (L286) |
| No Redis (Lite mode) | `router.NewSyncTaskExecutor` (L292) | `router.NewNoopTaskInspector` (L298) |

### 5. Data Source Scheduler (L311-314)

| Line | Registration | Maps to Field |
|------|-------------|---------------|
| 312 | `datasource.NewScheduler` | `dsScheduler` |

### 6. Handler Layer (L340-385)

| Line | Registration |
|------|-------------|
| 345 | `handler.NewKnowledgeBaseHandler` |

### 7. Router Layer (L394)

| Line | Registration |
|------|-------------|
| 394 | `router.NewRouter` |

## Dependency Resolution Map

The following table shows how each field of `knowledgeBaseService` is resolved by the dig container:

| Field | Interface | Concrete Provider | Registration Line |
|-------|-----------|-------------------|-------------------|
| `repo` | `KnowledgeBaseRepository` | `repository.NewKnowledgeBaseRepository` | L148 |
| `kgRepo` | `KnowledgeRepository` | `repository.NewKnowledgeRepository` | L149 |
| `chunkRepo` | `ChunkRepository` | `repository.NewChunkRepository` | L151 |
| `shareRepo` | `KBShareRepository` | `repository.NewKBShareRepository` | L166 |
| `kbShareService` | `KBShareService` | `service.NewKBShareService` | L193 |
| `modelService` | `ModelService` | `service.NewModelService` | L200 |
| `retrieveEngine` | `RetrieveEngineRegistry` | `initRetrieveEngineRegistry` | L128 |
| `ownership` | `TenantStoreOwnership` | `retriever.NewVectorStoreRepoOwnership` | L236 |
| `tenantRepo` | `TenantRepository` | `repository.NewTenantRepository` | L143 |
| `fileSvc` | `FileService` | `initFileService` | L117 |
| `storageResolver` | `StorageBackendResolver` | `service.NewStorageBackendServiceWithResources` + adapter | L250-252 |
| `graphEngine` | `RetrieveGraphRepository` | `neo4jRepo.NewNeo4jRepository` | L160 |
| `asynqClient` | `TaskEnqueuer` | `router.NewAsyncqClient` or `router.NewSyncTaskExecutor` | L273 / L292 |
| `taskInspector` | `TaskInspector` | `router.NewAsynqTaskInspector` or `router.NewNoopTaskInspector` | L286 / L298 |
| `taskPendingRepo` | `TaskPendingOpsRepository` | `repository.NewTaskPendingOpsRepository` | L175 |
| `dsRepo` | `DataSourceRepository` | `repository.NewDataSourceRepository` | L172 |
| `syncLogRepo` | `SyncLogRepository` | `repository.NewSyncLogRepository` | L173 |
| `dsScheduler` | `*datasource.Scheduler` | `datasource.NewScheduler` | L312 |
| `audit` | `AuditLogService` | `service.NewAuditLogService` | L189 |

## API Endpoint Wiring

The `POST /knowledge-bases` endpoint is wired in `internal/router/routes_knowledge.go:202`:

```go
kbManagement.POST("", g.Contributor(), handler.CreateKnowledgeBase)
```

The full call chain:

1. **Route** — `internal/router/routes_knowledge.go:202`
2. **Handler** — `internal/handler/knowledgebase.go:352` (`KnowledgeBaseHandler.CreateKnowledgeBase`)
3. **Service** — `internal/application/service/knowledgebase.go:113` (`knowledgeBaseService.CreateKnowledgeBase`)
4. **Repository** — `internal/application/repository/knowledgebase.go:26` (`knowledgeBaseRepository.CreateKnowledgeBase`)

## Key Design Notes

- The dig container resolves dependencies by type in topological order, ensuring all parameters are available when `NewKnowledgeBaseService` is invoked.
- `KBShareService` (L193) must be registered before `KnowledgeService` (L195) as noted by the inline comment.
- `StorageBackendResolver` is provided via a type adapter from `StorageBackendService` (L252), since the same concrete type implements both interfaces.
- Task infrastructure (`asynqClient`, `taskInspector`) is conditionally registered based on Redis availability, switching between distributed (asynq) and synchronous (in-process) execution modes.
