# Document Upload and Intelligent Q&A Retrieval Flow

This document describes the end-to-end flow from uploading a PDF document into a knowledge base through to answering a user question via intelligent retrieval-augmented generation (RAG).

---

## Table of Contents

1. [Overview](#overview)
2. [Phase 1: Document Upload API](#phase-1-document-upload-api)
3. [Phase 2: Asynchronous Document Processing](#phase-2-asynchronous-document-processing)
4. [Phase 3: Intelligent Q&A Retrieval](#phase-3-intelligent-qa-retrieval)
5. [Key Data Structures](#key-data-structures)
6. [Configuration and Extensibility](#configuration-and-extensibility)

---

## Overview

The system follows a three-phase architecture:

```
┌──────────────┐     ┌──────────────────────────────┐     ┌──────────────────────────┐
│  HTTP Upload  │────▶│  Async Processing Pipeline    │────▶│  RAG Q&A Pipeline        │
│  (sync API)   │     │  (asynq task queue)           │     │  (event-driven pipeline) │
└──────────────┘     └──────────────────────────────┘     └──────────────────────────┘
   Handler →            DocReader → Chunker →               Search → Rerank →
   Service →            Embedder → VectorStore              Merge → LLM Stream
   Asynq Enqueue
```

- **Phase 1** (synchronous): The HTTP handler accepts the file, persists it to object storage, creates a `Knowledge` database record, and enqueues an asynq task.
- **Phase 2** (asynchronous): A worker picks up the task, parses the document into markdown, splits it into chunks, generates vector embeddings, and writes them to the vector store.
- **Phase 3** (on-demand): When a user asks a question, the RAG pipeline retrieves relevant chunks via hybrid (vector + keyword) search, reranks them, and feeds them into an LLM to generate a streaming answer.

---

## Phase 1: Document Upload API

### API Endpoint

```
POST /knowledge-bases/:kb_id/knowledge
```

**Handler**: `KnowledgeHandler.CreateKnowledgeFromFile` (`internal/handler/knowledge.go:316`)

### Request Processing

The handler parses a `multipart/form-data` request with the following fields:

| Field              | Type     | Description                                        |
|--------------------|----------|----------------------------------------------------|
| `file`             | File     | The uploaded file (PDF, DOCX, etc.)                |
| `fileName`         | String   | Optional override for the file name                |
| `metadata`         | JSON     | Arbitrary key-value metadata                       |
| `enable_multimodel`| Bool     | Enable multimodal (image OCR/caption) processing   |
| `process_config`   | JSON     | Per-document processing overrides                  |
| `tag_ids`          | JSON     | Tag IDs to associate with the document             |
| `channel`          | String   | Import channel identifier                          |

### Service Layer

**Service**: `knowledgeService.CreateKnowledgeFromFile` (`internal/application/service/knowledge_create.go`)

The service performs these steps:

1. **File name processing**: Supports folder-style paths (e.g. `folder/subfolder/file.pdf`) via `SplitKnowledgeRelativePath`, which separates the folder path from the base file name.

2. **File type detection**: `getFileType(fileName)` determines the file extension. Video files are rejected at this stage (`IsVideoType` → error).

3. **File storage**: The uploaded file bytes are persisted via `FileService.SaveBytes()`, which writes to the configured object storage backend (local filesystem, S3, etc.). The returned `filePath` is stored on the `Knowledge` record.

4. **Knowledge record creation**: A `Knowledge` row is inserted into the database with:
   - `parse_status = "pending"`
   - `enable_status = "disabled"` (not yet searchable)
   - `file_path`, `file_name`, `file_type`, `file_size`, `file_hash`
   - `embedding_model_id` inherited from the parent knowledge base

5. **Tenant storage accounting**: The tenant's `storage_used` is incremented by the file size.

6. **Asynq task enqueue**: An asynq task of type `document:process` (`types.TypeDocumentProcess`) is created with a `DocumentProcessPayload` containing:
   ```json
   {
     "tenant_id": 1,
     "knowledge_id": "uuid-of-knowledge",
     "knowledge_base_id": "uuid-of-kb",
     "file_path": "/path/to/stored/file",
     "file_name": "document.pdf",
     "file_type": "pdf",
     "enable_multimodel": true,
     "request_id": "trace-id"
   }
   ```

The handler returns immediately with the created `Knowledge` object (status `pending`). The actual parsing happens asynchronously.

### RBAC

- Write access requires `apiKeyIngest` permission (checked by `OwnedKBOrAdmin()` middleware).
- Read access requires `apiKeyRetrieve` permission.

---

## Phase 2: Asynchronous Document Processing

### Task Dispatch

The asynq task `document:process` is routed to `knowledgeService.ProcessDocument` (`internal/application/service/knowledge_process.go:3149`) via the task mux:

```go
// internal/router/task.go:265
mux.HandleFunc(types.TypeDocumentProcess, params.KnowledgeService.ProcessDocument)
```

### ProcessDocument Flow

```
ProcessDocument
  ├── Idempotency & status checks
  ├── Resolve EffectiveProcessConfig
  ├── convert()                          ← Step 1: Document parsing
  │     ├── resolveDocReader()           ← Select parser engine
  │     ├── reader.Read()                ← Parse file → markdown
  │     └── return ReadResult
  ├── Image resolution                   ← Step 2: Store images
  │     ├── imageResolver.ResolveAndStore()
  │     └── imageResolver.ResolveRemoteImages()
  ├── Chunking                           ← Step 3: Split into chunks
  │     ├── chunker.Split()              ← Flat chunking
  │     └── chunker.SplitParentChild()   ← Parent-child chunking
  └── processChunks()                    ← Step 4: Embed + index
        ├── CreateChunks()               ← Persist chunks to DB
        ├── BatchIndex()                 ← Embed + write to vector store
        ├── finalizeIndexedKnowledgeState()
        └── Enqueue post-process tasks   ← Summary, multimodal, etc.
```

#### Step 1: Document Parsing (`convert()`)

Location: `knowledge_process.go:3565`

The `convert()` method resolves the appropriate `DocReader` implementation based on the parser engine configuration and file type:

| Parser Engine       | Implementation                                    | Protocol   |
|---------------------|---------------------------------------------------|------------|
| `grpc` (default)    | `GRPCDocumentReader` (`docparser/grpc_parser.go`) | gRPC       |
| `http`              | `HTTPDocumentReader` (`docparser/http_parser.go`) | HTTP       |
| `builtin`           | `SimpleFormatReader` (`docparser/builtin_converter.go`) | In-process |
| `mineru`            | `MinerUReader` (`docparser/mineru_converter.go`)  | HTTP       |
| `mineru_cloud`      | `MinerUCloudReader` (`docparser/mineru_cloud_converter.go`) | HTTP |
| `paddleocr_vl`      | `PaddleOCRVLReader` (`docparser/paddleocr_vl_converter.go`) | Local |
| `paddleocr_vl_cloud`| `PaddleOCRVLCloudReader` (`docparser/paddleocr_vl_cloud_converter.go`) | HTTP |
| `weknoracloud`      | `WeKnoraCloudSignedDocumentReader` (`docparser/weknoracloud_http_reader.go`) | HTTP |

All readers implement the `interfaces.DocReader` interface:
```go
type DocReader interface {
    Read(ctx context.Context, req *types.ReadRequest) (*types.ReadResult, error)
}
```

The `ReadRequest` carries:
- `FileContent` (bytes), `FileName`, `FileType` for file imports
- `URL` for web page imports
- `ParserEngine` and `ParserEngineOverrides` for engine-specific tuning

The `ReadResult` returns:
- `MarkdownContent`: The parsed document as markdown text
- `ImageRefs`: Extracted images with byte data or base64
- `IsAudio` / `AudioData`: For audio file transcription
- `Metadata`: Page count, title, etc.

The DocReader call is wrapped in `callDocReaderWithTimeout()` with a configurable timeout (default 30 minutes) to prevent hung parsers from blocking workers indefinitely.

#### Step 1.5: Audio Transcription (if applicable)

If the parsed result indicates an audio file (`IsAudio = true`), the system:
1. Resolves the ASR model from `EffectiveProcessConfig.ASRConfig`
2. Calls `asrModel.Transcribe()` to convert speech to text
3. Replaces `convertResult.MarkdownContent` with the transcription

#### Step 2: Image Resolution and Storage

If `imageResolver` is available and the document contains images:

1. **`ResolveAndStore()`**: Processes inline/base64 images from the DocReader output, uploads them to object storage, and rewrites markdown image references to point to the stored URLs.
2. **`ResolveRemoteImages()`**: Downloads remote `http(s)` image URLs found in the markdown, uploads them to storage, and replaces the URLs.

The stored image metadata (`StoredImage` slice) is passed downstream for multimodal processing.

#### Step 3: Chunking

Location: `internal/infrastructure/chunker/`

The markdown content is split into chunks using the Go-native chunker:

**Flat chunking** (`chunker.Split()`):
- Splits by heading hierarchy and paragraph boundaries
- Respects `ChunkingConfig` settings: `chunk_size`, `chunk_overlap`, `separator`
- Each chunk carries `Content`, `ContextHeader` (heading breadcrumb), `Seq`, `Start`, `End`

**Parent-child chunking** (`chunker.SplitParentChild()`):
- Creates large parent chunks and smaller child chunks
- Each child references a `ParentIndex` for context enrichment
- Parent chunks are stored in the DB but NOT embedded into the vector index
- Child chunks are embedded and indexed for retrieval

Line endings are normalized via `chunker.NormalizeLineEndings()` before splitting.

#### Step 4: Chunk Processing (`processChunks()`)

Location: `knowledge_process.go:244`

This is the core indexing step:

1. **Idempotent cleanup**: Deletes any existing chunks and index data for the knowledge (supports re-processing).

2. **Chunk object creation**: Converts `ParsedChunk` protos into `types.Chunk` database objects with:
   - UUID, tenant/knowledge/KB foreign keys
   - `ChunkType`: `text` (normal), `parent_text` (parent in parent-child mode)
   - `ParentChunkID` linkage for child chunks
   - `PreChunkID` / `NextChunkID` doubly-linked list (flat mode only)

3. **Persist chunks**: `chunkService.CreateChunks()` writes all chunks (text + parent) to the database.

4. **Build index info**: For each text/child chunk, an `IndexInfo` is created with:
   - `Content`: The chunk text prepended with the document title and context header (`buildKnowledgeIndexContent()`)
   - `SourceID`, `ChunkID`, `KnowledgeID`, `KnowledgeBaseID`

5. **Storage quota check**: `retrieveEngine.EstimateStorageSize()` calculates the vector storage needed; the system verifies the tenant has sufficient quota.

6. **Batch vector indexing**: `retrieveEngine.BatchIndex()`:
   - Generates embeddings for all `IndexInfo.Content` using the KB's embedding model
   - Writes vectors + metadata to the configured vector store (Milvus, Qdrant, pgvector, sqlite-vec, etc.)
   - Also writes keyword index entries (for BM25/hybrid search)

7. **Finalize state**: `finalizeIndexedKnowledgeState()` sets:
   - `enable_status = "enabled"` (document is now searchable)
   - `parse_status = "processing"` (if multimodal or text chunks exist, awaiting post-process)
   - `parse_status = "completed"` (if no further enrichment needed)

8. **Post-process enqueue**: If no multimodal tasks are pending, a `TypeKnowledgePostProcess` task is enqueued for:
   - Summary generation (LLM-based document description)
   - Question generation (optional, for FAQ enrichment)
   - Graph extraction (knowledge graph from entities)

   If multimodal images exist, image processing tasks are enqueued first; the post-process task is enqueued after they complete.

### Stage Tracking

Throughout the pipeline, stage transitions are tracked via `beginStage` / `endStage` / `failStage` / `skipStage` for observability:

| Stage              | Description                          |
|--------------------|--------------------------------------|
| `StageDocReader`   | Document parsing by DocReader        |
| `StageChunking`    | Chunk creation and DB persistence    |
| `StageEmbedding`   | Vector embedding and index writing   |
| `StageMultimodal`  | Image OCR/caption processing         |

### Abort Handling

At multiple checkpoints, the pipeline checks `isKnowledgeAborted()` to detect user-initiated cancellation or deletion. If detected:
- **Deleting**: Already-written chunks and index entries are cleaned up
- **Cancelled**: Persisted data is kept; processing stops gracefully

---

## Phase 3: Intelligent Q&A Retrieval

### API Endpoints

| Endpoint                                    | Handler                          | Description                        |
|---------------------------------------------|----------------------------------|------------------------------------|
| `POST /knowledge-chat/:session_id`          | `Handler.KnowledgeQA`            | RAG Q&A with LLM summarization    |
| `POST /agent-chat/:session_id`              | `Handler.AgentQA`                | Agent-based Q&A with tool calling |
| `POST /knowledge-bases/:id/search`          | `KnowledgeBaseHandler.HybridSearch` | Pure search (no LLM)           |
| `POST /sessions/:id/search-knowledge`       | `Handler.SearchKnowledge`        | Search-only (no LLM summary)      |

### KnowledgeQA Pipeline

Location: `internal/application/service/session_knowledge_qa.go:23`

The RAG pipeline is assembled dynamically based on the request scope:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        RAG Pipeline (needsRAG = true)                   │
│                                                                         │
│  LOAD_HISTORY ──▶ QUERY_UNDERSTAND ──▶ CHUNK_SEARCH_PARALLEL ──▶       │
│                                                                       │
│  CHUNK_RERANK ──▶ [WEB_FETCH] ──▶ CHUNK_MERGE ──▶ FILTER_TOP_K ──▶   │
│                                                                       │
│  [DATA_ANALYSIS] ──▶ INTO_CHAT_MESSAGE ──▶ CHAT_COMPLETION_STREAM     │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────┐
│        Pure Chat Pipeline (needsRAG = false)  │
│                                               │
│  [LOAD_HISTORY] ──▶ CHAT_COMPLETION_STREAM   │
└──────────────────────────────────────────────┘
```

#### Stage Details

1. **LOAD_HISTORY**: Loads recent chat history (up to `max_rounds` turns) for multi-turn context.

2. **QUERY_UNDERSTAND**: Optionally rewrites the user query for better retrieval:
   - Query rewriting (`EnableRewrite`): Uses an LLM to reformulate the query
   - VLM image description: If images are attached, a vision model describes them

3. **CHUNK_SEARCH_PARALLEL**: The core retrieval stage, handled by `PluginSearch` (`chat_pipeline/search.go`):
   - Groups search targets by embedding model (same model → shared query embedding)
   - Computes query embedding once per model group
   - Runs KB search and web search concurrently
   - **KB search** calls `knowledgeBaseService.HybridSearch()` per target group
   - **Web search** (if enabled) calls the configured web search provider

4. **CHUNK_RERANK**: Reranks search results using a cross-encoder rerank model for improved relevance. Also applies wiki-boost scoring.

5. **WEB_FETCH** (optional): Fetches and extracts content from web search result URLs.

6. **CHUNK_MERGE**: Merges results from all sources (KB + web), deduplicates by chunk ID and content signature, and removes partial overlaps (≥85% token overlap).

7. **FILTER_TOP_K**: Keeps only the top-K results by score, applying the configured `RerankTopK` limit.

8. **DATA_ANALYSIS** (optional): Runs data analysis on structured results.

9. **INTO_CHAT_MESSAGE**: Assembles the final prompt:
   - Injects retrieved chunk content as context
   - Prepends the system prompt with KB-specific instructions
   - Includes chat history
   - Adds the user query

10. **CHAT_COMPLETION_STREAM**: Streams the LLM response via SSE, emitting `EventAgentFinalAnswer` events chunk by chunk.

### HybridSearch Internals

Location: `internal/application/service/knowledgebase_search.go:87`

`HybridSearch` is the core retrieval method:

1. **KB resolution**: Loads all KB records, validates embedding model consistency across multi-KB searches, and authorizes cross-tenant access.

2. **Query embedding**: Computes the query vector once using the KB's embedding model (unless pre-computed by the caller).

3. **Store grouping**: KBs sharing the same vector store are grouped for combined retrieval (reduces round-trips).

4. **Fan-out retrieval**: For each store group, calls `retrieveEngine.Retrieve()` which performs:
   - **Vector search**: Cosine similarity between query embedding and chunk embeddings
   - **Keyword search**: BM25-style matching on chunk content
   - Results are returned with scores from each retriever type

5. **Score fusion**: Vector and keyword results are classified, deduplicated, and fused using Reciprocal Rank Fusion (RRF) or score-based merging.

6. **Context enrichment**: Each result is enriched with its parent chunk content (if parent-child chunking is enabled) and surrounding chunk context.

### SearchKnowledge (Search-Only)

Location: `session_knowledge_qa.go:800`

The `SearchKnowledge` method runs only the retrieval stages (no LLM summarization):
```
CHUNK_SEARCH → CHUNK_RERANK → CHUNK_MERGE → FILTER_TOP_K
```
Returns raw `SearchResult` objects with chunk content, scores, and metadata.

### Fallback Strategy

When no relevant chunks are found (`ErrSearchNothing`), the system applies a fallback:
- **Fixed**: Returns a pre-configured `FallbackResponse` string
- **Model**: Uses the LLM with a `FallbackPrompt` template to generate a contextual response

---

## Key Data Structures

### Knowledge

Represents a single document in a knowledge base:

| Field              | Description                                    |
|--------------------|------------------------------------------------|
| `ID`               | UUID primary key                               |
| `TenantID`         | Owning tenant                                  |
| `KnowledgeBaseID`  | Parent knowledge base                          |
| `Type`             | `file`, `url`, `passage`                       |
| `ParseStatus`      | `pending` → `processing` → `completed`/`failed`/`cancelled`/`deleting` |
| `EnableStatus`     | `disabled` → `enabled`                         |
| `SummaryStatus`    | `none` → `pending` → `completed`/`failed`      |
| `FilePath`         | Object storage path                            |
| `FileName`         | Original file name                             |
| `FileType`         | File extension (pdf, docx, etc.)               |
| `EmbeddingModelID` | Model used for vectorization                   |
| `StorageSize`      | Estimated vector storage bytes                 |

### Chunk

Represents a text segment of a document:

| Field              | Description                                    |
|--------------------|------------------------------------------------|
| `ID`               | UUID primary key                               |
| `KnowledgeID`      | Parent knowledge document                      |
| `Content`          | Chunk text content                             |
| `ContextHeader`    | Heading breadcrumb for context                 |
| `ChunkIndex`       | Sequential position in document                |
| `ChunkType`        | `text`, `parent_text`, `image_ocr`, `image_caption` |
| `ParentChunkID`    | Link to parent chunk (parent-child mode)       |
| `PreChunkID`       | Previous chunk in sequence                     |
| `NextChunkID`      | Next chunk in sequence                         |
| `StartAt` / `EndAt`| Character offset range in source document      |

### SearchResult

Represents a retrieved chunk with relevance scoring:

| Field              | Description                                    |
|--------------------|------------------------------------------------|
| `ID`               | Chunk ID                                       |
| `Content`          | Chunk text                                     |
| `Score`            | Relevance score (0.0–1.0)                      |
| `MatchType`        | `vector`, `keyword`, `hybrid`, `history`       |
| `KnowledgeID`      | Source document                                |
| `KnowledgeBaseID`  | Source knowledge base                          |
| `Context`          | Surrounding chunk content for enrichment       |

---

## Configuration and Extensibility

### Parser Engine Selection

The parser engine is resolved per file type via `ChunkingConfig.ResolveParserEngine(fileType)` with optional `ParserEngineRules` for type-specific overrides. Tenant-level and upload-level overrides are merged via `MergeParserEngineOverrides()`.

### Chunking Configuration

```yaml
chunking:
  chunk_size: 500
  chunk_overlap: 50
  separator: "\n"
  enable_parent_child: false
  parent_chunk_size: 1500
  child_chunk_size: 300
```

### Retrieval Configuration

Tenant-level `RetrievalConfig` controls retrieval parameters:

| Parameter            | Default | Description                        |
|----------------------|---------|------------------------------------|
| `embedding_top_k`    | 5       | Number of chunks to retrieve       |
| `vector_threshold`   | 0.2     | Minimum vector similarity score    |
| `keyword_threshold`  | 0.3     | Minimum keyword match score        |
| `rerank_top_k`       | 5       | Chunks after reranking             |
| `rerank_threshold`   | 0.0     | Minimum rerank score               |
| `rerank_model_id`    | —       | Cross-encoder model for reranking  |

### Vector Store Backends

The `RetrieveEngineRegistry` supports multiple backends, resolved per-KB via `VectorStoreID`:

- **sqlite-vec**: Embedded vector search (default for small deployments)
- **Milvus**: Distributed vector database
- **Qdrant**: Vector similarity search engine
- **pgvector**: PostgreSQL vector extension
- **Elasticsearch**: Full-text + vector hybrid search

The `KeywordsVectorHybridRetrieveEngineService` (`retriever/keywords_vector_hybrid_indexer.go`) combines vector and keyword (BM25) indices in a single engine, with concurrent batch indexing and score normalization.

### Asynq Task Configuration

Document processing tasks use configurable retry and timeout:

```go
asynq.NewTask(types.TypeDocumentProcess, payloadBytes,
    asynq.MaxRetry(3),
    asynq.Queue(types.QueueDefault),
    asynq.Timeout(60 * time.Minute),
)
```

The `ProcessDocument` handler is idempotent: if the knowledge is already `completed`, it returns immediately. Failed tasks can be retried up to `MaxRetry` before marking the knowledge as permanently failed.
