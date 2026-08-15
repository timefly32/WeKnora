# Knowledge-Based Quick Q\&A Flow

This document provides a detailed, step-by-step description of how a user's question is processed through the knowledge-based quick Q\&A pipeline (also called "KnowledgeQA" or "normal mode"). It traces the full lifecycle from the HTTP request arriving at the handler to the streaming SSE response reaching the frontend.

***

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Step 1: HTTP Request and Handler](#2-step-1-http-request-and-handler)
3. [Step 2: Service Setup and Pipeline Assembly](#3-step-2-service-setup-and-pipeline-assembly)
4. [Step 3: Pipeline Execution — Stage by Stage](#4-step-3-pipeline-execution--stage-by-stage)
   - [3.1 LOAD\_HISTORY](#31-load_history)
   - [3.2 QUERY\_UNDERSTAND](#32-query_understand)
   - [3.3 CHUNK\_SEARCH\_PARALLEL](#33-chunk_search_parallel)
   - [3.4 CHUNK\_RERANK](#34-chunk_rerank)
   - [3.5 WEB\_FETCH (conditional)](#35-web_fetch-conditional)
   - [3.6 CHUNK\_MERGE](#36-chunk_merge)
   - [3.7 FILTER\_TOP\_K](#37-filter_top_k)
   - [3.8 DATA\_ANALYSIS (conditional)](#38-data_analysis-conditional)
   - [3.9 INTO\_CHAT\_MESSAGE](#39-into_chat_message)
   - [3.10 CHAT\_COMPLETION\_STREAM](#310-chat_completion_stream)
5. [Fallback Handling](#5-fallback-handling)
6. [SSE Event Protocol](#6-sse-event-protocol)
7. [Key Data Structures](#7-key-data-structures)
8. [Configuration Reference](#8-configuration-reference)

***

## 1. Architecture Overview

The KnowledgeQA pipeline follows a **plugin-based event-driven architecture**. Each pipeline stage is implemented as a `Plugin` that registers for specific `EventType`s. The `EventManager` builds a handler chain per event type and triggers them sequentially.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         KnowledgeQA Pipeline (RAG mode)                      │
│                                                                              │
│  ┌──────────────┐   ┌───────────────────┐   ┌───────────────────────────┐   │
│  │ LOAD_HISTORY │──▶│ QUERY_UNDERSTAND  │──▶│ CHUNK_SEARCH_PARALLEL    │   │
│  └──────────────┘   └───────────────────┘   └───────────────────────────┘   │
│                                                      │                      │
│                                                      ▼                      │
│  ┌──────────────┐   ┌──────────────┐   ┌───────────────────────────┐       │
│  │ FILTER_TOP_K │◀──│ CHUNK_MERGE  │◀──│ [WEB_FETCH] + CHUNK_RERANK│       │
│  └──────┬───────┘   └──────────────┘   └───────────────────────────┘       │
│         │                                                                   │
│         ▼                                                                   │
│  ┌──────────────────┐   ┌──────────────────────┐   ┌────────────────────┐  │
│  │ [DATA_ANALYSIS]  │──▶│ INTO_CHAT_MESSAGE    │──▶│CHAT_COMPLETION_STREAM│ │
│  └──────────────────┘   └──────────────────────┘   └────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Pure chat mode** (no KB, no web search) uses a simplified pipeline:

```
[LOAD_HISTORY] → [CHAT_COMPLETION_STREAM]
```

***

## 2. Step 1: HTTP Request and Handler

### API Endpoint

```
POST /knowledge-chat/:session_id
Content-Type: application/json
Accept: text/event-stream
```

### Handler: `Handler.KnowledgeQA`

Location: `internal/handler/session/qa.go:746`

The handler performs these steps:

#### 2.1 Parse and Validate Request (`parseQARequest`)

The handler parses the JSON body into a `CreateKnowledgeQARequest` struct:

| Field                | Type        | Description                                      |
| -------------------- | ----------- | ------------------------------------------------ |
| `query`              | String      | The user's question (required)                   |
| `knowledge_base_ids` | \[]String   | KB IDs to search (optional, from session config) |
| `knowledge_ids`      | \[]String   | Specific document IDs to search                  |
| `tag_scopes`         | \[]TagScope | Tag-based scope constraints                      |
| `agent_enabled`      | Bool        | Whether agent mode is requested                  |
| `agent_id`           | String      | Custom agent ID                                  |
| `web_search_enabled` | Bool        | Enable web search alongside KB search            |
| `images`             | \[]String   | Image URLs attached to the question              |
| `attachments`        | \[]Attach   | Inline file attachments (base64)                 |
| `attachment_ids`     | \[]String   | Pre-uploaded session-scoped document IDs         |
| `disable_title`      | Bool        | Skip auto title generation                       |

The handler also:

- Resolves the session by `session_id` (returns 404 if not found)
- Resolves the custom agent if `agent_id` is provided
- Determines the effective tenant ID (for shared-agent scenarios)
- Extracts `@mention` items (KBs, knowledge files, tags, MCP services)

#### 2.2 Create User and Assistant Messages

Two messages are persisted to the database before the pipeline starts:

1. **User message**: Contains the query text, image attachments, and document metadata
2. **Assistant message**: Created with empty content, to be filled by the streaming response

#### 2.3 Setup SSE Stream

The handler configures a Server-Sent Events (SSE) stream:

- Sets `Content-Type: text/event-stream` and SSE-specific headers
- Creates an `EventBus` for the session
- Registers event listeners on the `EventBus`:
  - `EventAgentThought` → accumulates reasoning/thinking content
  - `EventAgentFinalAnswer` → accumulates answer content, marks completion on `Done=true`
  - `EventAgentComplete` → emitted when the final answer is done

#### 2.4 Launch Async Pipeline Execution

The QA service is invoked in a **goroutine** so the handler can immediately start forwarding SSE events:

```go
go func() {
    h.resolveTemporaryAttachments(streamCtx, reqCtx)
    h.runVLMAnalysisIfNeeded(streamCtx, reqCtx, mode)
    qaReq := reqCtx.buildQARequest()
    h.sessionService.KnowledgeQA(streamCtx.asyncCtx, qaReq, streamCtx.eventBus)
}()
```

The main goroutine blocks on `handleAgentEventsForSSE()`, which reads events from the `EventBus` and writes them as SSE frames to the HTTP response writer.

***

## 3. Step 2: Service Setup and Pipeline Assembly

### Service: `sessionService.KnowledgeQA`

Location: `internal/application/service/session_knowledge_qa.go:23`

#### 3.1 Resolve Knowledge Bases

`resolveKnowledgeBases(ctx, req)` determines which KBs to search:

- If `req.CustomAgent` is set, uses the agent's `KBSelectionMode`:
  - `"all"`: loads all tenant KBs (+ shared KBs if not a shared-agent)
  - `"selected"`: uses the agent's configured `KnowledgeBases` list
  - `"none"`: no KBs
- Otherwise, uses the request's `KnowledgeBaseIDs` and `KnowledgeIDs` directly

#### 3.2 Resolve Chat Model

`resolveChatModelID(ctx, req, knowledgeBaseIDs, knowledgeIDs)` selects the LLM:

1. If the session has a `SummaryModelID` that is a Remote model → use it
2. If any KB has a Remote `SummaryModelID` → use it
3. Fall back to the session's `SummaryModelID`
4. Fall back to the first KB's `SummaryModelID`
5. Last resort: find any available `ModelTypeKnowledgeQA` model

#### 3.3 Build Search Targets

`buildSearchTargets(ctx, tenantID, knowledgeBaseIDs, knowledgeIDs, tagScopes)` creates a unified list of `SearchTarget` objects:

- **Full KB targets** (`SearchTargetTypeKnowledgeBase`): search all documents in a KB
- **Specific knowledge targets** (`SearchTargetTypeKnowledge`): search only specified documents within a KB
- **Tag-scoped targets**: search documents matching specific tags within a KB

Each target carries:

- `KnowledgeBaseID`, `TenantID` (resolved for cross-tenant access)
- `KnowledgeIDs` (for specific-file targets)
- `TagIDs`, `ScopeTagIDs` (for tag-scoped targets)
- `DisableRecallThresholds` (set true for explicit scopes)

#### 3.4 Assemble ChatManage Object

The `ChatManage` struct is the **shared mutable state** that flows through all pipeline stages:

```go
chatManage := &types.ChatManage{
    PipelineRequest: { /* query, thresholds, model IDs, search targets */ },
    PipelineState:   { /* RewriteQuery, ImageDescription, QuotedContext */ },
    PipelineContext:  { /* EventBus, MessageID, UserMessageID */ },
}
```

Key fields initialized:

- `RewriteQuery = req.Query` (may be overwritten by QUERY\_UNDERSTAND)
- `VectorThreshold`, `KeywordThreshold`, `EmbeddingTopK`, `RerankTopK`, `RerankThreshold` from config
- `SummaryConfig` (system prompt, context template, temperature, etc.)
- `FallbackStrategy` and `FallbackResponse`/`FallbackPrompt`

#### 3.5 Determine Pipeline Stages

The pipeline is dynamically assembled based on the request scope:

```go
hasKB := HasKnowledgeRetrievalScope(searchTargets, knowledgeBaseIDs, knowledgeIDs)
needsRAG := hasKB || req.WebSearchEnabled

if !needsRAG {
    // Pure chat
    pipeline = [LOAD_HISTORY, CHAT_COMPLETION_STREAM]
} else {
    // RAG
    pipeline = [LOAD_HISTORY, QUERY_UNDERSTAND, CHUNK_SEARCH_PARALLEL,
                CHUNK_RERANK, WEB_FETCH?, CHUNK_MERGE, FILTER_TOP_K,
                DATA_ANALYSIS?, INTO_CHAT_MESSAGE, CHAT_COMPLETION_STREAM]
}
```

Conditional stages:

- `WEB_FETCH`: only when `req.WebSearchEnabled` is true
- `DATA_ANALYSIS`: only when `chatManage.DataAnalysisEnabled` is true

#### 3.6 Execute Pipeline

`KnowledgeQAByEvent(ctx, chatManage, pipeline)` iterates through the event list:

```go
for _, eventType := range eventList {
    err := s.eventManager.Trigger(ctx, eventType, chatManage)
    if err == ErrSearchNothing {
        s.handleFallbackResponse(ctx, chatManage)
        return nil
    }
    if err != nil {
        return err.Err
    }
}
```

Each `Trigger` call invokes the registered plugin's `OnEvent` method, which reads from and writes to the shared `chatManage` object.

***

## 4. Step 3: Pipeline Execution — Stage by Stage

### 3.1 LOAD\_HISTORY

**Plugin**: `PluginLoadHistory` (`chat_pipeline/load_history.go`)

**Purpose**: Load recent conversation history for multi-turn context.

**Behavior**:

1. If `MaxRounds <= 0` (multi-turn disabled by agent config), skip entirely
2. Call `loadAndProcessHistory(ctx, messageService, sessionID, maxRounds, fetchCount)`:
   - Fetches the last `fetchCount` messages from the database
   - Groups them into Q\&A pairs by `RequestID`
   - Strips `<think>` tags from assistant answers
   - Sorts by recency, limits to `maxRounds`
   - Reverses to chronological order
3. Stores result in `chatManage.History`

**Output**: `chatManage.History` — a `[]*types.History` slice of Q\&A pairs.

***

### 3.2 QUERY\_UNDERSTAND

**Plugin**: `PluginQueryUnderstand` (`chat_pipeline/query_understand.go`)

**Purpose**: Rewrite the user query for better retrieval and classify the query intent.

**Behavior**:

1. **Skip condition**: If `EnableRewrite` is false AND no images are attached, skip (pass through original query)
2. **Load history**: If not already loaded by LOAD\_HISTORY, fetch history for rewrite context
3. **Select model**:
   - If images are present and the chat model supports vision → use chat model with images
   - If images are present and a VLM model is configured → use VLM model
   - Otherwise → use the chat model (or a dedicated `QueryUnderstandModelID` if configured)
4. **Build prompts**:
   - System prompt: `RewritePromptSystem` (from config or agent override)
   - User prompt: `RewritePromptUser` with placeholders replaced:
     - `{{conversation}}` → formatted history
     - `{{query}}` → original query + image/attachment metadata
     - `{{language}}` → response language
5. **Call LLM**: Temperature 0.3, max 150 tokens (500 for images)
6. **Parse structured output**: Expected JSON format:
   ```json
   {
     "rewrite_query": "optimized search query",
     "intent": "kb_search",
     "image_description": "description of uploaded images"
   }
   ```
   - If JSON parsing fails, the raw text is treated as the rewritten query
   - Intent values: `kb_search` (needs retrieval), `general_chat` (no retrieval needed)
7. **Update chatManage**:
   - `chatManage.RewriteQuery` = rewritten query
   - `chatManage.Intent` = classified intent
   - `chatManage.ImageDescription` = image description (if any)
8. **Intent-based prompt override**: If intent is `general_chat` (no retrieval needed), apply an intent-specific system prompt override from `IntentSystemPrompts` config

**Output**: `chatManage.RewriteQuery`, `chatManage.Intent`, `chatManage.ImageDescription`

***

### 3.3 CHUNK\_SEARCH\_PARALLEL

**Plugin**: `PluginSearchParallel` (`chat_pipeline/search_parallel.go`)

**Purpose**: Perform parallel chunk search and entity (knowledge graph) search.

**Behavior**:

1. **Intent-based skip**: If `chatManage.NeedsRetrieval()` returns false (intent was `general_chat`), skip this stage entirely
2. **Deep-copy chatManage**: Creates two independent copies — one for chunk search, one for entity search — to avoid concurrent read/write on shared slices
3. **Run two tasks in parallel**:
   #### Task A: Chunk Search (`PluginSearch.OnEvent`)
   Location: `chat_pipeline/search.go`

   a. **Concurrent KB + Web search**:
   - **KB search** (`searchByTargets`):
     - Batch-fetch KB records to determine embedding model grouping
     - Resolve actual model identities (name + endpoint) so cross-tenant KBs sharing the same physical model share one embedding computation
     - Group targets by embedding model key
     - For each model group:
       - Compute query embedding **once** via `GetQueryEmbedding(ctx, kbID, queryText)`
       - If embedding fails, degrade to keyword-only search for targets with keyword index; report error for vector-only targets
       - Separate full-KB targets (combined into one `HybridSearch` call) from specific-knowledge targets (individual calls)
       - Run combined and individual searches concurrently within the group
   - **Web search** (`searchWebIfEnabled`):
     - If `WebSearchEnabled` and a provider is configured, call `webSearchService.Search()`
     - Convert web results to `SearchResult` objects with `MatchType = "web_search"`
   b. **Query expansion** (if enabled and recall is low):
   - If `EnableQueryExpansion` is true and result count < `EmbeddingTopK`
   - Generates expanded query variants and runs additional searches
   c. **Result**: `chatManage.SearchResult` populated with all hits
   #### Task B: Entity Search (`PluginSearchEntity.OnEvent`)
   Location: `chat_pipeline/search_entity.go`
   - Only runs if `chatManage.Entity` is non-empty (entities extracted by QUERY\_UNDERSTAND or pre-configured)
   - Searches the knowledge graph for matching nodes/relations
   - Loads associated chunks from the graph results
   - Converts chunks to `SearchResult` objects with `MatchType = "graph"`
4. **Merge results**: Combine chunk and entity search results, deduplicate by chunk ID and content signature
5. **Empty result handling**: If no results, return `ErrSearchNothing` (triggers fallback)

**Output**: `chatManage.SearchResult` — deduplicated `[]*types.SearchResult`

#### HybridSearch Internals

Location: `internal/application/service/knowledgebase_search.go:87`

When `searchByTargets` calls `knowledgeBaseService.HybridSearch()`, the following happens:

1. **Batch-load KB records** and authorize access (same-tenant OK; cross-tenant requires Organization share permission)
2. **Validate embedding model consistency** across multi-KB searches
3. **Compute query embedding** once (if not pre-computed by the caller)
4. **Group KBs by vector store** (KBs sharing the same store can be searched in one call)
5. **Fan-out retrieval**: For each store group, call `retrieveEngine.Retrieve()`:
   - **Vector search**: Cosine similarity between query embedding and chunk embeddings
   - **Keyword search**: BM25-style matching on chunk content
6. **Score fusion**: Classify results as vector/keyword, deduplicate, and fuse using Reciprocal Rank Fusion (RRF)
7. **FAQ post-processing**: If the primary KB is FAQ-type, apply iterative TopK growth
8. **Context enrichment**: Load surrounding chunk content for each result (unless `SkipContextEnrichment`)

***

### 3.4 CHUNK\_RERANK

**Plugin**: `PluginRerank` (`chat_pipeline/rerank.go`)

**Purpose**: Re-score search results using a cross-encoder rerank model for improved relevance.

**Behavior**:

1. **Skip conditions**: If `NeedsRetrieval()` is false, no results, or no rerank model configured → skip
2. **Prepare passages**: For each search result, build an enriched passage by:
   - Cleaning markdown formatting (strip images, links, code fences, table separators, headings, etc.)
   - Appending image OCR/caption text and generated questions from chunk metadata
3. **Call rerank model**: `rerankModel.Rerank(ctx, query, passages)`
   - Returns `[]RankResult` with `Index` and `RelevanceScore`
4. **Threshold filtering**: Keep only results with `RelevanceScore >= RerankThreshold`
   - If all results are filtered out but the top score ≥ 0.15 (or 0 for explicit scopes), keep the top-1 as a safety net
5. **Threshold degradation**: If no results pass the threshold and the original threshold > 0.3, retry with `threshold * 0.7` (floor 0.3)
6. **Composite scoring**: `compositeScore = 0.6 * modelScore + 0.3 * baseScore + 0.1 * sourceWeight`
   - Web search results get `sourceWeight = 0.95`; KB results get `1.0`
7. **FAQ score boost**: If `FAQPriorityEnabled` and the chunk is FAQ-type, multiply score by `FAQScoreBoost` (capped at 1.0)
8. **MMR (Maximal Marginal Relevance)**: Apply MMR to select `RerankTopK` results with diversity:
   - `MMR = λ * relevance - (1-λ) * max_redundancy` (λ = 0.7)
   - Redundancy measured by Jaccard similarity of token sets
   - Iteratively selects the result with highest MMR until `RerankTopK` results are chosen

**Output**: `chatManage.RerankResult` — top-K reranked `[]*types.SearchResult`

***

### 3.5 WEB\_FETCH (conditional)

**Plugin**: `PluginWebFetch` (`chat_pipeline/web_fetch.go`)

**Purpose**: Fetch full page content for top web search results, replacing snippet text.

**Behavior**:

1. Only runs when `WebFetchEnabled` AND `WebSearchEnabled`
2. Selects top-N web results from `chatManage.RerankResult` (default N=3)
3. Fetches each URL in parallel using `web_fetch.FetchURLContent()`
4. Replaces snippet content with fetched full content (truncated to 8000 chars)
5. Failed fetches are silently skipped (snippet content retained)

**Output**: Updated `chatManage.RerankResult` with enriched web result content

***

### 3.6 CHUNK\_MERGE

**Plugin**: `PluginMerge` (`chat_pipeline/merge.go`)

**Purpose**: Merge, deduplicate, and enrich search result chunks for optimal LLM context.

**Behavior** (8-step pipeline):

1. **Select input**: Use `RerankResult` if available; otherwise fall back to `SearchResult` sorted by score
2. **Initial dedup**: Remove duplicates by chunk ID and content signature
3. **Inject history references**: Load relevant references from conversation history:
   - Filter by Jaccard similarity ≥ 0.15 between query and chunk content
   - Discount score by 0.6 to rank below fresh results
   - Cap at 3 history results
   - Deduplicate against current results
4. **Resolve parent chunks**: For parent-child chunking mode:
   - Batch-fetch parent chunks from DB
   - For text children: expand content to include full parent text, scope image info to the matched child only
   - For image children (OCR/caption): resolve the text parent and grandparent, merge their content
   - Mark `ContentRewritten = true` and record `SubChunkID`
5. **Group and merge sequential content**: Group chunks by `KnowledgeID + ChunkType`, then within each group:
   - Sort by `ChunkIndex`
   - Merge sequential chunks whose content overlaps or is adjacent into a single result
6. **Populate FAQ answers**: For FAQ-type chunks, load the full Q\&A pair from chunk metadata and replace the content with the formatted answer
7. **Expand short contexts**: For text chunks shorter than 350 characters:
   - Load neighboring chunks (previous/next) from DB
   - Merge neighbor content up to 850 characters total
   - Re-merge any sequential or contained bodies introduced by expansion
8. **Final dedup**: Remove duplicates by ID + content signature + partial content overlap (≥85% token overlap)

**Output**: `chatManage.MergeResult` — merged, enriched `[]*types.SearchResult`

***

### 3.7 FILTER\_TOP\_K

**Plugin**: `PluginFilterTopK` (`chat_pipeline/filter_top_k.go`)

**Purpose**: Truncate results to the configured top-K limit.

**Behavior**:

1. Sort results deterministically by score (descending), then by `KnowledgeID`, `ChunkType`, `ChunkIndex`, `ID` as tiebreakers
2. If `RerankTopK > 0` and result count exceeds it, truncate to `RerankTopK`
3. Applies to `MergeResult` > `RerankResult` > `SearchResult` (first non-empty wins)

**Output**: Truncated `chatManage.MergeResult` (or `RerankResult`/`SearchResult`)

***

### 3.8 DATA\_ANALYSIS (conditional)

**Plugin**: `PluginDataAnalysis` (`chat_pipeline/data_analysis.go`)

**Purpose**: Run structured data analysis on search results when enabled.

**Behavior**: Only activated when `chatManage.DataAnalysisEnabled` is true. Performs SQL-like analysis on structured data found in the search results.

***

### 3.9 INTO\_CHAT\_MESSAGE

**Plugin**: `PluginIntoChatMessage` (`chat_pipeline/into_chat_message.go`)

**Purpose**: Assemble the final prompt that will be sent to the LLM.

**Behavior**:

1. **FAQ priority separation**: If `FAQPriorityEnabled`:
   - Separate results into FAQ and document groups
   - Check for high-confidence FAQ (score ≥ `FAQDirectAnswerThreshold`)
2. **Input validation**: Sanitize the user query for safety
3. **No-retrieval path** (intent = `general_chat`):
   - Build user content from query + image description + quoted context + attachments
   - Render through `ContextTemplate` with empty contexts
   - Set `chatManage.UserContent` and return
4. **RAG path**:
   a. **Build document header**: List unique source documents with title, description, and metadata:
   ```xml
   <documents>
     <document>
       <title>...</title>
       <description>...</description>
       <metadata>...</metadata>
     </document>
   </documents>
   ```
   b. **Build contexts string**:
   - With FAQ priority:
     ```xml
     <source type="faq" priority="high">
       <context id="FAQ-1" match="exact">...</context>
     </source>
     <source type="document" priority="supplementary">
       <context id="DOC-1">...</context>
     </source>
     ```
   - Without FAQ priority:
     ```xml
     <context id="1">passage text</context>
     <context id="2">passage text</context>
     ```
   c. **Render context template**: Replace placeholders in `SummaryConfig.ContextTemplate`:
   - `{{query}}` → sanitized user query
   - `{{contexts}}` → rendered contexts XML
   - `{{language}}` → response language
   d. **Append multimodal content**:
   - Image description (if chat model doesn't support vision)
   - Quoted context
   - Attachment content
5. **Persist rendered content**: Asynchronously write the full RAG-augmented `UserContent` back to the user message in the database, so subsequent conversation turns can see the retrieval context in history

**Output**: `chatManage.UserContent` — the complete prompt string for the LLM

***

### 3.10 CHAT\_COMPLETION\_STREAM

**Plugin**: `PluginChatCompletionStream` (`chat_pipeline/chat_completion_stream.go`)

**Purpose**: Stream the LLM response to the client via SSE.

**Behavior**:

1. **Prepare chat model**: Resolve the chat model and configure options (temperature, max tokens, thinking mode, etc.)
2. **Prepare messages** (`prepareMessagesWithModelContext`):
   - Build the system prompt from `SummaryConfig.Prompt` (or `SystemPromptOverride` if set by intent logic)
   - Append the model context protocol prompt (for citation handle resolution)
   - Append history messages (Q\&A pairs from `chatManage.History`)
   - Append the current user message (`chatManage.UserContent`)
   - If the chat model supports vision and images are present, attach image URLs to the user message
   - Replace rendered contexts in the message with model-context tool results (citation handles)
3. **Emit knowledge references**: Before starting the stream, emit a `knowledge_references` SSE event so the frontend receives citation data while the stream is still open
4. **Start streaming**: Call `chatModel.ChatStream(ctx, messages, opt)` which returns a `<-chan StreamResponse`
5. **Consume stream in goroutine**:
   - `ResponseTypeThinking` → emit `EventAgentThought` (shown as "thinking" card in UI)
   - `ResponseTypeAnswer` → emit `EventAgentFinalAnswer` (shown as answer text)
   - `ResponseTypeError` → emit `EventError`
   - When `Done=true` on the answer, mark `answerCompleted`
   - On context cancellation, flush decoders and close thinking
6. **Stream decoders**: Both thinking and answer channels use `StreamDecoder` instances that buffer partial content for citation handle resolution (handles may span multiple provider chunks)

**Output**: SSE events streamed to the client in real-time

***

## 5. Fallback Handling

When any retrieval stage returns `ErrSearchNothing` (no relevant chunks found), the pipeline short-circuits and invokes `handleFallbackResponse`:

### Fixed Fallback

- Returns the pre-configured `FallbackResponse` string (e.g., "Sorry, I cannot answer this question based on the available knowledge.")
- Emitted as a single `EventAgentFinalAnswer` with `Done=true` and `IsFallback=true`

### Model Fallback

- Renders the `FallbackPrompt` template with:
  - `{{query}}` → the user's question
  - `{{kb_documents}}` → a listing of all documents in the KB (titles only)
  - `{{language}}` → response language
- Starts a streaming LLM call with the fallback prompt
- The LLM is instructed to use its general knowledge since no KB content matched
- Falls back to fixed response if the model call fails

***

## 6. SSE Event Protocol

The frontend receives the following SSE event types during a KnowledgeQA session:

| Event Type              | Data Shape                    | Description                          |
| ----------------------- | ----------------------------- | ------------------------------------ |
| `EventAgentThought`     | `{Content, Done}`             | Thinking/reasoning content           |
| `EventAgentFinalAnswer` | `{Content, Done, IsFallback}` | Answer text chunk (streamed)         |
| `EventAgentComplete`    | `{FinalAnswer}`               | Stream completed                     |
| `EventError`            | `{Error, Stage, SessionID}`   | Pipeline error                       |
| `knowledge_references`  | Citation metadata             | Source citations for the answer      |
| `progress`              | Stage progress info           | Retrieval/understand progress events |

The `EventAgentFinalAnswer` events are accumulated by the handler's event listener. When `Done=true`, the handler:

1. Persists the complete answer to the assistant message in the database
2. Emits `EventAgentComplete` to signal stream end
3. Optionally generates a session title (if `generateTitle=true` and the session has no title)

***

## 7. Key Data Structures

### ChatManage

The central state object shared across all pipeline stages:

| Field              | Type              | Set By                  | Description                     |
| ------------------ | ----------------- | ----------------------- | ------------------------------- |
| `Query`            | String            | Service setup           | Original user question          |
| `RewriteQuery`     | String            | QUERY\_UNDERSTAND       | Optimized query for retrieval   |
| `Intent`           | QueryIntent       | QUERY\_UNDERSTAND       | `kb_search` or `general_chat`   |
| `ImageDescription` | String            | QUERY\_UNDERSTAND       | VLM-generated image description |
| `History`          | \[]\*History      | LOAD\_HISTORY           | Conversation history Q\&A pairs |
| `SearchResult`     | \[]\*SearchResult | CHUNK\_SEARCH\_PARALLEL | Raw search hits                 |
| `RerankResult`     | \[]\*SearchResult | CHUNK\_RERANK           | Reranked top-K results          |
| `MergeResult`      | \[]\*SearchResult | CHUNK\_MERGE            | Merged and enriched results     |
| `RenderedContexts` | String            | INTO\_CHAT\_MESSAGE     | XML-formatted context for LLM   |
| `UserContent`      | String            | INTO\_CHAT\_MESSAGE     | Final prompt sent to LLM        |
| `SearchTargets`    | SearchTargets     | Service setup           | Unified search scope            |
| `EventBus`         | \*EventBus        | Handler                 | SSE event bus                   |

### SearchResult

Represents a single retrieved chunk:

| Field              | Description                                                              |
| ------------------ | ------------------------------------------------------------------------ |
| `ID`               | Chunk UUID                                                               |
| `Content`          | Chunk text (may be rewritten by merge)                                   |
| `Score`            | Relevance score (0.0–1.0)                                                |
| `MatchType`        | `vector`, `keyword`, `hybrid`, `graph`, `history`, `web_search`          |
| `KnowledgeID`      | Source document ID                                                       |
| `KnowledgeBaseID`  | Source KB ID                                                             |
| `KnowledgeTitle`   | Document title                                                           |
| `ChunkType`        | `text`, `parent_text`, `faq`, `image_ocr`, `image_caption`, `web_search` |
| `ParentChunkID`    | Parent chunk (parent-child mode)                                         |
| `ChunkIndex`       | Sequential position in document                                          |
| `ImageInfo`        | JSON array of image metadata (URL, caption, OCR)                         |
| `ContentRewritten` | True if merge stage modified the content                                 |
| `SubChunkID`       | Child chunk IDs included in merged content                               |

***

## 8. Configuration Reference

### Conversation Config (`config.yaml`)

| Parameter                       | Default | Description                              |
| ------------------------------- | ------- | ---------------------------------------- |
| `max_rounds`                    | 5       | Max history Q\&A rounds for context      |
| `vector_threshold`              | 0.2     | Min vector similarity score              |
| `keyword_threshold`             | 0.3     | Min keyword match score                  |
| `embedding_top_k`               | 5       | Number of chunks to retrieve per search  |
| `rerank_top_k`                  | 5       | Chunks after reranking                   |
| `rerank_threshold`              | 0.0     | Min rerank score                         |
| `enable_rewrite`                | false   | Enable query rewriting                   |
| `enable_query_expansion`        | false   | Enable query expansion for low recall    |
| `fallback_strategy`             | `fixed` | `fixed` or `model`                       |
| `fallback_response`             | —       | Fixed fallback text                      |
| `fallback_prompt`               | —       | Model fallback prompt template           |
| `summary.prompt`                | —       | System prompt for RAG answer generation  |
| `summary.context_template`      | —       | Template for assembling query + contexts |
| `summary.temperature`           | —       | LLM temperature                          |
| `summary.max_completion_tokens` | —       | Max tokens in LLM response               |
| `intent_system_prompts`         | —       | Map of intent → system prompt override   |

### Retrieval Config (tenant-level)

| Parameter           | Default | Description                  |
| ------------------- | ------- | ---------------------------- |
| `embedding_top_k`   | 5       | Overrides global default     |
| `vector_threshold`  | 0.2     | Overrides global default     |
| `keyword_threshold` | 0.3     | Overrides global default     |
| `rerank_top_k`      | 5       | Overrides global default     |
| `rerank_threshold`  | 0.0     | Overrides global default     |
| `rerank_model_id`   | —       | Specific rerank model to use |

### Custom Agent Overrides

When a custom agent is active, it can override:

- `SystemPrompt` / `SystemPromptOverride`
- `Temperature`, `TopP`
- `MultiTurnEnabled` → controls `MaxRounds`
- `HistoryTurns` → overrides `MaxRounds`
- `EnableRewrite`, `RewritePromptSystem`, `RewritePromptUser`
- `FallbackStrategy`, `FallbackResponse`, `FallbackPrompt`
- `FAQPriorityEnabled`, `FAQScoreBoost`, `FAQDirectAnswerThreshold`
- `KBSelectionMode`, `KnowledgeBases`
- `VLMModelID` → model for image analysis
- `WebSearchEnabled`, `WebSearchMaxResults`
- `IntentPromptOverrides` → per-intent system prompt overrides

