# CHUNK_SEARCH 详细流程

本文档详细描述 WeKnora RAG 流程中 `CHUNK_SEARCH`（向量/关键词混合检索）阶段的完整执行流程，覆盖从 Pipeline 触发、检索目标解析、Embedding 分组、HybridSearch 调用、多 Store 扇出、RRF 融合、上下文富化到结果返回的全链路。

---

## 目录

1. [概述与定位](#1-概述与定位)
2. [关键概念与数据结构](#2-关键概念与数据结构)
3. [Pipeline 触发与上下文](#3-pipeline-触发与上下文)
4. [CHUNK_SEARCH_PARALLEL：并行入口](#4-chunk_search_parallel并行入口)
5. [CHUNK_SEARCH：核心检索流程](#5-chunk_search核心检索流程)
6. [searchByTargets：按目标检索](#6-searchbytargets按目标检索)
7. [HybridSearch：混合检索服务](#7-hybridsearch混合检索服务)
8. [多 Store 扇出与归一化](#8-多-store-扇出与归一化)
9. [RRF 融合与去重](#9-rrf-融合与去重)
10. [FAQ 后处理](#10-faq-后处理)
11. [processSearchResults：上下文富化](#11-processsearchresults上下文富化)
12. [Web 检索分支](#12-web-检索分支)
13. [查询扩展（Query Expansion）](#13-查询扩展query-expansion)
14. [错误处理与降级策略](#14-错误处理与降级策略)
15. [进度上报与可观测性](#15-进度上报与可观测性)
16. [关键源码索引](#16-关键源码索引)

---

## 1. 概述与定位

`CHUNK_SEARCH` 是 RAG 流水线中的**核心检索阶段**，负责从知识库中召回与用户问题相关的文档片段（chunk）。它位于 `QUERY_UNDERSTAND`（查询理解/改写）之后、`CHUNK_RERANK`（重排）之前，是连接用户意图与知识库内容的桥梁。

在 WeKnora 的插件化流水线中，`CHUNK_SEARCH` 通过两种事件类型暴露：

| 事件类型 | 触发场景 | 实现插件 |
|---------|---------|---------|
| `CHUNK_SEARCH` | 单独的 chunk 检索（如 `SearchKnowledge` API、`rag` 静态流水线） | `PluginSearch` |
| `CHUNK_SEARCH_PARALLEL` | RAG 主流程，并行执行 chunk 检索 + 实体（图谱）检索 | `PluginSearchParallel` |

`CHUNK_SEARCH_PARALLEL` 是 RAG 主流程的实际入口，它内部包装了 `CHUNK_SEARCH` 与 `ENTITY_SEARCH` 两个子任务并行执行。

---

## 2. 关键概念与数据结构

### 2.1 ChatManage

整个流水线的运行时上下文，由 `PipelineRequest`（不可变配置）+ `PipelineState`（可变中间状态）+ `PipelineContext`（运行时句柄）组成。与 `CHUNK_SEARCH` 相关的字段：

- `Query` / `RewriteQuery`：原始查询 / 改写后的查询（检索使用 `RewriteQuery`）
- `SearchTargets`：预计算的统一检索目标列表（`[]*SearchTarget`）
- `KnowledgeBaseIDs` / `KnowledgeIDs`：原始 KB / Knowledge ID（兼容旧路径）
- `EmbeddingTopK`：向量召回 TopK
- `VectorThreshold` / `KeywordThreshold`：向量/关键词召回阈值
- `WebSearchEnabled` / `WebSearchProviderID`：Web 检索开关与 Provider
- `EnableQueryExpansion`：低召回时是否触发查询扩展
- `Entity` / `EntityKBIDs` / `EntityKnowledge`：实体检索所需数据
- `SearchResult`：检索阶段输出（`[]*SearchResult`）

定义位置：`internal/types/chat_manage.go:143`

### 2.2 SearchTarget

统一的检索目标抽象，支持两种类型：

```go
type SearchTarget struct {
    Type                    SearchTargetType // knowledge_base | knowledge
    KnowledgeBaseID         string
    TenantID                uint64           // 跨租户共享 KB 必需
    KnowledgeIDs            []string         // Type=knowledge 时生效
    TagIDs                  []string         // KB 内标签过滤
    ScopeTagIDs             []string         // 用户选择的逻辑标签范围
    DisableRecallThresholds bool             // 显式范围下关闭召回阈值
}
```

定义位置：`internal/types/search.go:26`

`RecallThresholds()` 方法在 `DisableRecallThresholds=true` 时返回 `(0, 0)`，确保用户显式选定的范围不会被阈值提前过滤。

### 2.3 SearchParams

传递给 `HybridSearch` 的检索参数：

```go
type SearchParams struct {
    QueryText             string
    QueryEmbedding        []float32  // 预计算的查询向量
    VectorThreshold       float64
    KeywordThreshold      float64
    MatchCount            int
    DisableKeywordsMatch  bool
    DisableVectorMatch    bool
    KnowledgeIDs          []string
    TagIDs                []string
    ScopeTagIDs           []string
    KnowledgeBaseIDs      []string  // 多 KB 合并检索
    SkipContextEnrichment bool      // Pipeline 跳过上下文富化
}
```

定义位置：`internal/types/search.go:230`

### 2.4 SearchResult

检索结果项，包含 chunk 内容、知识元数据、得分、匹配类型等。定义位置：`internal/types/search.go:151`。

### 2.5 EventType 与流水线组装

```go
const (
    LOAD_HISTORY           EventType = "load_history"
    QUERY_UNDERSTAND       EventType = "query_understand"
    CHUNK_SEARCH           EventType = "chunk_search"
    CHUNK_SEARCH_PARALLEL  EventType = "chunk_search_parallel"
    ENTITY_SEARCH          EventType = "entity_search"
    CHUNK_RERANK           EventType = "chunk_rerank"
    WEB_FETCH              EventType = "web_fetch"
    CHUNK_MERGE            EventType = "chunk_merge"
    FILTER_TOP_K           EventType = "filter_top_k"
    INTO_CHAT_MESSAGE      EventType = "into_chat_message"
    CHAT_COMPLETION_STREAM EventType = "chat_completion_stream"
)
```

定义位置：`internal/types/chat_manage.go:262`

RAG 主流程通过 `PipelineBuilder` 动态组装：

```go
pipeline = types.NewPipelineBuilder().
    AddIf(hasHistory, types.LOAD_HISTORY).
    Add(types.QUERY_UNDERSTAND).
    Add(types.CHUNK_SEARCH_PARALLEL).   // ← 检索入口
    Add(types.CHUNK_RERANK).
    AddIf(req.WebSearchEnabled, types.WEB_FETCH).
    Add(types.CHUNK_MERGE).
    Add(types.FILTER_TOP_K).
    AddIf(chatManage.DataAnalysisEnabled, types.DATA_ANALYSIS).
    Add(types.INTO_CHAT_MESSAGE).
    Add(types.CHAT_COMPLETION_STREAM).
    Build()
```

组装位置：`internal/application/service/session_knowledge_qa.go:186`

---

## 3. Pipeline 触发与上下文

### 3.1 入口：KnowledgeQAByEvent

`sessionService.KnowledgeQAByEvent` 是流水线的执行引擎，按 `eventList` 顺序逐个触发 `EventManager.Trigger`。

```go
for _, eventType := range eventList {
    stageCtx, stageSpan = langfuse.GetManager().StartSpan(ctx, ...)
    if IsConsolidatedRetrievalStage(eventType, chatManage) && retrievalProgress == nil {
        retrievalProgress = BeginRetrievalProgress(stageCtx, chatManage)
    }
    err := s.eventManager.Trigger(stageCtx, eventType, chatManage)
    if ShouldCloseRetrievalProgress(eventType, lastRetrievalStage, err) {
        EndRetrievalProgress(stageCtx, chatManage, retrievalProgress, ...)
    }
    // 错误处理：ErrSearchNothing → fallback；其他错误 → 中断
}
```

位置：`internal/application/service/session_knowledge_qa.go:658`

关键点：
- `CHUNK_SEARCH_PARALLEL` / `CHUNK_RERANK` / `CHUNK_MERGE` / `FILTER_TOP_K` 同属"合并检索窗口"（consolidated retrieval window），共享一个对前端可见的 `knowledge_search` tool_call 进度
- `ErrSearchNothing` 不视为错误，转而触发 fallback 响应
- 每个阶段包裹在 Langfuse span 中，便于追踪

### 3.2 EventManager 与 Plugin 链

`EventManager` 通过 `Register` 收集插件，按 `ActivationEvents()` 建立事件 → 插件列表的映射，并用 `buildHandler` 构造洋葱式 next 链。

```go
type Plugin interface {
    OnEvent(ctx, eventType, chatManage, next func() *PluginError) *PluginError
    ActivationEvents() []EventType
}
```

位置：`internal/application/service/chat_pipeline/chat_pipeline.go:9`

`CHUNK_SEARCH` 注册到 `PluginSearch`，`CHUNK_SEARCH_PARALLEL` 注册到 `PluginSearchParallel`。

---

## 4. CHUNK_SEARCH_PARALLEL：并行入口

`PluginSearchParallel` 是 RAG 主流程的检索入口，它**并行**执行两个子任务：

1. **chunk_search**：调用内部 `PluginSearch.OnEvent(CHUNK_SEARCH, ...)` 执行向量/关键词混合检索
2. **entity_search**：调用内部 `PluginSearchEntity.OnEvent(ENTITY_SEARCH, ...)` 执行知识图谱实体检索

### 4.1 流程图

```
                CHUNK_SEARCH_PARALLEL
                        │
        ┌───────────────┼───────────────┐
        │                               │
        ▼                               ▼
  chunk_search                    entity_search
  (PluginSearch)                (PluginSearchEntity)
        │                               │
        │ HybridSearch                  │ GraphRepo.SearchNode
        │ + Web Search                  │ + ChunkRepo 加载
        ▼                               ▼
  chunkCM.SearchResult           entityCM.SearchResult
        │                               │
        └───────────────┬───────────────┘
                        ▼
            合并 + removeDuplicateResults
                        │
                        ▼
              chatManage.SearchResult
                        │
                len == 0 ? ErrSearchNothing : next()
```

### 4.2 关键实现

位置：`internal/application/service/chat_pipeline/search_parallel.go:90`

```go
func (p *PluginSearchParallel) OnEvent(ctx, eventType, chatManage, next) *PluginError {
    // 1. 意图跳过：query_understand 判定无需检索
    if !chatManage.NeedsRetrieval() {
        return next()
    }

    // 2. 深拷贝避免并发读写冲突
    chunkCM := chatManage.Clone()
    entityCM := chatManage.Clone()

    // 3. 构造并行任务
    tasks := []ParallelTask{
        {Name: "chunk_search",  Run: ...},  // 调用 p.searchPlugin.OnEvent(CHUNK_SEARCH, chunkCM, noop)
        {Name: "entity_search", Run: ...},  // 调用 p.searchEntityPlugin.OnEvent(ENTITY_SEARCH, entityCM, noop)
    }
    errs := RunParallel(tasks...)

    // 4. 合并结果并去重
    chatManage.SearchResult = append(chunkCM.SearchResult, entityCM.SearchResult...)
    chatManage.SearchResult = removeDuplicateResults(chatManage.SearchResult)

    // 5. 空结果处理
    if len(chatManage.SearchResult) == 0 {
        if err, ok := errs["chunk_search"]; ok { return err }
        return ErrSearchNothing
    }
    return next()
}
```

### 4.3 设计要点

- **深拷贝隔离**：`chatManage.Clone()` 为两个子任务各自生成独立副本，避免 `SearchResult` 切片的并发读写竞争
- **ErrSearchNothing 容忍**：子任务返回 `ErrSearchNothing` 被转换为 `nil`，只有真正错误才向上传播
- **entity_search 条件跳过**：`len(chatManage.Entity) == 0` 时直接跳过图谱检索
- **去重合并**：`removeDuplicateResults` 按 chunk ID + 内容签名双重去重

---

## 5. CHUNK_SEARCH：核心检索流程

`PluginSearch.OnEvent` 是 `CHUNK_SEARCH` 事件的真正处理者，也是 `CHUNK_SEARCH_PARALLEL` 内部 chunk 子任务的实现。

位置：`internal/application/service/chat_pipeline/search.go:62`

### 5.1 主流程

```
OnEvent(CHUNK_SEARCH)
        │
        ▼
[1] 检查检索目标（KB targets 或 WebSearchEnabled）
        │ 无目标 → pipelineError("kb_not_found") → return nil
        ▼
[2] 并行执行两个 goroutine：
    ┌────────────────────┬────────────────────┐
    │ goroutine 1: KB 检索 │ goroutine 2: Web 检索│
    │ searchByTargets     │ searchWebIfEnabled  │
    └────────────────────┴────────────────────┘
        │ wg.Wait()
        ▼
[3] 错误处理：
    - kbSearchErr != nil && 无结果 → ErrSearch
    - kbSearchErr != nil && 有结果 → 警告，继续
        ▼
[4] chatManage.SearchResult = allResults
        ▼
[5] logSearchScoreSample("result_score_before_normalize")
        ▼
[6] 低召回检查：
    if EnableQueryExpansion && len(SearchResult) < max(1, EmbeddingTopK):
        expResults = runQueryExpansion(...)
        SearchResult = append(SearchResult, expResults...)
        ▼
[7] logSearchScoreSample("final_score")
        ▼
[8] 结果判断：
    len(SearchResult) != 0 → next()
    len(SearchResult) == 0 → ErrSearchNothing
```

### 5.2 关键代码片段

```go
func (p *PluginSearch) OnEvent(ctx, eventType, chatManage, next) *PluginError {
    hasKBTargets := types.HasKnowledgeRetrievalScope(
        chatManage.SearchTargets, chatManage.KnowledgeBaseIDs, chatManage.KnowledgeIDs)
    if !hasKBTargets && !chatManage.WebSearchEnabled {
        return nil  // 无检索目标
    }

    var wg sync.WaitGroup
    var mu sync.Mutex
    allResults := make([]*types.SearchResult, 0)

    wg.Add(2)
    go func() {  // KB 检索
        defer wg.Done()
        kbResults, err := p.searchByTargets(ctx, chatManage)
        kbSearchErr = err
        if len(kbResults) > 0 { mu.Lock(); allResults = append(allResults, kbResults...); mu.Unlock() }
    }()
    go func() {  // Web 检索
        defer wg.Done()
        webResults := p.searchWebIfEnabled(ctx, chatManage)
        if len(webResults) > 0 { mu.Lock(); allResults = append(allResults, webResults...); mu.Unlock() }
    }()
    wg.Wait()

    chatManage.SearchResult = allResults

    // 低召回 → 查询扩展
    if chatManage.EnableQueryExpansion && len(chatManage.SearchResult) < max(1, chatManage.EmbeddingTopK) {
        expResults := p.runQueryExpansion(ctx, chatManage)
        chatManage.SearchResult = append(chatManage.SearchResult, expResults...)
    }

    if len(chatManage.SearchResult) != 0 { return next() }
    return ErrSearchNothing
}
```

---

## 6. searchByTargets：按目标检索

`searchByTargets` 是 KB 检索的核心调度逻辑，位置：`internal/application/service/chat_pipeline/search.go:348`。

### 6.1 流程

```
searchByTargets(chatManage)
        │
        ▼
[1] 提取所有 SearchTarget 的 KB ID，批量加载 KB 元数据
    GetKnowledgeBasesByIDsOnly(kbIDs)
        │ 失败 → 降级为按 KB 单独计算 embedding
        ▼
[2] 解析 Embedding 模型身份键
    ResolveEmbeddingModelKeys(kbList)
    key = model.Name + "|" + model.Parameters.BaseURL
        │
        ▼
[3] 按 modelKey 分组 SearchTargets
    groups[modelKey] = []*SearchTarget
        │
        ▼
[4] 每个 modelKey 组并行执行：
    ┌─────────────────────────────────────────┐
    │ goroutine per modelKey:                 │
    │   a. 计算查询 embedding（一次）           │
    │      GetQueryEmbedding(targets[0].KBID) │
    │      失败 → 降级为 keyword-only          │
    │   b. 拆分 full-KB targets 与             │
    │      specific-knowledge targets          │
    │   c. full-KB 合并为一次 HybridSearch     │
    │   d. specific-knowledge 各自一次调用     │
    └─────────────────────────────────────────┘
        │ wg.Wait()
        ▼
[5] 返回 results, firstErr
```

### 6.2 Embedding 分组优化

**目的**：避免对同一物理 embedding 模型重复计算查询向量。

**实现**：
- `ResolveEmbeddingModelKeys` 将 KB 的 `EmbeddingModelID` 解析为 `model.Name + "|" + BaseURL`，跨租户共享同一物理模型的 KB 会得到相同 key
- 同一 key 下的所有 target 共享一次 `GetQueryEmbedding` 调用
- 同一 key 下的所有 full-KB target 进一步合并为**一次** `HybridSearch` 调用（通过 `params.KnowledgeBaseIDs` 传入多个 KB ID）

### 6.3 Embedding 失败降级

当某 modelKey 组的 embedding 计算失败时：

```go
for _, target := range targets {
    kb := kbMap[target.KnowledgeBaseID]
    if !targetReportsEmbedFailure(kb) {
        searchableTargets = append(searchableTargets, target)  // 保留有 keyword 索引的
        continue
    }
    recordError(...)  // FAQ 或 vector-only KB 报错
}
disableVector = true  // 该组禁用向量检索
```

`targetReportsEmbedFailure` 规则（`search.go:330`）：
- `kb == nil` → 不报错（保留以尝试 keyword 降级）
- FAQ 类型 → 报错（FAQ 无 keyword 索引）
- 启用 keyword → 不报错（可降级）
- 仅启用 vector → 报错

### 6.4 searchSingleTarget

针对 specific-knowledge target 的检索（`search.go:522`）：

```go
func (p *PluginSearch) searchSingleTarget(ctx, chatManage, t, queryText, queryEmbedding, disableVector, mu, results) error {
    vectorThreshold, keywordThreshold := t.RecallThresholds(
        chatManage.VectorThreshold, chatManage.KeywordThreshold)
    params := types.SearchParams{
        QueryText:             queryText,
        QueryEmbedding:        queryEmbedding,
        VectorThreshold:       vectorThreshold,
        KeywordThreshold:      keywordThreshold,
        MatchCount:            chatManage.EmbeddingTopK,
        TagIDs:                t.TagIDs,
        ScopeTagIDs:           t.ScopeTagIDs,
        SkipContextEnrichment: true,   // Pipeline 在 merge 阶段统一处理
        DisableVectorMatch:    disableVector,
    }
    if t.Type == types.SearchTargetTypeKnowledge {
        params.KnowledgeIDs = t.KnowledgeIDs
    }
    res, err := p.knowledgeBaseService.HybridSearch(ctx, t.KnowledgeBaseID, params)
    ...
}
```

注意：Pipeline 调用 `HybridSearch` 时统一设置 `SkipContextEnrichment=true`，上下文富化（父 chunk、相邻 chunk、关联 chunk）推迟到 `CHUNK_MERGE` 阶段处理。

---

## 7. HybridSearch：混合检索服务

`knowledgeBaseService.HybridSearch` 是底层检索服务，位置：`internal/application/service/knowledgebase_search.go:87`。

### 7.1 完整流程

```
HybridSearch(id, params)
        │
        ▼
[1] 确定 searchKBIDs = params.KnowledgeBaseIDs ?? [id]
        │
        ▼
[2] 批量加载 KB：GetKnowledgeBaseByIDs(searchKBIDs)
    + authorizeKBAccess（跨租户权限校验）
    + validateSameEmbeddingModel（embedding 模型一致性校验）
        │
        ▼
[3] pickPrimary(kbs, id) → kb（用于 embedding 模型与 FAQ 类型决策）
        │
        ▼
[4] 计算 over-retrieval matchCount：
    matchCount = max(params.MatchCount*5, 50) * len(searchKBIDs)
    上限 500
        │
        ▼
[5] 预计算查询 embedding（若未传入且 KB 启用向量）
    GetQueryEmbedding(kb.ID, params.QueryText)
        │
        ▼
[6] resolveStoreGroups：按 (VectorStoreID, TenantID) 分组
    - 每组解析 CompositeRetrieveEngine
    - buildRetrievalParams 构造向量/关键词检索参数
        │
        ▼
[7] retrieveFromStores：多 Store 并行扇出检索
    + 跨引擎分数归一化（EngineAwareNormalizer）
        │
        ▼
[8] classifyRetrievalResults：分离 vector / keyword 结果
        │
        ▼
[9] fuseOrDeduplicate：
    - 纯向量 → deduplicateByScore
    - 纯关键词 → deduplicateByScore
    - 混合 → fuseWithRRF
        │
        ▼
[10] applyFAQPostProcessing（仅 FAQ KB）
        │
        ▼
[11] 截断到 params.MatchCount
        │
        ▼
[12] processSearchResults：上下文富化 + 组装 SearchResult
```

### 7.2 over-retrieval 策略

```go
matchCount := max(params.MatchCount*5, 50) * len(searchKBIDs)
if matchCount > 500 { matchCount = 500 }
```

- 单 KB：`max(MatchCount*5, 50)`，下限 50
- 多 KB：按 KB 数量线性放大
- 全局上限 500

目的：为后续的 RRF 融合、rerank、去重留出足够的候选池。

### 7.3 buildRetrievalParams：构造检索参数

位置：`internal/application/service/knowledgebase_search.go:317`

按 KB 类型路由到不同索引：

| KB 类型 | 向量索引 | 关键词索引 |
|--------|---------|----------|
| 文档型（默认） | 默认向量索引 | 关键词索引 |
| FAQ | FAQ 向量索引 | 无 |
| Wiki/Graph-only | 跳过 | 跳过 |

```go
// 分区 KB IDs
var faqVectorKBIDs, docVectorKBIDs, docKeywordKBIDs []string
for _, kb := range groupKBs {
    if kb.IsVectorEnabled() && kb.EmbeddingModelID != "" {
        if kb.Type == types.KnowledgeBaseTypeFAQ {
            faqVectorKBIDs = append(faqVectorKBIDs, kb.ID)
        } else {
            docVectorKBIDs = append(docVectorKBIDs, kb.ID)
        }
    }
    if kb.IsKeywordEnabled() && kb.Type != types.KnowledgeBaseTypeFAQ {
        docKeywordKBIDs = append(docKeywordKBIDs, kb.ID)
    }
}

// 构造 RetrieveParams
if SupportRetriever(VectorRetrieverType) && !DisableVectorMatch {
    // 文档向量检索
    if len(docVectorKBIDs) > 0 { appendVectorParams(docVectorKBIDs, "") }
    // FAQ 向量检索
    if len(faqVectorKBIDs) > 0 { appendVectorParams(faqVectorKBIDs, KnowledgeTypeFAQ) }
}
if SupportRetriever(KeywordsRetrieverType) && !DisableKeywordsMatch && len(docKeywordKBIDs) > 0 {
    // 关键词检索
    retrieveParams = append(retrieveParams, types.RetrieveParams{...})
}
```

---

## 8. 多 Store 扇出与归一化

### 8.1 resolveStoreGroups：Store 分组

位置：`internal/application/service/knowledgebase_search_storegroup.go:86`

按 `(VectorStoreID, OwnerTenantID)` 分组 KB：

```go
type partitionKey struct {
    storeID  string
    tenantID uint64
}
buckets := make(map[partitionKey][]*types.KnowledgeBase)
for _, kb := range kbs {
    sid := ""
    if kb.HasVectorStore() { sid = *kb.VectorStoreID }
    key := partitionKey{storeID: sid, tenantID: kb.TenantID}
    buckets[key] = append(buckets[key], kb)
}
```

**为什么按 (StoreID, TenantID) 而非仅 StoreID**：组织共享 KB 的 Store 归属源租户，必须用 `kb.TenantID` 进行 ownership 查找。

每组解析一个 `CompositeRetrieveEngine`，并设置 12 秒的 `storeResolveBudget` 上限。

### 8.2 retrieveFromStores：并行扇出

位置：`internal/application/service/knowledgebase_search_fanout.go:48`

```go
func (s *knowledgeBaseService) retrieveFromStores(ctx, groups, normalizer) ([]*types.RetrieveResult, error) {
    if len(groups) == 1 {
        return groups[0].Engine.Retrieve(ctx, paramsWithTopK(groups[0]))  // 快速路径
    }

    timeout := multiStoreRetrieveTimeout()  // 默认 30s，可由 MULTI_STORE_RETRIEVE_TIMEOUT_SEC 覆盖
    g, gctx := errgroup.WithContext(ctx)
    g.SetLimit(defaultMultiStoreFanoutLimit)  // 4

    for i := range groups {
        grp := groups[i]
        g.Go(func() error {
            gcCtx, cancel := context.WithTimeout(gctx, timeout)
            defer cancel()
            res, err := grp.Engine.Retrieve(gcCtx, paramsWithTopK(grp))
            ...
        })
    }
    if err := g.Wait(); err != nil {
        if isParentCancelled(ctx) { return nil, ctx.Err() }
        return nil, apperrors.NewVectorStoreUnavailableError(...)  // 统一类型化错误
    }

    // 跨引擎分数归一化（仅当结果来自 2+ 不同引擎类型）
    if hasMixedEngineTypes(all) {
        for _, rr := range all {
            for _, hit := range rr.Results {
                hit.Score = normalizer.Normalize(ctx, hit.Score, rr.RetrieverType, rr.RetrieverEngineType)
            }
        }
    }
    return all, nil
}
```

**关键设计**：
- **快速路径**：单 group 直接调用，零扇出开销
- **失败策略**：all-or-nothing，首个 group 错误即失败并取消兄弟任务
- **并发上限**：`defaultMultiStoreFanoutLimit = 4`
- **超时**：每 group 独立 30s 超时
- **分数归一化**：仅当结果跨多种引擎类型时才应用 `EngineAwareNormalizer`，同引擎结果保持原生分数尺度

### 8.3 CompositeRetrieveEngine.Retrieve

位置：`internal/application/service/retriever/composite.go:32`

复合引擎按 `RetrieveParams.RetrieverType` 路由到具体引擎实现（Elasticsearch / Milvus / Qdrant / Postgres / SQLite / Weaviate / Infinity / TencentVectorDB / Doris / ElasticFaiss），多个 RetrieveParams 并发执行。

---

## 9. RRF 融合与去重

位置：`internal/application/service/knowledgebase_search_fusion.go`

### 9.1 结果分类

```go
func classifyRetrievalResults(ctx, retrieveResults) (vectorResults, keywordResults []*types.IndexWithScore) {
    for _, rr := range retrieveResults {
        if rr.RetrieverType == types.VectorRetrieverType {
            vectorResults = append(vectorResults, rr.Results...)
        } else {
            keywordResults = append(keywordResults, rr.Results...)
        }
    }
}
```

### 9.2 融合策略选择

```go
func fuseOrDeduplicate(ctx, vectorResults, keywordResults, retrievalCfg) []*types.IndexWithScore {
    if len(keywordResults) == 0 {
        return deduplicateByScore(vectorResults)  // 纯向量：保留原始 embedding 分数
    }
    if len(vectorResults) == 0 {
        return deduplicateByScore(keywordResults)  // 纯关键词
    }
    return fuseWithRRF(ctx, vectorResults, keywordResults, retrievalCfg)  // 混合：RRF
}
```

### 9.3 RRF 公式

```
RRF_score(chunk) = vectorWeight / (k + vectorRank) + keywordWeight / (k + keywordRank)
```

- `k`：平滑常数，来自 `retrievalCfg.GetEffectiveRRFK()`
- `vectorWeight` / `keywordWeight`：来自 `retrievalCfg.GetEffectiveRRFWeights()`
- 排名（rank）从 1 开始，按 retriever 返回的分数降序

### 9.4 deduplicateByScore

按 chunk ID 去重，保留最高分，按分数降序排序。用于单 retriever 场景，**保留原始 embedding 分数**（对 FAQ 重要）。

---

## 10. FAQ 后处理

位置：`internal/application/service/knowledgebase_search_faq.go:24`

仅对 `KnowledgeBaseTypeFAQ` 类型 KB 生效，两种模式：

### 10.1 迭代检索

触发条件：`len(chunks) < params.MatchCount && len(vectorResults) == matchCount`（首次召回不足且向量结果已饱和）

```go
maxIterations := 5
currentTopK := matchCount * 3  // 起始 TopK
for i := 0; i < maxIterations; i++ {
    for _, grp := range groups { grp.TopK = currentTopK }
    retrieveResults, err := s.retrieveFromStores(ctx, groups, ...)
    // 负向问题过滤 + 去重 + 合并
    if len(uniqueChunks) >= matchCount { break }
    if totalRetrieved < currentTopK { break }  // 无更多结果
    currentTopK *= 2  // 指数退避放大
}
```

### 10.2 负向问题过滤

当不触发迭代检索时，对每个 FAQ chunk 检查其 `FAQMetadata.NegativeQuestions`：

```go
func matchesNegativeQuestions(queryTextLower string, negativeQuestions []string) bool {
    for _, negativeQ := range negativeQuestions {
        if queryTextLower == strings.ToLower(strings.TrimSpace(negativeQ)) {
            return true  // 命中负向问题 → 过滤
        }
    }
    return false
}
```

---

## 11. processSearchResults：上下文富化

位置：`internal/application/service/knowledgebase_search_results.go:14`

将 `[]*IndexWithScore`（检索命中）转换为 `[]*SearchResult`（带元数据的最终结果）。

### 11.1 流程

```
processSearchResults(chunks, skipEnrichment)
        │
        ▼
[1] buildChunkIndex：收集 knowledgeIDs / chunkIDs / scores / matchTypes
        │
        ▼
[2] fetchKnowledgeDataWithShared：批量加载知识元数据（含共享 KB）
        │
        ▼
[3] listChunksByIDWithShared：批量加载 chunk 数据
        │
        ▼
[4] 若 !skipEnrichment：
    collectEnrichmentChunkIDs 收集富化 ID：
    - 父 chunk（ParentChunkID）→ MatchTypeParentChunk
    - 关联 chunk（RelationChunks）→ MatchTypeRelationChunk
    - 相邻 chunk（NextChunkID / PreChunkID，仅文本类型）→ MatchTypeNearByChunk
        │
        ▼
[5] 二轮富化：若主结果含图片 chunk，再解析一层 parent_text
        │
        ▼
[6] assembleSearchResults：组装最终 SearchResult
    - 第一轮：按 inputChunks 原始顺序添加主结果
    - 第二轮：添加富化 chunk（父/相邻/关联）
        │
        ▼
[7] EnrichSearchResultsImageInfo：补充图片信息
```

### 11.2 isSearchableChunk 过滤

```go
func (s *knowledgeBaseService) isSearchableChunk(chunk *types.Chunk) bool {
    if chunk == nil || !chunk.IsEnabled { return false }
    if chunk.IndexStatus == "processing" || chunk.IndexStatus == "failed" { return false }
    return slices.Contains([]types.ChunkType{
        types.ChunkTypeText, types.ChunkTypeSummary,
        types.ChunkTypeTableColumn, types.ChunkTypeTableSummary,
        types.ChunkTypeFAQ,
        types.ChunkTypeImageOCR, types.ChunkTypeImageCaption,
    }, chunk.ChunkType)
}
```

### 11.3 Pipeline 中的 SkipContextEnrichment

Pipeline 调用 `HybridSearch` 时统一设置 `SkipContextEnrichment=true`，原因：
- 上下文富化（父/相邻/关联 chunk）由 `CHUNK_MERGE` 阶段统一处理
- 避免在检索阶段重复加载富化 chunk
- 减少 DB 查询次数

---

## 12. Web 检索分支

位置：`internal/application/service/chat_pipeline/search.go:583`

`searchWebIfEnabled` 在 `WebSearchEnabled=true` 且配置了 `WebSearchProviderID` 时执行：

```go
func (p *PluginSearch) searchWebIfEnabled(ctx, chatManage) []*types.SearchResult {
    if !chatManage.WebSearchEnabled || p.webSearchService == nil { return nil }
    providerID := chatManage.WebSearchProviderID
    if providerID == "" { return nil }

    webConfig := types.EffectiveWebSearchConfig(tenant.WebSearchConfig)
    if chatManage.WebSearchMaxResults > 0 {
        webConfig.MaxResults = chatManage.WebSearchMaxResults  // Agent 级覆盖
    }

    webCtx, webSpan := langfuse.GetManager().StartSpan(ctx, ...)
    webResults, err := p.webSearchService.Search(webCtx, providerID, webConfig, chatManage.RewriteQuery)
    webSpan.Finish(...)

    res := searchutil.ConvertWebSearchResults(webResults)
    return res
}
```

Web 结果通过 `searchutil.ConvertWebSearchResults` 转换为 `[]*SearchResult`，`ChunkType="web_search"`，与 KB 结果合并后统一进入后续阶段。

---

## 13. 查询扩展（Query Expansion）

位置：`internal/application/service/chat_pipeline/query_expansion.go:16`

### 13.1 触发条件

```go
if chatManage.EnableQueryExpansion && len(chatManage.SearchResult) < max(1, chatManage.EmbeddingTopK) {
    expResults := p.runQueryExpansion(ctx, chatManage)
    chatManage.SearchResult = append(chatManage.SearchResult, expResults...)
}
```

### 13.2 本地查询变体生成（无 LLM）

`expandQueries` 使用纯本地规则生成查询变体：

1. **停用词去除**：移除中英文停用词（的/是/在/the/is/...）
2. **引号短语提取**：提取 `"..."` / `「...」` / `『...』` 中的内容
3. **分隔符切分**：按 `,，;；、。！？!?` 切分取长片段
4. **疑问词去除**：移除 `什么是/如何/怎么/为什么/...` 等疑问词前缀

最多生成 5 个变体，使用 jieba 搜索模式分词。

### 13.3 并发扩展检索

```go
expTopK := max(chatManage.EmbeddingTopK*2, chatManage.RerankTopK*2)
expKwTh := chatManage.KeywordThreshold * 0.8  // 放宽关键词阈值

jobs := len(expansions) * len(chatManage.SearchTargets)
capSem := min(jobs, 16)  // 并发上限 16
sem := make(chan struct{}, capSem)

for _, q := range expansions {
    for _, target := range chatManage.SearchTargets {
        go func(q string, t *SearchTarget) {
            sem <- struct{}{}
            defer func() { <-sem }()
            paramsExp := types.SearchParams{
                QueryText:        q,
                MatchCount:       expTopK,
                KeywordThreshold: expKwTh,
                ...
            }
            res, err := p.knowledgeBaseService.HybridSearch(ctx, t.KnowledgeBaseID, paramsExp)
            ...
        }(q, target)
    }
}
```

**关键参数**：
- `expTopK`：扩大 2 倍 TopK
- `expKwTh`：关键词阈值放宽 20%
- 并发上限 16

---

## 14. 错误处理与降级策略

### 14.1 错误类型

| 错误 | 含义 | 处理 |
|------|------|------|
| `ErrSearchNothing` | 检索无结果 | Pipeline 转入 fallback 响应 |
| `ErrSearch` | KB 检索失败 | 向上传播，中断流水线 |
| `ErrVectorStoreUnavailableError` (2201) | 向量库不可用 | 统一类型化错误，不泄露 Store UUID |
| `ErrVectorStoreBindingInvalidError` (2200) | Store 绑定无效 | 同上 |

### 14.2 降级路径

```
Embedding 失败
    │
    ├─ KB 有 keyword 索引 → disableVector=true，仅 keyword 检索
    ├─ KB 是 FAQ → 报错（FAQ 无 keyword 索引）
    └─ KB 仅 vector → 报错

多 Store 扇出失败
    │
    ├─ 父 context 取消 → 返回 ctx.Err()
    └─ 其他失败 → NewVectorStoreUnavailableError（all-or-nothing）

KB 元数据批量加载失败
    │
    └─ 所有 target 落入空 key 组，HybridSearch 内部按 KB 单独计算 embedding
```

### 14.3 跨租户权限

`authorizeKBAccess`（`knowledgebase_search_storegroup.go:198`）：
- 同租户 KB：直接通过
- 跨租户共享 KB：调用 `kbShareService.HasTenantKBPermission` 校验 Viewer 角色
- 权限不足：返回 `NotFoundError`（避免泄露 KB 存在性）

### 14.4 Embedding 模型一致性

`validateSameEmbeddingModel`（`knowledgebase_search_storegroup.go:247`）：
- 多 KB 检索要求所有 KB 使用同一 embedding 模型
- Wiki/Graph-only KB（无 embedding 模型）被容忍
- 不一致 → `BadRequestError`

---

## 15. 进度上报与可观测性

### 15.1 合并检索窗口

`CHUNK_SEARCH_PARALLEL` / `CHUNK_RERANK` / `CHUNK_MERGE` / `FILTER_TOP_K` 共享一个对前端可见的 `knowledge_search` tool_call 进度（`progress.go:40`）：

```go
func IsConsolidatedRetrievalStage(stage, chatManage) bool {
    switch stage {
    case types.CHUNK_SEARCH_PARALLEL, types.CHUNK_RERANK, types.CHUNK_MERGE, types.FILTER_TOP_K:
        return chatManage.NeedsRetrieval()
    case types.WEB_FETCH:
        return chatManage.WebSearchEnabled
    case types.DATA_ANALYSIS:
        return chatManage.DataAnalysisEnabled && chatManage.NeedsRetrieval()
    }
    return false
}
```

进度通过 `EventBus` 发送 `EventAgentToolCall` / `EventAgentToolResult` 事件，前端展示为"正在检索知识库"。

### 15.2 关闭时机

```go
func ShouldCloseRetrievalProgress(stage, lastRetrievalStage, stageErr) bool {
    return stage == lastRetrievalStage || stageErr != nil
}
```

- 正常完成：到达最后一个检索阶段
- 异常完成：任一检索阶段返回错误（含 `ErrSearchNothing`）

### 15.3 Langfuse 追踪

- 每个流水线阶段包裹在 `pipeline.<event_type>` span 中
- `HybridSearch` 内部对 `retrieve` 步骤单独建 span
- Web 检索对 `web_search` 建独立 span
- `CHAT_COMPLETION_STREAM` 跳过 stage span（流式 goroutine 生命周期长于 span）

### 15.4 结构化日志

通过 `pipelineInfo` / `pipelineWarn` / `pipelineError` 输出结构化日志，关键 action：

| Stage | Action | 含义 |
|-------|--------|------|
| Search | input | 检索输入参数 |
| Search | plan | 检索计划（targets/topK/thresholds）|
| Search | embedding_groups | embedding 模型分组结果 |
| Search | group_plan | 单组检索计划 |
| Search | combined_kb_result | 合并 KB 检索结果 |
| Search | kb_result | 单 KB 检索结果 |
| Search | result_score_before_normalize | 归一化前分数采样 |
| Search | final_score | 最终分数采样 |
| Search | recall_low | 低召回触发查询扩展 |
| Search | expansion_start | 查询扩展开始 |
| Search | expansion_done | 查询扩展完成 |
| SearchParallel | start | 并行检索开始 |
| SearchParallel | chunk_search_done | chunk 检索完成 |
| SearchParallel | entity_search_done | 实体检索完成 |
| SearchParallel | complete | 并行检索完成 |

---

## 16. 关键源码索引

| 文件 | 关键函数/类型 | 作用 |
|------|-------------|------|
| `internal/types/chat_manage.go:262` | `EventType` 常量 | 流水线事件定义 |
| `internal/types/chat_manage.go:143` | `ChatManage` | 流水线运行时上下文 |
| `internal/types/search.go:26` | `SearchTarget` | 统一检索目标 |
| `internal/types/search.go:230` | `SearchParams` | 检索参数 |
| `internal/types/search.go:151` | `SearchResult` | 检索结果 |
| `internal/application/service/session_knowledge_qa.go:186` | `PipelineBuilder` 组装 | RAG 流水线组装 |
| `internal/application/service/session_knowledge_qa.go:658` | `KnowledgeQAByEvent` | 流水线执行引擎 |
| `internal/application/service/chat_pipeline/chat_pipeline.go` | `Plugin` / `EventManager` | 插件化框架 |
| `internal/application/service/chat_pipeline/search_parallel.go:90` | `PluginSearchParallel.OnEvent` | 并行检索入口 |
| `internal/application/service/chat_pipeline/search.go:62` | `PluginSearch.OnEvent` | CHUNK_SEARCH 主流程 |
| `internal/application/service/chat_pipeline/search.go:348` | `searchByTargets` | 按目标检索调度 |
| `internal/application/service/chat_pipeline/search.go:522` | `searchSingleTarget` | 单目标检索 |
| `internal/application/service/chat_pipeline/search.go:583` | `searchWebIfEnabled` | Web 检索分支 |
| `internal/application/service/chat_pipeline/query_expansion.go:16` | `runQueryExpansion` | 查询扩展 |
| `internal/application/service/chat_pipeline/search_entity.go:42` | `PluginSearchEntity.OnEvent` | 实体（图谱）检索 |
| `internal/application/service/chat_pipeline/progress.go` | `BeginRetrievalProgress` 等 | 进度上报 |
| `internal/application/service/knowledgebase_search.go:87` | `HybridSearch` | 混合检索服务 |
| `internal/application/service/knowledgebase_search.go:317` | `buildRetrievalParams` | 检索参数构造 |
| `internal/application/service/knowledgebase_search_storegroup.go:86` | `resolveStoreGroups` | Store 分组 |
| `internal/application/service/knowledgebase_search_fanout.go:48` | `retrieveFromStores` | 多 Store 扇出 |
| `internal/application/service/knowledgebase_search_fusion.go:33` | `fuseOrDeduplicate` | RRF 融合 |
| `internal/application/service/knowledgebase_search_fusion.go:84` | `fuseWithRRF` | RRF 算法 |
| `internal/application/service/knowledgebase_search_faq.go:24` | `applyFAQPostProcessing` | FAQ 后处理 |
| `internal/application/service/knowledgebase_search_results.go:14` | `processSearchResults` | 上下文富化 |
| `internal/application/service/retriever/composite.go:32` | `CompositeRetrieveEngine.Retrieve` | 复合引擎检索 |

---

## 附录：完整调用链路

```
用户问题
    │
    ▼
sessionService.KnowledgeQAByEvent
    │
    ▼
EventManager.Trigger(CHUNK_SEARCH_PARALLEL)
    │
    ▼
PluginSearchParallel.OnEvent
    │
    ├─ RunParallel
    │   │
    │   ├─ chunk_search: PluginSearch.OnEvent(CHUNK_SEARCH)
    │   │   │
    │   │   ├─ searchByTargets
    │   │   │   │
    │   │   │   ├─ ResolveEmbeddingModelKeys（分组）
    │   │   │   │
    │   │   │   ├─ per modelKey goroutine:
    │   │   │   │   ├─ GetQueryEmbedding（一次）
    │   │   │   │   ├─ full-KB 合并 → HybridSearch
    │   │   │   │   └─ per knowledge target → HybridSearch
    │   │   │   │
    │   │   │   │   HybridSearch:
    │   │   │   │     ├─ authorizeKBAccess
    │   │   │   │     ├─ validateSameEmbeddingModel
    │   │   │   │     ├─ resolveStoreGroups（按 StoreID+TenantID 分组）
    │   │   │   │     ├─ retrieveFromStores（并行扇出 + 分数归一化）
    │   │   │   │     │   └─ CompositeRetrieveEngine.Retrieve
    │   │   │   │     │       ├─ VectorRetriever（向量检索）
    │   │   │   │     │       └─ KeywordsRetriever（关键词检索）
    │   │   │   │     ├─ classifyRetrievalResults
    │   │   │   │     ├─ fuseOrDeduplicate（RRF 或 dedupByScore）
    │   │   │   │     ├─ applyFAQPostProcessing（FAQ 迭代/负向过滤）
    │   │   │   │     └─ processSearchResults（上下文富化，Pipeline 跳过）
    │   │   │   │
    │   │   │   └─ 返回 []*SearchResult
    │   │   │
    │   │   ├─ searchWebIfEnabled（并行）
    │   │   │
    │   │   ├─ runQueryExpansion（低召回时）
    │   │   │
    │   │   └─ chatManage.SearchResult = allResults
    │   │
    │   └─ entity_search: PluginSearchEntity.OnEvent(ENTITY_SEARCH)
    │       ├─ GraphRepo.SearchNode（图谱检索）
    │       ├─ filterSeenChunk（过滤已见 chunk）
    │       ├─ ChunkRepo.ListChunksByID
    │       └─ chunk2SearchResult
    │
    ├─ 合并 chunkCM.SearchResult + entityCM.SearchResult
    ├─ removeDuplicateResults（ID + 内容签名去重）
    │
    └─ next() → CHUNK_RERANK
```
