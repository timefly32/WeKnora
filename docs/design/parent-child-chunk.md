# 父子 Chunk（Parent-Child Chunking）设计

本文档详细描述 WeKnora 中**父子 chunk**（Parent-Child Chunking）机制的完整设计，覆盖从配置、切分、存储、索引、检索、merge 到编辑维护的全生命周期。父子 chunk 是 WeKnora 提升 RAG 召回质量与上下文完整性的核心特性之一。

***

## 目录

1. [设计动机与核心思想](#1-设计动机与核心思想)
2. [关键概念与数据结构](#2-关键概念与数据结构)
3. [配置与启用](#3-配置与启用)
4. [切分阶段：两级分块](#4-切分阶段两级分块)
5. [存储与索引](#5-存储与索引)
6. [检索阶段：上下文富化](#6-检索阶段上下文富化)
7. [Merge 阶段：父 chunk 解析](#7-merge-阶段父-chunk-解析)
8. [编辑维护：父子一致性](#8-编辑维护父子一致性)
9. [克隆与移动](#9-克隆与移动)
10. [图片 chunk 的父子链](#10-图片-chunk-的父子链)
11. [关键源码索引](#11-关键源码索引)

***

## 1. 设计动机与核心思想

### 1.1 问题背景

传统单层分块面临一个根本矛盾：

- **小块**（如 384 字符）：向量匹配精度高，但召回的上下文片段过短，LLM 缺乏足够背景信息
- **大块**（如 4096 字符）：上下文完整，但向量匹配精度低，且 embedding 向量被"稀释"

### 1.2 核心思想

父子 chunk 采用**两级分块**策略解耦"匹配"与"上下文"：

```
┌─────────────────────────────────────────────────────────┐
│  Parent Chunk (大窗口, ~4096 字符)                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │ Child #0 │  │ Child #1 │  │ Child #2 │  (小窗口,    │
│  │ ~384 字符│  │ ~384 字符│  │ ~384 字符│   用于向量)  │
│  └──────────┘  └──────────┘  └──────────┘              │
└─────────────────────────────────────────────────────────┘
```

- **Child chunk**：小窗口，参与向量索引与检索匹配
- **Parent chunk**：大窗口，**不参与向量索引**，仅在检索后作为上下文富化使用

检索时匹配 child，返回时用 parent 内容补全上下文，兼顾匹配精度与上下文完整性。

### 1.3 与其他父子关系的区别

WeKnora 中 `ParentChunkID` 字段被多种场景复用，需区分：

| 父子关系                      | 父类型           | 子类型                           | 用途                     |
| ------------------------- | ------------- | ----------------------------- | ---------------------- |
| **Parent-Child Chunking** | `parent_text` | `text`                        | 大窗口上下文（本文档主题）          |
| **图片归属**                  | `text`        | `image_ocr` / `image_caption` | 图片 OCR/描述归属到文本 chunk   |
| **摘要归属**                  | `text`        | `summary`                     | 摘要 chunk 关联到首个文本 chunk |

本文档聚焦第一种（`parent_text` → `text`），后两种在 [第 10 节](#10-图片-chunk-的父子链) 简述。

***

## 2. 关键概念与数据结构

### 2.1 ChunkType

定义位置：`internal/types/chunk.go:15`

```go
const (
    ChunkTypeText       ChunkType = "text"         // 普通文本 chunk（child 或 flat）
    ChunkTypeParentText ChunkType = "parent_text"  // 父子策略中的父 chunk
    ChunkTypeImageOCR   ChunkType = "image_ocr"    // 图片 OCR
    ChunkTypeImageCaption ChunkType = "image_caption" // 图片描述
    // ... 其他类型
)
```

关键约束：`ChunkTypeParentText` **不参与向量索引**，仅存 DB 用于上下文检索。

### 2.2 Chunk 结构体

定义位置：`internal/types/chunk.go:113`

与父子 chunk 相关的核心字段：

```go
type Chunk struct {
    ID              string    // UUID
    Content         string    // 文本内容
    ChunkType       ChunkType // chunk 类型
    ParentChunkID   string    // 父 chunk ID（gorm: index）
    ChunkIndex      int       // 在文档中的序号
    StartAt         int       // 在原文中的起始字符位置
    EndAt           int       // 在原文中的结束字符位置
    PreChunkID      string    // 前一个同级 chunk
    NextChunkID     string    // 后一个同级 chunk
    SourceContent   string    // 不可变的原始解析输出（编辑后回填）
    ContentRevision int       // 用户编辑次数
    IndexStatus     string    // ready | processing | failed
    ContextHeader   string    // Markdown 标题面包屑（embedding 时前置）
    // ...
}
```

`ParentChunkID` 字段被三种场景共用（见 [1.3 节](#13-与其他父子关系的区别)）。

### 2.3 ParsedChunk 与 ParsedParentChunk

定义位置：`internal/types/docparser.go:72`

```go
type ParsedChunk struct {
    Content       string
    ContextHeader string
    Seq           int
    Start         int
    End           int
    ParentIndex   int // -1 = 顶层 chunk; >=0 = child，指向 ParentChunks 切片索引
}

type ParsedParentChunk struct {
    Content string
    Seq     int
    Start   int
    End     int
}
```

`ParentIndex` 是切分阶段的临时索引，在 DB 写入时被替换为真实的 `ParentChunkID`。

### 2.4 ParentChildResult 与 ChildChunk

定义位置：`internal/infrastructure/chunker/splitter.go:844`

```go
type ParentChildResult struct {
    Parents  []Chunk
    Children []ChildChunk
}

type ChildChunk struct {
    Chunk
    ParentIndex int // 指向 Parents 切片；-1 表示无父（优化场景）
}
```

### 2.5 MatchType

定义位置：`internal/types/embedding.go:15`

```go
const (
    MatchTypeEmbedding     MatchType = iota
    MatchTypeKeywords
    MatchTypeNearByChunk
    MatchTypeHistory
    MatchTypeParentChunk   // 父 chunk 匹配（富化得到）
    MatchTypeRelationChunk // 关系 chunk 匹配
    // ...
)
```

`MatchTypeParentChunk` 标识检索结果中通过父 chunk 富化得到的条目。

### 2.6 SearchResult

定义位置：`internal/types/search.go:180`

```go
type SearchResult struct {
    ID            string
    Content       string
    ChunkType     string
    ParentChunkID string  // 透传 chunk 的 ParentChunkID
    MatchType     MatchType
    Score         float64
    SubChunkID    []string // merge 后记录被合并的子 chunk
    ContentRewritten bool  // merge 阶段是否重写了 Content
    // ...
}
```

***

## 3. 配置与启用

### 3.1 ChunkingConfig

定义位置：`internal/types/knowledgebase.go:184`

```go
type ChunkingConfig struct {
    ChunkSize         int      // 基础 chunk 大小
    ChunkOverlap      int      // 基础重叠
    Separators        []string
    EnableParentChild bool     // 是否启用父子分块
    ParentChunkSize   int      // 父 chunk 大小（默认 4096）
    ChildChunkSize    int      // 子 chunk 大小（默认 384）
    Strategy          string   // 自适应分块策略
    // ...
}
```

### 3.2 配置覆盖逻辑

定义位置：`internal/application/service/knowledge_process_config.go:266`

```go
// EnableParentChild 是权威字段：调用方发送完整快照，
// 显式 false 必须能关闭父子分块（不仅仅是开启）
result.EnableParentChild = override.EnableParentChild
if override.ParentChunkSize != 0 {
    result.ParentChunkSize = override.ParentChunkSize
}
if override.ChildChunkSize != 0 {
    result.ChildChunkSize = override.ChildChunkSize
}
```

### 3.3 派生 SplitterConfig

定义位置：`internal/infrastructure/chunker/strategy.go:218`

```go
func DeriveParentChildConfigs(base SplitterConfig, parentSize, childSize int) (parent, child SplitterConfig) {
    if parentSize <= 0 { parentSize = 4096 }
    if childSize <= 0  { childSize = 384 }
    parent = SplitterConfig{
        ChunkSize:    parentSize,
        ChunkOverlap: base.ChunkOverlap,    // 继承基础重叠
        Separators:   base.Separators,
        Strategy:     base.Strategy,        // 必须传播策略
    }
    child = SplitterConfig{
        ChunkSize:    childSize,
        ChunkOverlap: childSize / 5,        // 子 chunk 重叠 = 子大小 / 5
        Separators:   base.Separators,
        Strategy:     base.Strategy,
    }
    return
}
```

**关键设计**：策略（`Strategy`）必须传播到 parent 和 child 配置。空策略会回退到 legacy 层，导致 heading splitter 不运行，父子 chunk 静默丢失标题对齐和 `ContextHeader` 面包屑。

### 3.4 调用入口

定义位置：`internal/application/service/knowledge_create.go:1234`

```go
if eff.ChunkingConfig.EnableParentChild {
    parentCfg, childCfg := buildParentChildConfigs(eff.ChunkingConfig, chunkCfg)
    pcResult := chunker.SplitParentChild(clean, parentCfg, childCfg)
    // 转换为 ParsedChunk + ParsedParentChunk
    parsed = make([]types.ParsedChunk, len(pcResult.Children))
    for i, c := range pcResult.Children {
        parsed[i] = types.ParsedChunk{
            Content:       c.Content,
            ContextHeader: c.ContextHeader,
            Seq:           c.Seq, Start: c.Start, End: c.End,
            ParentIndex:   c.ParentIndex,
        }
    }
    parentChunks := make([]types.ParsedParentChunk, len(pcResult.Parents))
    for i, p := range pcResult.Parents {
        parentChunks[i] = types.ParsedParentChunk{Content: p.Content, Seq: p.Seq, Start: p.Start, End: p.End}
    }
    opts.ParentChunks = parentChunks
}
```

***

## 4. 切分阶段：两级分块

### 4.1 SplitParentChild 入口

定义位置：`internal/infrastructure/chunker/strategy.go:135`

```go
func SplitParentChild(text string, parentCfg, childCfg SplitterConfig) ParentChildResult {
    result, _ := splitParentChild(text, parentCfg, childCfg, false)
    return result
}
```

### 4.2 两级分块流程

定义位置：`internal/infrastructure/chunker/strategy.go:148`

```
┌─────────────────────────────────────────────────────────────┐
│ Step 1: 用 parentCfg 切分全文 → parents []Chunk             │
│   parents = Split(text, parentCfg)                          │
│   （策略感知：heading/heuristic/recursive 自适应选择）       │
├─────────────────────────────────────────────────────────────┤
│ Step 2: 遍历每个 parent，用 childCfg 切分 → subs []Chunk    │
│   for _, parent := range parents {                          │
│       subs = Split(parent.Content, childCfg)                │
│                                                             │
│       // 优化：若 parent 只切出 1 个子块且内容与 parent 相同 │
│       // 则不保留 parent（parentIndex = -1）                 │
│       if len(subs) > 1 || (len(subs)==1 && subs[0]!=parent)│
│           parentIndex = len(newParents)                     │
│           newParents = append(newParents, parent)           │
│       }                                                     │
│                                                             │
│       // 调整子块偏移量到文档级                              │
│       for _, sub := range subs {                            │
│           sub.Seq = childSeq                                │
│           sub.Start += parent.Start                         │
│           sub.End   += parent.Start                         │
│           sub.ContextHeader = mergeBreadcrumbs(             │
│               parent.ContextHeader, sub.ContextHeader)      │
│           children = append(children, ChildChunk{...})      │
│           childSeq++                                        │
│       }                                                     │
│   }                                                         │
└─────────────────────────────────────────────────────────────┘
```

### 4.3 关键优化：无意义父子关系的剔除

```go
parentIndex := -1
if len(subs) > 1 || (len(subs) == 1 && subs[0].Content != parent.Content) {
    parentIndex = len(newParents)
    newParents = append(newParents, parent)
}
```

当一个 parent 只切出 1 个子块且内容与 parent 完全相同时（即 parent 本身就足够小），**不保留 parent**，该子块的 `ParentIndex = -1`，退化为普通 flat chunk。这避免了无意义的父子关系和额外的 DB 存储。

### 4.4 偏移量调整

子块偏移量从"相对 parent 内容"调整为"文档级"：

```go
sub.Start += parent.Start
sub.End   += parent.Start
```

使用**加法偏移**而非基于 Content 长度的计算，确保带 `ContextHeader` 前置的 chunk 仍保持正确的位置追踪。

### 4.5 ContextHeader 面包屑合并

定义位置：`internal/infrastructure/chunker/strategy.go:245`

```go
func mergeBreadcrumbs(parent, child string) string
```

子块在 parent 内容上重新运行标题检测时，其首个面包屑行通常与 parent 的最后一个面包屑行重复（parent 的开头标题位于子块输入顶部）。`mergeBreadcrumbs` 去除该重复，避免 embedding 上下文冗余。

### 4.6 SplitTextParentChild（纯文本版）

定义位置：`internal/infrastructure/chunker/splitter.go:860`

`SplitTextParentChild` 是不感知策略的纯文本两级分块，逻辑与 `splitParentChild` 相同但不调用 `Split`（策略感知），而是调用 `SplitText`。用于不需要策略分级的场景。

***

## 5. 存储与索引

### 5.1 processChunks 中的父子处理

定义位置：`internal/application/service/knowledge_process.go:371`

#### 5.1.1 创建 parent DB chunks

```go
hasParentChild := len(options.ParentChunks) > 0
var parentDBChunks []*types.Chunk
if hasParentChild {
    parentDBChunks = make([]*types.Chunk, len(options.ParentChunks))
    for i, pc := range options.ParentChunks {
        parentDBChunks[i] = &types.Chunk{
            ID:              uuid.New().String(),
            Content:         pc.Content,
            ChunkIndex:      pc.Seq,
            ChunkType:       types.ChunkTypeParentText,  // 关键：parent_text 类型
            StartAt:         pc.Start,
            EndAt:           pc.End,
            // ...
        }
    }
    // 设置 parent 之间的 prev/next 链
    for i := range parentDBChunks {
        if i > 0 {
            parentDBChunks[i-1].NextChunkID = parentDBChunks[i].ID
            parentDBChunks[i].PreChunkID    = parentDBChunks[i-1].ID
        }
    }
}
```

#### 5.1.2 创建 child DB chunks 并关联 parent

```go
for idx, chunkData := range chunks {
    textChunk := &types.Chunk{
        Content:   chunkData.Content,
        ChunkType: types.ChunkTypeText,
        // ...
    }
    // 关联父 chunk
    if hasParentChild && chunkData.ParentIndex >= 0 && chunkData.ParentIndex < len(parentDBChunks) {
        textChunk.ParentChunkID = parentDBChunks[chunkData.ParentIndex].ID
    }
    insertChunks = append(insertChunks, textChunk)
}
```

`ParentIndex`（切分阶段临时索引）在此被替换为真实的 `ParentChunkID`（DB UUID）。

#### 5.1.3 child 之间不设 prev/next 链

```go
// 设置文本 Chunk 之间的前后关系 (skip if parent-child, children don't need prev/next links)
if !hasParentChild {
    for i, chunk := range textChunks {
        // ... 设置 prev/next
    }
}
```

父子模式下，**child chunk 之间不设置 prev/next 链**。原因：child 的上下文通过 parent 提供，相邻关系由 parent 内的 child 序列隐含表达。

### 5.2 DB 写入

```go
// parent chunks 先入（DB 但不入向量索引）
if hasParentChild {
    insertChunks = append(insertChunks, parentDBChunks...)
}
// child chunks 后入
insertChunks = append(insertChunks, textChunks...)

// 统一写入 DB
s.chunkService.CreateChunks(ctx, insertChunks)
```

### 5.3 向量索引：仅 child

定义位置：`internal/application/service/knowledge_process.go:512`

```go
// Create index information — only for child/flat chunks, NOT parent chunks.
// Parent chunks are stored for context retrieval but do not need vector embeddings.
indexInfoList := make([]*types.IndexInfo, 0, len(textChunks))
for _, chunk := range textChunks {
    indexContent := buildKnowledgeIndexContent(knowledge, chunk.EmbeddingContent())
    indexInfoList = append(indexInfoList, &types.IndexInfo{
        Content:    indexContent,
        SourceID:   chunk.ID,
        ChunkID:    chunk.ID,
        // ...
    })
}
retrieveEngine.BatchIndex(ctx, embeddingModel, indexInfoList)
```

**关键约束**：`textChunks` 切片在父子模式下只包含 `ChunkType == ChunkTypeText && ParentChunkID != ""` 的 child chunk，parent chunk 被显式排除。

### 5.4 存储与索引对照表

| Chunk 类型                      | DB 存储 | 向量索引 | 用途     |
| ----------------------------- | ----- | ---- | ------ |
| `parent_text`                 | ✅     | ❌    | 上下文富化  |
| `text`（child）                 | ✅     | ✅    | 向量匹配   |
| `text`（flat，无父）               | ✅     | ✅    | 向量匹配   |
| `image_ocr` / `image_caption` | ✅     | ✅    | 图片文本匹配 |
| `summary`                     | ✅     | ✅    | 摘要匹配   |

***

## 6. 检索阶段：上下文富化

### 6.1 两条富化路径

WeKnora 中父 chunk 富化有两条路径，分别服务于不同场景：

| 路径       | 触发场景                              | `SkipContextEnrichment` | 实现位置                              |
| -------- | --------------------------------- | ----------------------- | --------------------------------- |
| **即时富化** | `SearchKnowledge` API、`rag` 静态流水线 | `false`                 | `knowledgebase_search_results.go` |
| **延迟富化** | RAG 主流水线（`CHUNK_SEARCH_PARALLEL`） | `true`                  | `chat_pipeline/merge.go`          |

RAG 主流水线设置 `SkipContextEnrichment=true`，将父 chunk 富化推迟到 `CHUNK_MERGE` 阶段统一处理，避免检索与 merge 重复加载父 chunk。

### 6.2 processSearchResults：即时富化

定义位置：`internal/application/service/knowledgebase_search_results.go:14`

#### 6.2.1 collectEnrichmentChunkIDs

```go
func (s *knowledgeBaseService) collectEnrichmentChunkIDs(
    ctx context.Context, allChunks []*types.Chunk, idx *chunkIndex,
) []string {
    for _, chunk := range allChunks {
        // 收集父 chunk
        if chunk.ParentChunkID != "" && !idx.processedIDs[chunk.ParentChunkID] {
            additionalIDs = append(additionalIDs, chunk.ParentChunkID)
            idx.processedIDs[chunk.ParentChunkID] = true
            idx.scores[chunk.ParentChunkID]     = idx.scores[chunk.ID]       // 继承 child 分数
            idx.matchTypes[chunk.ParentChunkID] = types.MatchTypeParentChunk // 标记为父匹配
        }
        // 收集关系 chunk、邻近 chunk ...
    }
}
```

**关键设计**：父 chunk 继承对应 child 的分数，并标记 `MatchTypeParentChunk`。

#### 6.2.2 二轮父 chunk 解析（图片场景）

```go
// Second round: only needed when image chunks are among primary results
// (image → text resolved above, now text → parent_text).
if s.hasImageChunks(allChunks) {
    parentIDs := s.collectParentChunkIDs(additionalChunks, index)
    if len(parentIDs) > 0 {
        parentChunks, err := s.listChunksByIDWithShared(ctx, tenantID, parentIDs)
        // ...
    }
}
```

当主结果包含图片 chunk 时，需要二轮解析：图片 chunk 的父是 text chunk，text chunk 的父才是 `parent_text`。`collectParentChunkIDs` 专门用于这种"只解析父链、不扩展邻近/关系"的二轮场景。

#### 6.2.3 assembleSearchResults

```go
// First pass: 主结果按原始顺序
for _, inputChunk := range inputChunks {
    // ...
}

// Second pass: 富化结果（parent, nearby, relation）
if !skipEnrichment {
    for chunkID, chunk := range chunkMap {
        if addedChunkIDs[chunkID] || !s.isSearchableChunk(chunk) {
            continue
        }
        // ...
    }
}
```

#### 6.2.4 isSearchableChunk：父 chunk 不入结果

定义位置：`internal/application/service/knowledgebase_search_results.go:337`

```go
func (s *knowledgeBaseService) isSearchableChunk(chunk *types.Chunk) bool {
    // ...
    return slices.Contains([]types.ChunkType{
        types.ChunkTypeText, types.ChunkTypeSummary,
        types.ChunkTypeTableColumn, types.ChunkTypeTableSummary,
        types.ChunkTypeFAQ,
        types.ChunkTypeImageOCR, types.ChunkTypeImageCaption,
    }, chunk.ChunkType)
}
```

**关键约束**：`ChunkTypeParentText` **不在**可搜索列表中。父 chunk 虽被加载到 `chunkMap`，但不会出现在最终 `SearchResult` 列表中——它的内容通过 merge 阶段合并到 child 结果中。

### 6.3 去重：不按 ParentChunkID 去重

定义位置：`internal/application/service/chat_pipeline/search.go:188`

```go
// Only deduplicate by exact chunk ID — do NOT treat shared ParentChunkID
// as duplicates, because different child chunks of the same parent carry
// different content segments that may all be relevant.
if seen[r.ID] {
    continue
}
```

**关键设计**：同一 parent 的不同 child 可能携带不同内容片段，都可能与查询相关，因此**不按** **`ParentChunkID`** **去重**，只按 chunk ID 去重。

***

## 7. Merge 阶段：父 chunk 解析

### 7.1 CHUNK\_MERGE 流程概览

定义位置：`internal/application/service/chat_pipeline/merge.go:44`

```
Step 1: selectInputResults     — 选择 rerank 或 search 结果
Step 2: dedup                  — 初始去重
Step 3: injectHistoryResults   — 注入历史引用
Step 4: resolveParentChunks    — ★ 父 chunk 解析（本节重点）
Step 5: groupAndMergeCurrentContent — 按知识源+类型分组，合并连续区间
Step 6: populateFAQAnswers     — 填充 FAQ 答案
Step 7: expandShortContextWithNeighbors — 扩展短上下文
Step 7.5: groupAndMergeCurrentContent  — 再次合并
Step 8: dedup + removePartialOverlaps   — 最终去重
```

### 7.2 resolveParentChunks：核心解析

定义位置：`internal/application/service/chat_pipeline/merge.go:218`

#### 7.2.1 收集父 chunk ID 并批量加载

```go
parentIDs := make(map[string]struct{})
for _, r := range results {
    if r.ParentChunkID != "" {
        parentIDs[r.ParentChunkID] = struct{}{}
    }
}
// 批量获取父 chunk
parentChunks, err := p.chunkRepo.ListChunksByID(ctx, tenantID, ids)
parentMap := make(map[string]*types.Chunk, len(parentChunks))
for _, c := range parentChunks {
    parentMap[c.ID] = c
}
```

#### 7.2.2 图片场景的祖父 chunk 解析

图片命中存在 **image → text → parent\_text** 三级链：

```go
// Image hits have an image -> text -> parent_text chain.
imageTextParentIDs := make(map[string]struct{})
for _, r := range results {
    if r.ChunkType == string(types.ChunkTypeImageOCR) ||
       r.ChunkType == string(types.ChunkTypeImageCaption) {
        imageTextParentIDs[r.ParentChunkID] = struct{}{}
    }
}
// 对每个图片命中的 text 父，再解析其 parent_text 祖父
for _, parent := range parentChunks {
    if _, needed := imageTextParentIDs[parent.ID]; !needed { continue }
    if parent.ParentChunkID == "" || parent.ChunkType != types.ChunkTypeText { continue }
    // 收集祖父 ID
    grandparentIDs = append(grandparentIDs, parent.ParentChunkID)
}
// 批量获取祖父 chunk
grandparents, fetchErr := p.chunkRepo.ListChunksByID(ctx, tenantID, grandparentIDs)
```

#### 7.2.3 图片信息作用域化

```go
// Batch-fetch image_info scoped to matched text children only.
textChildIDs := collectScopedTextChildIDs(results, parentMap)
var scopedImageInfo map[string]string
if len(textChildIDs) > 0 {
    scopedImageInfo = searchutil.CollectImageInfoByChunkIDs(ctx, p.chunkRepo, tenantID, textChildIDs)
}
```

**关键设计**：图片信息按"匹配到的 text child"作用域化，避免图片密集的 parent 注入每个兄弟页面的 OCR/Caption。

#### 7.2.4 text → parent\_text：扩展为完整父内容

```go
case string(types.ChunkTypeText):
    parent, ok := parentMap[r.ParentChunkID]
    if !ok || parent.Content == "" || parent.ChunkType != types.ChunkTypeParentText {
        continue
    }
    // 作用域化图片信息到当前 child
    assignScopedImageInfo(r, scopedImageInfo, r.ID)
    // 按 ImageInfo 修剪 parent 中的 Markdown 图片
    parentContent := searchutil.PruneMarkdownImagesByImageInfo(parent.Content, r.ImageInfo)
    // 拼接：parent 内容 + child 内容
    r.Content = searchutil.JoinChunkContent(parentContent, r.Content, "\n\n")
    r.ContentRewritten = true
    // 记录子 chunk ID
    if !containsID(r.SubChunkID, r.ID) {
        r.SubChunkID = append(r.SubChunkID, r.ID)
    }
```

**核心价值**：text child 命中后，用完整 parent 内容补全上下文，这是父子 chunk 的核心价值所在。

#### 7.2.5 image → text → parent\_text：三级链解析

```go
case string(types.ChunkTypeImageOCR), string(types.ChunkTypeImageCaption):
    textParent, ok := parentMap[r.ParentChunkID]
    if !ok || textParent.Content == "" || textParent.ChunkType != types.ChunkTypeText {
        continue
    }
    contentSource := textParent
    // 若 text parent 还有 parent_text 祖父，用祖父作为上下文源
    if textParent.ParentChunkID != "" {
        if grandparent, found := parentMap[textParent.ParentChunkID]; found &&
            grandparent.ChunkType == types.ChunkTypeParentText && grandparent.Content != "" {
            contentSource = grandparent
        }
    }
    r.Content = textParent.Content
    r.ChunkIndex = textParent.ChunkIndex
    // 作用域化图片信息
    assignScopedImageInfo(r, scopedImageInfo, textParent.ID)
    // 拼接：祖父上下文 + text parent 内容
    textContent := searchutil.PruneMarkdownImagesByImageInfo(textParent.Content, r.ImageInfo)
    parentContent := searchutil.PruneMarkdownImagesByImageInfo(contentSource.Content, r.ImageInfo)
    r.Content = searchutil.JoinChunkContent(parentContent, textContent, "\n\n")
    r.ContentRewritten = true
```

### 7.3 坐标系不变性

测试验证：`TestResolveParentChunksUsesCurrentContentAndImageURLs`

```go
result := &types.SearchResult{
    ID: "child", ChunkType: string(types.ChunkTypeText),
    ParentChunkID: "parent", Content: "current edited child body",
    StartAt: 999, EndAt: 1001,  // 编辑后的坐标
}
got := plugin.resolveParentChunks(ctx, &types.ChatManage{}, []*types.SearchResult{result})
// 断言：坐标不变
if got[0].StartAt != 999 || got[0].EndAt != 1001 {
    t.Fatalf("source coordinates changed")
}
```

**关键设计**：merge 阶段**不修改** `StartAt`/`EndAt` 坐标。父 chunk 内容通过 URL 作用域化（而非按坐标切片）拼接，保留 child 的编辑后坐标。

### 7.4 collectScopedTextChildIDs

定义位置：`internal/application/service/chat_pipeline/merge.go:390`

```go
func collectScopedTextChildIDs(results []*types.SearchResult, parentMap map[string]*types.Chunk) []string {
    for _, r := range results {
        if r.ParentChunkID == "" { continue }
        switch r.ChunkType {
        case string(types.ChunkTypeText):
            // text child：父必须是 parent_text
            parent := parentMap[r.ParentChunkID]
            if parent == nil || parent.ChunkType != types.ChunkTypeParentText { continue }
            ids = append(ids, r.ID)
        case string(types.ChunkTypeImageOCR), string(types.ChunkTypeImageCaption):
            // image child：收集其 text parent 的 ID
            ids = append(ids, r.ParentChunkID)
        }
    }
}
```

***

## 8. 编辑维护：父子一致性

### 8.1 编辑 child 后重建 parent

定义位置：`internal/application/service/chunk.go:580`

当用户编辑 child chunk 内容后，需要将编辑后的 child 内容叠加到 parent 的不可变源内容上：

```go
func (s *chunkService) rebuildParentContent(ctx context.Context, edited *types.Chunk) error {
    parent, err := s.chunkRepository.GetChunkByID(ctx, edited.TenantID, edited.ParentChunkID)
    children, err := s.chunkRepository.ListChunkByParentID(ctx, edited.TenantID, parent.ID)

    base := parent.SourceContent  // 不可变原始内容
    if base == "" {
        base = parent.Content
        parent.SourceContent = base
    }
    baseRunes := []rune(base)

    // 收集所有已编辑 child 的替换区间
    type replacement struct { start, end int; content string; updatedAt time.Time }
    for _, child := range children {
        if child.ContentRevision == 0 { continue }  // 跳过未编辑的
        start, end := child.StartAt-parent.StartAt, child.EndAt-parent.StartAt
        if start >= 0 && end >= start && end <= len(baseRunes) {
            replacements = append(replacements, replacement{start, end, child.Content, child.UpdatedAt})
        }
    }
```

### 8.2 冲突处理：保留最新编辑

```go
// 按 updatedAt 降序排序
sort.Slice(replacements, func(i, j int) bool {
    return replacements[i].updatedAt.After(replacements[j].updatedAt)
})

selected := make([]replacement, 0)
conflicts := make([]replacement, 0)
for _, candidate := range replacements {
    overlaps := false
    for _, existing := range selected {
        if candidate.start < existing.end && candidate.end > existing.start {
            overlaps = true; break
        }
    }
    if !overlaps {
        selected = append(selected, candidate)
    } else {
        conflicts = append(conflicts, candidate)
    }
}
```

**关键设计**：重叠的编辑区间无法同时占据同一源区间。保留最新编辑，冲突的编辑追加到 parent 内容末尾（宁可少量重复，也不丢失已接受的编辑）。

### 8.3 逆序应用替换

```go
// 逆序应用（保留坐标有效性）
sort.Slice(replacements, func(i, j int) bool {
    return replacements[i].start > replacements[j].start
})
for _, repl := range replacements {
    baseRunes = append(
        append(append([]rune{}, baseRunes[:repl.start]...), []rune(repl.content)...),
        baseRunes[repl.end:]...,
    )
}
parent.Content = string(baseRunes)
// 冲突编辑追加
for _, conflict := range conflicts {
    parent.Content = searchutil.JoinChunkContent(parent.Content, conflict.content, "\n\n")
}
```

### 8.4 编辑触发链

定义位置：`internal/application/service/chunk.go:467`

```go
bodyChanged := newContent != chunk.Content
// ...
if bodyChanged && chunk.ParentChunkID != "" {
    if err := s.rebuildParentContent(ctx, chunk); err != nil {
        // ...
    }
}
if bodyChanged || newEnabled != revision.IsEnabled {
    if err := s.syncEditedChunkImages(ctx, chunk); err != nil { /* ... */ }
}
if bodyChanged || newEnabled != revision.IsEnabled {
    // 触发摘要刷新
    enqueueSummaryRefresh(ctx, ...)
}
if err := s.syncChunkIndex(ctx, chunk); err != nil { /* ... */ }
```

编辑 child 后的完整链：重建 parent → 同步图片子 chunk → 触发摘要刷新 → 重建向量索引。

### 8.5 测试验证

`TestRebuildParentContentPreservesConflictingEdits`（`chunk_edit_parent_test.go:205`）验证：两个重叠的编辑区间（older 和 newer）都被保留在重建后的 parent 内容中。

***

## 9. 克隆与移动

### 9.1 父子 ID 重映射

定义位置：`internal/application/service/knowledge_clone_move.go:377`

```go
// 克隆时复制 ParentChunkID
targetChunk := &types.Chunk{
    // ...
    ParentChunkID: sourceChunk.ParentChunkID,
    // ...
}
srcTodst[sourceChunk.ID] = targetChunk.ID

// 第二轮：重映射 ID
for _, targetChunk := range targetChunks {
    if val, ok := srcTodst[targetChunk.ParentChunkID]; ok {
        targetChunk.ParentChunkID = val  // 映射到新 parent ID
    } else {
        targetChunk.ParentChunkID = ""   // 无对应则清空
    }
    // 同样处理 PreChunkID / NextChunkID
}
```

**关键设计**：克隆时先保留原 `ParentChunkID`，建立 `srcID → dstID` 映射后，第二轮统一重映射所有父子、前后关系 ID。

***

## 10. 图片 chunk 的父子链

### 10.1 图片 chunk 的父是 text chunk

图片 OCR/Caption chunk 的 `ParentChunkID` 指向所属的 `text` chunk（而非 `parent_text`）：

```go
// internal/application/service/image_multimodal.go:311
newChunks = append(newChunks, &types.Chunk{
    ChunkType:     types.ChunkTypeImageOCR,
    ParentChunkID: payload.ChunkID,  // text chunk ID
    // ...
})
```

### 10.2 三级链：image → text → parent\_text

在父子分块模式下，图片命中的完整链路为：

```
parent_text (大窗口上下文)
    └── text (child, 向量匹配)
            └── image_ocr / image_caption (图片文本匹配)
```

merge 阶段的 `resolveParentChunks` 专门处理这种三级链（见 [7.2.5 节](#725-image--text--parent_text三级链解析)）。

### 10.3 图片信息收集的两级解析

定义位置：`internal/searchutil/imageinfo.go:50`

`CollectImageInfoByChunkIDs` 支持两级解析：

```go
// 若 chunkIDs 是 text chunk，其直接子是 image chunk → 一次查询
// 若 chunkIDs 是 parent_text chunk，其子是 text chunk，
//     text chunk 的子才是 image chunk → 两次查询
children, err := chunkRepo.ListChunksByParentIDs(ctx, tenantID, chunkIDs)
for _, child := range children {
    switch child.ChunkType {
    case types.ChunkTypeImageOCR, types.ChunkTypeImageCaption:
        addInfo(child.ParentChunkID, child)  // text → image
    case types.ChunkTypeText:
        textChildIDs = append(textChildIDs, child.ID)
        textToParent[child.ID] = child.ParentChunkID  // parent_text → text
    }
}
// 二轮：text → image
if len(textChildIDs) > 0 {
    grandChildren, err := chunkRepo.ListChunksByParentIDs(ctx, tenantID, textChildIDs)
    for _, gc := range grandChildren {
        if parentTextID, ok := textToParent[gc.ParentChunkID]; ok {
            addInfo(parentTextID, gc)  // parent_text → image（经 text 中转）
        }
    }
}
```

### 10.4 摘要 chunk 的父

```go
// internal/application/service/knowledge_process.go:1303
summaryChunk := &types.Chunk{
    ChunkType:     types.ChunkTypeSummary,
    ParentChunkID: textChunks[0].ID,  // 关联到首个文本 chunk
    // ...
}
```

摘要 chunk 的父是首个 text chunk，用于摘要归属追踪，不参与父子分块的上下文富化逻辑。

***

## 11. 关键源码索引

### 11.1 类型定义

| 文件                                   | 行号  | 内容                                                                       |
| ------------------------------------ | --- | ------------------------------------------------------------------------ |
| `internal/types/chunk.go`            | 15  | `ChunkType` 常量（`ChunkTypeParentText` 等）                                  |
| `internal/types/chunk.go`            | 113 | `Chunk` 结构体（`ParentChunkID` 字段在 158 行）                                   |
| `internal/types/chunk.go`            | 180 | `ChunkRevision` 编辑历史快照                                                   |
| `internal/types/knowledgebase.go`    | 184 | `ChunkingConfig`（`EnableParentChild`、`ParentChunkSize`、`ChildChunkSize`） |
| `internal/types/docparser.go`        | 72  | `ParsedChunk`（`ParentIndex` 字段在 89 行）                                    |
| `internal/types/docparser.go`        | 108 | `ParsedParentChunk`                                                      |
| `internal/types/embedding.go`        | 15  | `MatchType`（`MatchTypeParentChunk` 在 20 行）                               |
| `internal/types/search.go`           | 180 | `SearchResult`（`ParentChunkID` 字段）                                       |
| `internal/types/interfaces/chunk.go` | 55  | `ListChunkByParentID` 接口                                                 |
| `internal/types/interfaces/chunk.go` | 57  | `ListChunksByParentIDs` 接口                                               |

### 11.2 切分阶段

| 文件                                            | 行号  | 内容                               |
| --------------------------------------------- | --- | -------------------------------- |
| `internal/infrastructure/chunker/strategy.go` | 135 | `SplitParentChild` 策略感知入口        |
| `internal/infrastructure/chunker/strategy.go` | 148 | `splitParentChild` 两级分块实现        |
| `internal/infrastructure/chunker/strategy.go` | 218 | `DeriveParentChildConfigs` 派生配置  |
| `internal/infrastructure/chunker/strategy.go` | 245 | `mergeBreadcrumbs` 面包屑合并         |
| `internal/infrastructure/chunker/splitter.go` | 844 | `ParentChildResult`、`ChildChunk` |
| `internal/infrastructure/chunker/splitter.go` | 860 | `SplitTextParentChild` 纯文本版      |

### 11.3 入库阶段

| 文件                                                         | 行号   | 内容                                      |
| ---------------------------------------------------------- | ---- | --------------------------------------- |
| `internal/application/service/knowledge_create.go`         | 1234 | 知识创建时父子分块分支                             |
| `internal/application/service/knowledge_process.go`        | 239  | `buildParentChildConfigs`               |
| `internal/application/service/knowledge_process.go`        | 371  | `hasParentChild` 逻辑，创建 parent DB chunks |
| `internal/application/service/knowledge_process.go`        | 431  | child chunk 关联 `ParentChunkID`          |
| `internal/application/service/knowledge_process.go`        | 447  | `textChunks` 过滤（仅 child）                |
| `internal/application/service/knowledge_process.go`        | 512  | 向量索引（仅 child，排除 parent）                 |
| `internal/application/service/knowledge_process_config.go` | 266  | `EnableParentChild` 配置覆盖                |

### 11.4 检索阶段

| 文件                                                             | 行号  | 内容                                    |
| -------------------------------------------------------------- | --- | ------------------------------------- |
| `internal/application/service/knowledgebase_search_results.go` | 14  | `processSearchResults` 即时富化           |
| `internal/application/service/knowledgebase_search_results.go` | 129 | `collectEnrichmentChunkIDs` 收集父 ID    |
| `internal/application/service/knowledgebase_search_results.go` | 179 | `collectParentChunkIDs` 二轮父解析         |
| `internal/application/service/knowledgebase_search_results.go` | 207 | `assembleSearchResults` 组装结果          |
| `internal/application/service/knowledgebase_search_results.go` | 337 | `isSearchableChunk`（排除 `parent_text`） |
| `internal/application/service/chat_pipeline/search.go`         | 188 | 去重（不按 `ParentChunkID`）                |

### 11.5 Merge 阶段

| 文件                                                                        | 行号  | 内容                               |
| ------------------------------------------------------------------------- | --- | -------------------------------- |
| `internal/application/service/chat_pipeline/merge.go`                     | 44  | `OnEvent` merge 流程               |
| `internal/application/service/chat_pipeline/merge.go`                     | 218 | `resolveParentChunks` 父 chunk 解析 |
| `internal/application/service/chat_pipeline/merge.go`                     | 271 | 图片祖父 chunk 解析                    |
| `internal/application/service/chat_pipeline/merge.go`                     | 317 | text/image 分支处理                  |
| `internal/application/service/chat_pipeline/merge.go`                     | 390 | `collectScopedTextChildIDs`      |
| `internal/application/service/chat_pipeline/merge.go`                     | 424 | `assignScopedImageInfo`          |
| `internal/application/service/chat_pipeline/merge_parent_resolve_test.go` | 13  | 父 chunk 解析测试                     |
| `internal/application/service/chat_pipeline/merge_parent_resolve_test.go` | 55  | 图片祖父链测试                          |

### 11.6 编辑维护

| 文件                                                       | 行号  | 内容                                  |
| -------------------------------------------------------- | --- | ----------------------------------- |
| `internal/application/service/chunk.go`                  | 413 | 编辑重试时重建 parent                      |
| `internal/application/service/chunk.go`                  | 467 | 编辑后触发 `rebuildParentContent`        |
| `internal/application/service/chunk.go`                  | 528 | `syncEditedChunkImages` 同步图片子 chunk |
| `internal/application/service/chunk.go`                  | 580 | `rebuildParentContent` 重建 parent 内容 |
| `internal/application/service/chunk.go`                  | 644 | `syncChunkIndex` 重建向量索引             |
| `internal/application/service/chunk_edit_parent_test.go` | 205 | 冲突编辑保留测试                            |

### 11.7 克隆与移动

| 文件                                                     | 行号  | 内容                    |
| ------------------------------------------------------ | --- | --------------------- |
| `internal/application/service/knowledge_clone_move.go` | 377 | 克隆时复制 `ParentChunkID` |
| `internal/application/service/knowledge_clone_move.go` | 404 | ID 重映射                |

### 11.8 图片信息

| 文件                                                  | 行号   | 内容                                |
| --------------------------------------------------- | ---- | --------------------------------- |
| `internal/searchutil/imageinfo.go`                  | 50   | `CollectImageInfoByChunkIDs` 两级解析 |
| `internal/searchutil/imageinfo.go`                  | 153  | `EnrichSearchResultsImageInfo`    |
| `internal/application/service/image_multimodal.go`  | 311  | 图片 chunk 创建（父为 text）              |
| `internal/application/service/knowledge_process.go` | 2977 | 图片 caption chunk 创建               |
| `internal/application/service/knowledge_process.go` | 2993 | 图片 OCR chunk 创建                   |

### 11.9 存储层

| 文件                                         | 行号  | 内容                            |
| ------------------------------------------ | --- | ----------------------------- |
| `internal/application/repository/chunk.go` | 275 | `ListChunkByParentID` 单父查询    |
| `internal/application/repository/chunk.go` | 289 | `ListChunksByParentIDs` 批量父查询 |

***

## 附录：父子 Chunk 完整生命周期时序图

```
┌─ 配置阶段 ──────────────────────────────────────────────────┐
│  ChunkingConfig.EnableParentChild = true                    │
│  ParentChunkSize = 4096, ChildChunkSize = 384               │
│  → DeriveParentChildConfigs 派生 parent/child SplitterConfig│
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─ 切分阶段 ──────────────────────────────────────────────────┐
│  SplitParentChild(text, parentCfg, childCfg)                │
│    1. Split(text, parentCfg) → parents                      │
│    2. for each parent: Split(parent, childCfg) → children   │
│    3. 优化：parent 只切 1 块且相同 → 不保留 parent           │
│    4. 调整子块偏移量到文档级                                  │
│    5. mergeBreadcrumbs 合并标题面包屑                        │
│    → ParentChildResult{Parents, Children}                   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─ 存储与索引阶段 ────────────────────────────────────────────┐
│  processChunks:                                             │
│    1. 创建 parent DB chunks (ChunkType=parent_text)          │
│    2. 设置 parent 之间 prev/next 链                          │
│    3. 创建 child DB chunks, 关联 ParentChunkID               │
│       (ParentIndex → 真实 ParentChunkID UUID)                │
│    4. child 之间不设 prev/next 链                            │
│    5. CreateChunks(parents + children) → DB                  │
│    6. BatchIndex(仅 child) → 向量索引                        │
│       (parent 显式排除)                                      │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─ 检索阶段（两条路径）──────────────────────────────────────┐
│  路径 A：即时富化 (SearchKnowledge API)                      │
│    processSearchResults(SkipContextEnrichment=false):        │
│      1. collectEnrichmentChunkIDs 收集 ParentChunkID         │
│      2. 批量加载父 chunk 到 chunkMap                         │
│      3. 图片场景二轮解析 (image→text→parent_text)            │
│      4. assembleSearchResults (parent_text 不入结果)         │
│                                                              │
│  路径 B：延迟富化 (RAG 主流水线)                              │
│    HybridSearch(SkipContextEnrichment=true)                  │
│      → 检索结果不含父 chunk，推迟到 merge 阶段                │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─ Merge 阶段（路径 B 的延续）───────────────────────────────┐
│  resolveParentChunks:                                       │
│    1. 收集所有 ParentChunkID                                 │
│    2. 批量加载父 chunk (ListChunksByID)                      │
│    3. 图片场景：解析祖父 parent_text                          │
│    4. 作用域化图片信息 (collectScopedTextChildIDs)           │
│    5. text child: 拼接 parent 内容 + child 内容              │
│    6. image child: 拼接祖父上下文 + text parent 内容         │
│    7. 不修改 StartAt/EndAt 坐标                              │
│    → ContentRewritten=true, SubChunkID 记录子 chunk          │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─ 编辑维护阶段 ──────────────────────────────────────────────┐
│  UpdateDocumentChunk (编辑 child):                           │
│    1. bodyChanged && ParentChunkID != ""                     │
│       → rebuildParentContent:                                │
│         a. 加载 parent.SourceContent (不可变)                │
│         b. 收集所有已编辑 child 的替换区间                    │
│         c. 冲突处理：保留最新，冲突追加末尾                   │
│         d. 逆序应用替换（保留坐标有效性）                     │
│         e. UpdateChunk(parent)                               │
│    2. syncEditedChunkImages (同步图片子 chunk)               │
│    3. enqueueSummaryRefresh (触发摘要刷新)                   │
│    4. syncChunkIndex (重建 child 向量索引)                   │
└─────────────────────────────────────────────────────────────┘
```

