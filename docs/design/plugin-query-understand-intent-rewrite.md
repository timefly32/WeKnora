# PluginQueryUnderstand — 查询改写与意图识别设计

## 1. 概述

`PluginQueryUnderstand` 是 WeKnora RAG Pipeline 中 `QUERY_UNDERSTAND` 事件链上的**外层插件**，负责在检索之前完成三件事：

1. **查询改写（Query Rewrite）**：基于对话历史对用户当前问题做指代消解与省略补全，生成一个自包含、可用于知识库检索的独立问题。
2. **意图分类（Intent Classification）**：将用户问题归类到 9 种意图之一，决定后续管线是否需要执行检索阶段。
3. **图片描述（Image Description）**：当用户附带图片时，生成图片的视觉描述与 OCR 文本，供后续上下文组装与历史回放使用。

源码位置：`internal/application/service/chat_pipeline/query_understand.go`

---

## 2. 在 Pipeline 中的位置

### 2.1 事件链结构

`QUERY_UNDERSTAND` 事件上串联了两个插件，按注册顺序（即责任链外→内层）执行：

| 链序 | 插件 | 职责 |
|------|------|------|
| 外层 | `PluginQueryUnderstand` | 改写 + 意图分类 + 图片描述 |
| 内层 | `PluginExtractEntity` | 图谱实体抽取（需 Neo4j 启用） |

注册顺序见 `internal/container/container.go`：

```go
must(container.Invoke(chatpipeline.NewPluginQueryUnderstand))  // QUERY_UNDERSTAND（链外层）
must(container.Invoke(chatpipeline.NewPluginLoadHistory))      // LOAD_HISTORY
must(container.Invoke(chatpipeline.NewPluginExtractEntity))    // QUERY_UNDERSTAND（链内层）
```

`PluginQueryUnderstand.OnEvent` 在完成改写与意图分类后调用 `next()`，进入 `PluginExtractEntity` 做实体抽取。

### 2.2 管线中的上下游

```
LOAD_HISTORY → QUERY_UNDERSTAND → CHUNK_SEARCH_PARALLEL → ...
                  ↑
                  ├── PluginQueryUnderstand（改写 + 意图 + 图片描述）
                  └── PluginExtractEntity（实体抽取，next() 之后）
```

- **上游输入**：`chatManage.Query`（原始查询）、`chatManage.History`（由 `LOAD_HISTORY` 加载的对话历史）、`chatManage.Images`（附件图片）。
- **下游输出**：`chatManage.RewriteQuery`（改写后查询）、`chatManage.Intent`（意图标签）、`chatManage.ImageDescription`（图片描述）、`chatManage.SystemPromptOverride`（非检索意图的 system prompt 覆盖）。

---

## 3. 核心数据结构

### 3.1 意图枚举（QueryIntent）

定义于 `internal/types/chat_manage.go:84-96`：

```go
type QueryIntent string

const (
    IntentKBSearch      QueryIntent = "kb_search"       // 知识库检索
    IntentWebSearch     QueryIntent = "web_search"      // 网络搜索
    IntentGreeting      QueryIntent = "greeting"        // 问候/感谢/告别
    IntentChitchat      QueryIntent = "chitchat"        // 闲聊
    IntentFollowUp      QueryIntent = "follow_up"       // 上下文追问
    IntentImageOnly     QueryIntent = "image_only"      // 纯图片理解
    IntentDocOnly       QueryIntent = "doc_only"        // 纯文档理解
    IntentSummarize     QueryIntent = "summarize"       // 对话总结
    IntentClarification QueryIntent = "clarification"   // 澄清/模糊问题
)
```

### 3.2 意图与检索的关系

```go
// QueryIntent.NeedsKBRetrieval — 仅以下意图需要知识库检索
func (i QueryIntent) NeedsKBRetrieval() bool {
    switch i {
    case IntentKBSearch, IntentClarification, IntentSummarize, "":
        return true   // 空值默认需要检索（安全兜底）
    default:
        return false
    }
}

// ChatManage.NeedsRetrieval — web_search 额外看 WebSearchEnabled 开关
func (c *ChatManage) NeedsRetrieval() bool {
    if c.Intent == IntentWebSearch {
        return c.WebSearchEnabled
    }
    return c.Intent.NeedsKBRetrieval()
}
```

**关键设计**：`NeedsRetrieval()` 是后续所有检索类插件（`CHUNK_SEARCH_PARALLEL`、`CHUNK_RERANK`、`CHUNK_MERGE` 等）的跳过条件。非检索意图直接跳过检索阶段，走 `INTO_CHAT_MESSAGE` → `CHAT_COMPLETION_STREAM` 路径。

### 3.3 模型输出结构

```go
type queryUnderstandOutput struct {
    RewriteQuery     string            `json:"rewrite_query"`
    Intent           types.QueryIntent `json:"intent"`
    ImageDescription string            `json:"image_description"`
}
```

模型被要求输出严格 JSON：`{"rewrite_query":"...","intent":"kb_search","image_description":"..."}`

---

## 4. 意图重写完整流程

### 4.1 流程图

```
┌─────────────────────────────────────────────────────────┐
│  OnEvent 入口                                            │
│  chatManage.RewriteQuery = chatManage.Query  (默认回退)  │
└──────────────────────┬──────────────────────────────────┘
                       ▼
              ┌────────────────┐
              │ 改写关闭且无图片？│
              └───────┬────────┘
                 是 ↓        否 ↓
        ┌──────────────┐   ┌─────────────────────┐
        │ skip → next()│   │ 加载/复用对话历史     │
        └──────────────┘   └──────────┬──────────┘
                                      ▼
                           ┌────────────────────┐
                           │ selectModel        │
                           │ (文本/视觉模型选择) │
                           └──────────┬─────────┘
                                      ▼
                           ┌────────────────────┐
                           │ buildPrompts       │
                           │ (system + user)    │
                           └──────────┬─────────┘
                                      ▼
                           ┌────────────────────┐
                           │ rewriteModel.Chat  │
                           │ (Temperature=0.3)  │
                           └──────────┬─────────┘
                                      ▼
                           ┌────────────────────┐
                           │ parseOutput        │
                           │ (JSON 解析 + 容错)  │
                           └──────────┬─────────┘
                                      ▼
              ┌─────────────────────────────────────────┐
              │ 有图片描述？异步回写 ImageCaption        │
              └────────────────┬────────────────────────┘
                               ▼
              ┌─────────────────────────────────────────┐
              │ NeedsRetrieval() == false？             │
              │   → applyIntentPromptOverride           │
              │   → 设置 SystemPromptOverride           │
              └────────────────┬────────────────────────┘
                               ▼
                          next()  → PluginExtractEntity
```

### 4.2 跳过条件

```go
needRewrite := chatManage.EnableRewrite
if !needRewrite && !hasImages {
    // 改写关闭且无图片 → 直接跳过，RewriteQuery 保持原始 Query
    return next()
}
```

- `EnableRewrite=false` 且无图片：跳过改写，`RewriteQuery = Query`，`Intent` 保持零值（空字符串，`NeedsKBRetrieval()` 返回 true，安全兜底为检索模式）。
- `EnableRewrite=false` 但有图片：仍需执行，目的是生成图片描述。
- `EnableRewrite=true`：无论是否有图片都执行改写。

### 4.3 历史加载

```go
var historyList []*types.History
if len(chatManage.History) > 0 {
    historyList = chatManage.History  // 复用 LOAD_HISTORY 已加载的历史
} else {
    historyList = p.loadHistory(ctx, chatManage)  // 自行加载
}
```

- 优先复用 `LOAD_HISTORY` 阶段已加载到 `chatManage.History` 的历史（避免重复查库）。
- 若历史为空（如纯聊天管线未走 `LOAD_HISTORY`），则自行调用 `loadAndProcessHistory` 加载。
- `MaxRounds <= 0` 表示 Agent 显式关闭多轮，直接返回 nil，**不**回退全局默认。

历史格式化函数 `formatConversationHistory` 将每轮对话包装为：

```
------BEGIN------
User question: <用户问题>
Assistant answer: <助手回答>
------END------
```

### 4.4 模型选择（selectModel）

根据是否有图片走不同路径：

| 场景 | 模型选择逻辑 | useImages |
|------|-------------|-----------|
| 有图片，ChatModel 支持视觉 | `ChatModelID` | true |
| 有图片，ChatModel 不支持视觉 | `VLMModelID`（专用视觉模型） | true |
| 有图片，无视觉模型可用 | 回退到文本模型 | false（图片描述为空） |
| 无图片 | `QueryUnderstandModelID` → 回退 `ChatModelID` | false |

**专用小模型支持**：`QueryUnderstandModelID` 允许为改写阶段单独指定一个轻量模型（降低成本与延迟）。若该模型不可用（被删除/禁用），回退到 `ChatModelID`。

### 4.5 Prompt 构建（buildPrompts）

#### 4.5.1 模板来源

| 配置项 | 默认来源 | Agent 覆盖 |
|--------|---------|-----------|
| System Prompt | `config.Conversation.RewritePromptSystem` | `chatManage.RewritePromptSystem` |
| User Prompt | `config.Conversation.RewritePromptUser` | `chatManage.RewritePromptUser` |

默认模板文件：`config/prompt_templates/rewrite.yaml`，id 为 `default_rewrite`。

#### 4.5.2 占位符渲染

```go
vals := types.PlaceholderValues{
    "conversation": conversationText,  // 格式化后的对话历史
    "query":        queryContent,       // 当前查询 + 图片/附件元信息
    "language":     chatManage.Language,
}
```

`queryContent` 在原始查询后追加图片/附件的存在性标记：

- 有图片：`\n\n<images_uploaded count="N" />`
- 无图片：`\n\n<no_image_attached />`
- 有附件：`Attachments.BuildPrompt()` 输出
- 无附件：`\n<no_document_attached />`

这些标记是意图分类的关键信号——`image_only` 意图要求 `<images_uploaded>` 存在，`doc_only` 要求实际附件存在。

#### 4.5.3 System Prompt 核心内容（rewrite.yaml default_rewrite）

System Prompt 要求模型完成三个任务：

**Task 1: 查询改写**
- 指代消解：将"它"、"这个"、"那个"等代词替换为明确主语
- 省略补全：补全缺失的关键信息，确保语义完整
- 保留原始含义和表达风格
- 改写结果必须是一个问题，不超过 30 个词
- **必须使用 `{language}` 语言**
- **关键约束**：改写结果用于知识库检索，必须保留具体实体、关键词和核心搜索词。禁止生成"请在知识库中查找..."等元指令，应产出包含实际搜索关键词的自包含问题
- **例外**：当用户想广泛阅读/浏览/整理/导出知识库内容而无特定搜索词时（如"请整理知识库中的数据"），保留原始查询的关键描述符，不要剥离为元指令

**Task 2: 意图分类**

按以下**优先级从上到下**匹配，使用第一个匹配项：

| 优先级 | 意图 | 判定条件 |
|--------|------|---------|
| 1 | `greeting` | 纯问候/感谢/告别，无实质问题 |
| 2 | `summarize` | 要求总结/整理**对话本身**。**关键**：若提及"知识库"/文档/文件/报告，则不是 summarize，应为 kb_search |
| 3 | `web_search` | 明确要求实时/最新/外部信息 |
| 4 | `kb_search` | 想搜索/查找/查询/阅读/浏览/整理/列出/提取知识库信息。包括特定搜索和广泛访问请求。**即使附带图片/文档，若意图涉及搜索匹配存储文档，仍为 kb_search** |
| 5 | `clarification` | 问题模糊或不完整，可能需要 KB 检索才能回答好 |
| 6 | `follow_up` | 明确引用之前对话内容（包括之前轮次上传但当前未重新附带的图片/文档），可从历史回答，无需新检索 |
| 7 | `image_only` | 仅想理解/描述/翻译/提取**当前附带图片**本身内容。**要求 `<images_uploaded>` 存在** |
| 8 | `doc_only` | 仅想理解/总结/翻译/提取**当前附带文档**本身内容。**要求实际附件存在** |
| 9 | `chitchat` | 闲聊/小事，无需检索 |

**默认**：不确定时始终选择 `kb_search`。

**关键区分规则**（Prompt 中以示例形式给出）：

- `image_only`/`doc_only` vs `kb_search`（带附件时）：
  - 上传图片/文档 + "这是什么"/"总结一下" → `image_only`/`doc_only`
  - 上传图片/文档 + "知识库里有这个吗" → `kb_search`
  - 上传图片/文档 + "帮我找相关文档" → `kb_search`

- `follow_up` vs `kb_search`：
  - "上面第二点再详细说说"（历史有足够上下文）→ `follow_up`
  - "这个话题还有什么相关的内容" → `kb_search`（需要新检索）
  - 追问分析之前轮次的图片/文档（历史已描述，当前未重新附带）→ `follow_up`
  - 追问明确要求搜索知识库 → `kb_search`

**Task 3: 图片分析**
- 有图片时必须提供非空 `image_description`
- 包含对象、场景、布局、关系及可见关键细节
- 图片含文字时，尽可能完整地包含 OCR 文本
- 视觉描述与 OCR 同时存在时，两者都包含
- 无图片时设为空字符串

**输出格式**：仅输出单个 JSON 对象，不输出 markdown/代码围栏/解释/额外文本。

### 4.6 模型调用

```go
response, err := rewriteModel.Chat(modelCtx, []chat.Message{
    {Role: "system", Content: systemContent},
    userMsg,  // 含图片时附带 Images
}, &chat.ChatOptions{
    Temperature:         0.3,   // 低温度保证确定性
    MaxCompletionTokens: maxTokens,  // 纯文本 150，含图片 500
    Thinking:            &thinking,  // false — 不启用思考模式
})
```

- `Temperature=0.3`：改写与分类需要确定性，避免模型发散。
- `MaxCompletionTokens`：纯文本 150（改写结果简短），含图片 500（图片描述需要更多 token）。
- `Thinking=false`：改写阶段不需要推理链，直接输出。
- 错误处理：模型调用失败时不阻断管线，`RewriteQuery` 保持原始 `Query`，`Intent` 保持空值（安全兜底为检索），直接 `next()`。

### 4.7 输出解析（parseOutput）

解析采用**多层容错**策略：

```
raw 模型输出
    │
    ▼
parseStructuredQueryOutput(content)
    │
    ├── 尝试 1: parseStructuredQueryOutputJSON(content)  ← 直接 JSON 解析
    │       成功 → 返回
    │
    ├── 尝试 2: 提取首个 { 到末个 } 的子串
    │       再 parseStructuredQueryOutputJSON(candidate)  ← 容忍 markdown 包裹/多余文本
    │       成功 → 返回
    │
    └── 全部失败 → 将原文当作 RewriteQuery，Intent 默认空值（→ kb_search）
```

#### 4.7.1 字段别名容错

```go
out := queryUnderstandOutput{
    RewriteQuery: firstStringField(obj,
        "rewrite_query", "rewritten_query", "query", "question"),  // 多别名
}
intentStr := firstStringField(obj, "intent")
desc := firstStringField(obj,
    "image_description", "image_desc", "image_text", "image_ocr_text", "description")
ocr := firstStringField(obj,
    "ocr_text", "ocr", "full_ocr", "image_ocr", "ocr_content")
```

模型可能输出不同的字段名，`firstStringField` 按优先级依次尝试。

#### 4.7.2 图片描述与 OCR 合并

```go
func mergeImageDescAndOCR(desc, ocr string) (string, bool) {
    if desc == "" && ocr == "" { return "", false }
    if desc == "" { return ocr, true }
    if ocr == "" { return desc, true }
    if strings.Contains(desc, ocr) { return desc, true }  // 避免重复
    return desc + "\n\n[OCR]\n" + ocr, true  // 合并
}
```

#### 4.7.3 解析失败的安全兜底

JSON 完全解析失败时：
- `RewriteQuery` = 原始模型输出文本（当作改写结果）
- `Intent` = 空字符串 → `NeedsKBRetrieval()` 返回 true → 走检索路径

这确保即使模型输出格式异常，管线仍能以"用原文检索"的方式继续运行。

### 4.8 图片描述异步回写

```go
if chatManage.ImageDescription != "" && chatManage.UserMessageID != "" {
    go p.updateUserMessageImageCaption(context.WithoutCancel(ctx), chatManage)
}
```

- 使用 `context.WithoutCancel(ctx)`：即使请求上下文被取消（用户 stop），回写仍能完成。
- 回写到 `user 消息.Images[0].Caption`，供下一轮历史加载时使用（`LOAD_HISTORY` 会把 Caption 附加到历史 user 消息）。
- 异步执行，不阻塞当前管线。

### 4.9 意图覆盖（applyIntentPromptOverride）

当意图为**非检索**意图时，设置 `SystemPromptOverride`，覆盖后续 `CHAT_COMPLETION_STREAM` 阶段的默认 system prompt：

```go
if !chatManage.NeedsRetrieval() {
    applyIntentPromptOverride(chatManage, p.config.Conversation.IntentSystemPrompts)
}
```

覆盖优先级：

```
1. Agent 级覆盖: chatManage.IntentPromptOverrides[intent]  (最高优先级)
2. 全局/租户级: config.Conversation.IntentSystemPrompts[intent]  (回退)
3. 无覆盖: SystemPromptOverride 保持空，使用默认 RAG system prompt
```

意图覆盖模板来自 `config/prompt_templates/intent_prompts.yaml`，每个模板的 `id` 与意图值一一对应：

| 意图 | 模板 id | 用途 |
|------|---------|------|
| `greeting` | greeting | 热情自然地回应问候，简要介绍能力 |
| `chitchat` | chitchat | 自然准确地闲聊，建议用户提更具体的问题 |
| `follow_up` | follow_up | 基于对话历史回答追问，不编造未出现的信息 |
| `image_only` | image_only | 基于图片内容做描述/分析/翻译/提取 |
| `doc_only` | doc_only | 基于文档内容做描述/分析/总结/翻译/提取 |
| `summarize` | summarize | 结构化总结对话历史，突出要点/结论/行动项 |
| `web_search` | web_search | 网络搜索未开启时的引导回复 |

**注意**：`kb_search`、`clarification` 不在覆盖列表中——它们需要检索，走默认 RAG system prompt。

---

## 5. 与 PluginExtractEntity 的协作

`PluginExtractEntity` 在 `QUERY_UNDERSTAND` 事件链内层执行（`PluginQueryUnderstand` 调用 `next()` 之后）：

```
PluginQueryUnderstand.OnEvent
    ├── 改写 + 意图分类 + 图片描述
    ├── applyIntentPromptOverride（非检索意图）
    └── next() → PluginExtractEntity.OnEvent
                        ├── 检查 NEO4J_ENABLE=true
                        ├── 检查检索范围内有 ExtractConfig.Enabled 的 KB
                        ├── LLM 抽取查询实体（graph_extraction.yaml 模板）
                        └── 写入 chatManage.Entity / EntityKBIDs / EntityKnowledge
```

实体抽取使用的是**原始查询** `chatManage.Query`（非改写后的 `RewriteQuery`），因为实体抽取关注的是用户原始表述中的命名实体。

---

## 6. 意图对管线走向的影响

```
                         ┌─────────────────────┐
                         │ PluginQueryUnderstand│
                         │ Intent = ?           │
                         └──────────┬──────────┘
                                    ▼
                         ┌─────────────────────┐
                         │ NeedsRetrieval()?    │
                         └──────┬──────────────┘
                           true │        false
                    ┌───────────┘         └───────────┐
                    ▼                                  ▼
        ┌──────────────────────┐         ┌─────────────────────────┐
        │ CHUNK_SEARCH_PARALLEL│         │ 跳过检索阶段             │
        │ CHUNK_RERANK         │         │ (search/rerank/merge/    │
        │ CHUNK_MERGE          │         │  filter/data_analysis)   │
        │ FILTER_TOP_K         │         │                          │
        │ (DATA_ANALYSIS)      │         │ SystemPromptOverride     │
        └──────────┬───────────┘         │ = intent_prompts[intent] │
                   ▼                      └──────────┬──────────────┘
        ┌──────────────────────┐                    ▼
        │ INTO_CHAT_MESSAGE    │         ┌─────────────────────────┐
        │ (渲染检索上下文)      │         │ INTO_CHAT_MESSAGE       │
        └──────────┬───────────┘         │ (contexts 为空，仍渲染   │
                   ▼                      │  current_time 等元数据)  │
        ┌──────────────────────┐         └──────────┬──────────────┘
        │ CHAT_COMPLETION_STREAM│                    ▼
        │ (默认 RAG system     │         ┌─────────────────────────┐
        │  prompt)             │         │ CHAT_COMPLETION_STREAM  │
        └──────────────────────┘         │ (intent-specific system │
                                         │  prompt override)       │
                                         └─────────────────────────┘
```

### 6.1 各意图的管线行为

| 意图 | NeedsRetrieval | 检索阶段 | System Prompt | 图片描述 |
|------|---------------|---------|--------------|---------|
| `kb_search` | true | 执行 | 默认 RAG prompt | 有图片时生成 |
| `clarification` | true | 执行 | 默认 RAG prompt | 有图片时生成 |
| `summarize` | true | 执行 | 默认 RAG prompt | 有图片时生成 |
| `""` (空) | true | 执行 | 默认 RAG prompt | 有图片时生成 |
| `web_search` | WebSearchEnabled | 条件执行 | web_search 覆盖 | 有图片时生成 |
| `greeting` | false | 跳过 | greeting 覆盖 | 有图片时生成 |
| `chitchat` | false | 跳过 | chitchat 覆盖 | 有图片时生成 |
| `follow_up` | false | 跳过 | follow_up 覆盖 | 有图片时生成 |
| `image_only` | false | 跳过 | image_only 覆盖 | 必须生成 |
| `doc_only` | false | 跳过 | doc_only 覆盖 | 有图片时生成 |

---

## 7. 关键设计决策

### 7.1 为什么改写和意图分类合并为一次 LLM 调用

改写和意图分类都需要理解对话上下文和用户问题语义，合并为一次调用：
- **降低延迟**：一次 LLM 调用 vs 两次。
- **一致性**：改写结果和意图判断基于同一次理解，避免两次调用产生矛盾。
- **成本优化**：可通过 `QueryUnderstandModelID` 指定轻量模型。

### 7.2 为什么空意图默认走检索路径

```go
case IntentKBSearch, IntentClarification, IntentSummarize, "":
    return true
```

模型调用失败、JSON 解析失败、模型未返回 intent 字段等异常情况下，`Intent` 为空字符串。将空值视为需要检索是**安全兜底**策略——宁可多检索（检索无结果会走 fallback 回复），也不漏检索（直接用模型通用知识回答，可能遗漏知识库中的权威信息）。

### 7.3 为什么 summarize 意图需要检索

`summarize` 意图表示用户想总结/整理**对话本身**，但对话中可能包含之前检索到的知识库内容。将 `summarize` 纳入 `NeedsKBRetrieval()=true` 是因为：
- 对话总结可能需要引用之前检索的文档片段。
- 若用户说"总结知识库中的数据"，这实际是 `kb_search` 而非 `summarize`（Prompt 中明确区分）。
- 真正的对话总结（`summarize`）走检索路径时，检索结果为空不会影响回答——`INTO_CHAT_MESSAGE` 仍会渲染 `current_time` 等元数据，模型基于历史回答。

### 7.4 为什么图片描述异步回写

- 图片描述的 DB 回写不影响当前管线结果（`ImageDescription` 已写入 `chatManage`，当前轮次直接使用）。
- 回写是为了**下一轮**历史加载时能携带图片描述。
- 使用 `context.WithoutCancel` 确保用户 stop 后回写仍能完成，避免历史数据不一致。

### 7.5 为什么用 JSON 结构化输出而非自由文本

- **可解析性**：JSON 格式使得改写结果、意图、图片描述三个字段可以一次性可靠提取。
- **容错设计**：即使模型输出带 markdown 围栏或多余文本，`parseStructuredQueryOutput` 也能通过提取 `{...}` 子串容错。
- **完全失败兜底**：JSON 完全无法解析时，将原文当作改写结果，意图默认空值（→ kb_search），保证管线不中断。

### 7.6 为什么 image_only/doc_only 要求附件存在标记

Prompt 中明确要求：
- `image_only` 必须有 `<images_uploaded>` 标记，若出现 `<no_image_attached />` 则**永远不要**分类为 `image_only`，应使用 `kb_search`。
- `doc_only` 必须有实际文档附件，若出现 `<no_document_attached />` 则**永远不要**分类为 `doc_only`。

这防止了模型在无附件时错误分类，导致跳过检索而无法回答。

---

## 8. 配置项汇总

### 8.1 PipelineRequest 中的相关配置

| 字段 | 类型 | 说明 |
|------|------|------|
| `EnableRewrite` | bool | 是否启用改写（关闭且无图片则跳过整个插件） |
| `QueryUnderstandModelID` | string | 改写阶段专用模型 ID（轻量模型），空则用 ChatModelID |
| `ChatModelID` | string | 默认对话模型 ID |
| `ChatModelSupportsVision` | bool | 对话模型是否支持视觉输入 |
| `VLMModelID` | string | 专用视觉模型 ID（ChatModel 不支持视觉时使用） |
| `RewritePromptSystem` | string | Agent 级改写 system prompt 覆盖 |
| `RewritePromptUser` | string | Agent 级改写 user prompt 覆盖 |
| `IntentPromptOverrides` | map[string]string | Agent 级意图 prompt 覆盖（key = intent 值） |
| `Language` | string | 回复语言（注入 Prompt 占位符 `{language}`） |
| `Images` | []Image | 附件图片列表 |
| `Attachments` | MessageAttachments | 文件附件 |
| `MaxRounds` | int | 多轮对话最大轮数（<=0 表示关闭多轮） |
| `WebSearchEnabled` | bool | 是否启用网络搜索（影响 web_search 意图的 NeedsRetrieval） |

### 8.2 Config 中的全局配置

| 字段 | 来源文件 | 说明 |
|------|---------|------|
| `Conversation.RewritePromptSystem` | `rewrite.yaml` (id=default_rewrite) | 默认改写 system prompt |
| `Conversation.RewritePromptUser` | `rewrite.yaml` (id=default_rewrite) | 默认改写 user prompt |
| `Conversation.IntentSystemPrompts` | `intent_prompts.yaml` | 意图→system prompt 映射 |

### 8.3 环境变量

| 变量 | 影响 |
|------|------|
| `NEO4J_ENABLE` | `true` 时启用 `PluginExtractEntity`（图谱实体抽取） |

---

## 9. 错误处理与降级

| 场景 | 行为 |
|------|------|
| 改写关闭且无图片 | 跳过插件，`RewriteQuery=Query`，`Intent=""`（→ 检索） |
| 模型获取失败 | 跳过插件，`RewriteQuery=Query`，`Intent=""`（→ 检索） |
| 模型调用失败 | 跳过插件，`RewriteQuery=Query`，`Intent=""`（→ 检索） |
| JSON 解析失败 | `RewriteQuery=原文`，`Intent=""`（→ 检索） |
| JSON 部分字段缺失 | 已解析的字段生效，缺失字段用默认值 |
| 图片描述回写失败 | 仅日志告警，不影响当前管线 |
| `QueryUnderstandModelID` 不可用 | 回退到 `ChatModelID` |
| 视觉模型不可用 | 回退到文本模型（图片描述为空） |

**核心原则**：任何失败都不阻断管线，降级为"用原始查询走检索路径"。

---

## 10. 模板加载与应用机制

### 10.1 模板文件目录结构

所有 Prompt 模板以 YAML 文件形式存放在 `config/prompt_templates/` 目录下，每个文件对应 `PromptTemplatesConfig` 的一个字段：

| YAML 文件 | 配置字段 | 用途 |
|-----------|---------|------|
| `rewrite.yaml` | `Rewrite` | 改写 + 意图分类 Prompt（含 system + user） |
| `intent_prompts.yaml` | `IntentPrompts` | 非检索意图的 system prompt 覆盖 |
| `system_prompt.yaml` | `SystemPrompt` | 默认 RAG system prompt |
| `context_template.yaml` | `ContextTemplate` | 检索上下文渲染模板 |
| `fallback.yaml` | `Fallback` | 兜底回复模板 |
| `generate_session_title.yaml` | `GenerateSessionTitle` | 会话标题生成 |
| `generate_summary.yaml` | `GenerateSummary` | 摘要生成 |
| `keywords_extraction.yaml` | `KeywordsExtraction` | 关键词抽取 |
| `agent_system_prompt.yaml` | `AgentSystemPrompt` | Agent 模式 system prompt |
| `graph_extraction.yaml` | `GraphExtraction` | 图谱实体抽取 |
| `generate_questions.yaml` | `GenerateQuestions` | 问题生成 |

### 10.2 模板文件格式

每个 YAML 文件统一使用 `templates` 作为顶层 key，每个模板是一个 `PromptTemplate` 结构：

```yaml
templates:
  - id: default_rewrite          # 唯一标识，用于配置引用
    name: "默认改写模板"          # 显示名称
    description: "..."           # 描述
    content: |                   # system prompt 内容
      You are a query understanding assistant...
    user: |                      # user prompt 内容
      Based on the conversation history...
    default: true                # 是否为默认模板
    has_knowledge_base: false    # 是否需要知识库
    has_web_search: false        # 是否需要网络搜索
    mode: ""                     # 模式标记（如 "model" 区分兜底类型）
    i18n:                        # 国际化
      zh-CN:
        name: "默认改写模板"
        description: "..."
```

### 10.3 加载流程

加载发生在 `config.LoadConfig()` 中，分三步：

```
LoadConfig()
    │
    ├─ 1. viper.Unmarshal → 解析 config.yaml 到 Config 结构体
    │     （PromptTemplates 字段此时为空，因为模板在独立文件中）
    │
    ├─ 2. loadPromptTemplates(configDir)
    │     │
    │     ├─ 定位目录: <configDir>/prompt_templates/
    │     ├─ 目录不存在 → 返回 nil（回退到 config.yaml 内嵌模板）
    │     ├─ 遍历 11 个已知文件名（硬编码映射）
    │     │   ├─ 文件不存在 → 跳过（continue）
    │     │   ├─ os.ReadFile → 读取文件内容
    │     │   └─ yaml.Unmarshal → 解析到 promptTemplateFile{Templates}
    │     └─ 返回填充后的 PromptTemplatesConfig
    │
    ├─ 3. cfg.PromptTemplates = promptTemplates
    │
    └─ 4. backfillConversationDefaults(&cfg)
          │
          ├─ 遍历 Conversation 中的 *_prompt_id 字段
          │   ├─ FindTemplateByID(pt, id) → 跨所有模板列表搜索
          │   └─ 找到 → 将 Content/User 回填到 Conversation.*Prompt 字段
          │
          └─ 构建 IntentSystemPrompts map:
              遍历 pt.IntentPrompts → conv.IntentSystemPrompts[t.ID] = t.Content
```

源码位置：
- `loadPromptTemplates`: `internal/config/config.go:1052`
- `backfillConversationDefaults`: `internal/config/config.go:923`
- `FindTemplateByID`: `internal/config/config.go:1008`

### 10.4 ID 引用机制

`config.yaml` 中的 `Conversation` 配置不直接写 Prompt 文本，而是通过 **ID 引用** 指向模板文件中的模板：

```yaml
conversation:
  rewrite_prompt_id: "default_rewrite"        # → rewrite.yaml 中 id=default_rewrite 的模板
  fallback_prompt_id: "default_fallback"      # → fallback.yaml 中 id=default_fallback 的模板
  generate_session_title_prompt_id: "..."
  generate_summary_prompt_id: "..."
  extract_entities_prompt_id: "..."
  extract_relationships_prompt_id: "..."
  generate_questions_prompt_id: "..."
```

`backfillConversationDefaults` 通过 `FindTemplateByID` 跨所有模板列表搜索匹配 ID，将模板的 `Content`（和 `User`）回填到 `Conversation` 结构体的对应字段：

| config.yaml 字段 | 回填目标字段 | 回填内容 |
|-----------------|------------|---------|
| `rewrite_prompt_id` | `RewritePromptSystem` + `RewritePromptUser` | `t.Content` + `t.User` |
| `fallback_prompt_id` | `FallbackPrompt` | `t.Content` |
| `generate_session_title_prompt_id` | `GenerateSessionTitlePrompt` | `t.Content` |
| `generate_summary_prompt_id` | `GenerateSummaryPrompt` | `t.Content` |
| `extract_entities_prompt_id` | `ExtractEntitiesPrompt` | `t.Content` |
| `extract_relationships_prompt_id` | `ExtractRelationshipsPrompt` | `t.Content` |
| `generate_questions_prompt_id` | `GenerateQuestionsPrompt` | `t.Content` |
| `summary.prompt_id` | `Summary.Prompt` | `t.Content` |
| `summary.context_template_id` | `Summary.ContextTemplate` | `t.Content` |

**意图覆盖模板**（`intent_prompts.yaml`）不走 ID 引用，而是直接构建为 map：

```go
conv.IntentSystemPrompts = make(map[string]string, len(pt.IntentPrompts))
for _, t := range pt.IntentPrompts {
    if t.ID != "" && t.Content != "" {
        conv.IntentSystemPrompts[t.ID] = t.Content  // key = 模板 ID = 意图值
    }
}
```

模板的 `id` 必须与 `QueryIntent` 枚举值完全一致（如 `greeting`、`chitchat`、`follow_up` 等）。

### 10.5 占位符渲染机制

模板中的 `{{key}}` 占位符通过 `types.RenderPromptPlaceholders` 统一渲染。

#### 10.5.1 渲染函数

```go
// internal/types/placeholder.go:200
func RenderPromptPlaceholders(template string, vals PlaceholderValues) string
```

- 遍历 `vals` map，将 `{{key}}` 替换为对应值
- **未知的占位符保持原样**（不报错，不删除）
- **自动填充**：当模板包含 `{{current_time}}`/`{{current_week}}`/`{{yesterday}}` 但调用方未提供时，自动生成：
  - `{{current_time}}` → `time.Now().Format("2006-01-02 15:04:05")`
  - `{{current_week}}` → `now.Weekday().String()`
  - `{{yesterday}}` → `now.AddDate(0, 0, -1).Format("2006-01-02")`

#### 10.5.2 改写阶段的占位符

`buildPrompts` 中注入的占位符：

```go
vals := types.PlaceholderValues{
    "conversation": conversationText,   // 格式化后的对话历史
    "query":        queryContent,        // 当前查询 + 图片/附件标记
    "language":     chatManage.Language, // 回复语言
}
// current_time / yesterday 由 RenderPromptPlaceholders 自动填充
```

支持的占位符（`PromptFieldRewriteSystemPrompt` / `PromptFieldRewritePrompt`）：

| 占位符 | 说明 | 来源 |
|--------|------|------|
| `{{query}}` | 用户当前问题 + 附件元信息标记 | `buildPrompts` 注入 |
| `{{conversation}}` | 格式化的历史对话 | `buildPrompts` 注入 |
| `{{language}}` | 用户界面语言偏好 | `buildPrompts` 注入 |
| `{{current_time}}` | 当前系统时间 | 自动填充 |
| `{{yesterday}}` | 昨天日期 | 自动填充 |

#### 10.5.3 意图覆盖阶段的占位符

`SystemPromptOverride` 在后续 `prepareMessagesWithHistory`（`common.go:87`）中被渲染：

```go
base := chatManage.SummaryConfig.Prompt
if chatManage.SystemPromptOverride != "" {
    base = chatManage.SystemPromptOverride   // 意图覆盖优先
}
systemPrompt := types.RenderPromptPlaceholders(base, types.PlaceholderValues{
    "query":    chatManage.Query,
    "language": chatManage.Language,
    "contexts": chatManage.RenderedContexts,
})
```

意图覆盖模板可使用的占位符（`PromptFieldSystemPrompt`）：

| 占位符 | 说明 |
|--------|------|
| `{{query}}` | 用户问题 |
| `{{contexts}}` | 检索到的上下文（非检索意图时为空） |
| `{{current_time}}` | 当前时间（自动填充） |
| `{{current_week}}` | 当前星期（自动填充） |
| `{{language}}` | 用户语言 |

### 10.6 模板优先级与覆盖链

运行时模板选择遵循 **Agent 级 > 全局默认** 的优先级：

```
┌─────────────────────────────────────────────────────────┐
│ buildPrompts (改写阶段)                                  │
│                                                         │
│  System Prompt:                                         │
│    chatManage.RewritePromptSystem  (Agent 级覆盖)        │
│    ↓ 为空时                                              │
│    config.Conversation.RewritePromptSystem (全局默认)    │
│    ↓ 来源                                                │
│    rewrite.yaml id=default_rewrite 的 content            │
│                                                         │
│  User Prompt:                                           │
│    chatManage.RewritePromptUser    (Agent 级覆盖)        │
│    ↓ 为空时                                              │
│    config.Conversation.RewritePromptUser (全局默认)      │
│    ↓ 来源                                                │
│    rewrite.yaml id=default_rewrite 的 user               │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ applyIntentPromptOverride (意图覆盖阶段)                 │
│                                                         │
│  chatManage.IntentPromptOverrides[intent]  (Agent 级)    │
│    ↓ 为空或纯空白时                                      │
│  config.Conversation.IntentSystemPrompts[intent] (全局)  │
│    ↓ 来源                                                │
│  intent_prompts.yaml 中 id=<intent> 的 content           │
│    ↓ 为空时                                              │
│  SystemPromptOverride 保持空 → 使用默认 RAG system prompt│
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ prepareMessagesWithHistory (Chat 阶段)                   │
│                                                         │
│  chatManage.SystemPromptOverride (意图覆盖)              │
│    ↓ 为空时                                              │
│  chatManage.SummaryConfig.Prompt (默认 RAG prompt)       │
│    ↓ 来源                                                │
│  system_prompt.yaml 中 id=<summary_prompt_id> 的 content │
└─────────────────────────────────────────────────────────┘
```

### 10.7 默认模板选择函数

```go
// DefaultTemplate: 返回标记为 default 的模板，无标记则返回第一个
func DefaultTemplate(templates []PromptTemplate) *PromptTemplate

// DefaultTemplateByMode: 按 mode 过滤后再选默认
func DefaultTemplateByMode(templates []PromptTemplate, mode string) *PromptTemplate
```

这两个函数用于前端模板选择场景。`backfillConversationDefaults` 不使用它们——它严格通过 `FindTemplateByID` 按 ID 精确匹配，确保配置中指定的 `*_prompt_id` 一定指向对应模板（而非"碰巧是默认的那个"）。

### 10.8 国际化（i18n）

模板支持 `i18n` 字段，用于本地化模板的 `name` 和 `description`（仅影响前端展示，不影响 `content`/`user` 的实际 Prompt 文本）：

```go
// LocalizeTemplates: 按 locale 深拷贝并替换 Name/Description
// 回退链: locale(如 "zh-CN") → 主语言(如 "zh") → 原始值
func LocalizeTemplates(templates []PromptTemplate, locale string) []PromptTemplate
```

Prompt 内容本身的多语言通过 `{{language}}` 占位符在运行时注入，而非通过 i18n 模板切换。

---

## 11. 源码文件索引

| 文件 | 职责 |
|------|------|
| `internal/application/service/chat_pipeline/query_understand.go` | PluginQueryUnderstand 主逻辑 |
| `internal/application/service/chat_pipeline/extract_entity.go` | PluginExtractEntity 实体抽取 |
| `internal/application/service/chat_pipeline/common.go` | `loadAndProcessHistory` 历史加载共享函数 |
| `internal/types/chat_manage.go` | `QueryIntent` 枚举、`NeedsRetrieval()`、`ChatManage` 结构 |
| `internal/container/container.go` | 插件注册（DI 容器） |
| `config/prompt_templates/rewrite.yaml` | 改写+意图分类 Prompt 模板 |
| `config/prompt_templates/intent_prompts.yaml` | 非检索意图的 system prompt 覆盖模板 |
| `internal/config/config.go` | Prompt 模板加载器 |
