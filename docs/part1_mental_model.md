# Claude Managed Agents — Zero to Hero Tutorial
## Part 1: The Mental Model (Read This First)

> **Series overview**
> - Part 1 — Mental model, architecture, core concepts ← you are here
> - Part 2 — Setup, first agent, dissecting the code
> - Part 3 — The agent loop deep dive
> - Part 4 — Tools, permissions, and control
> - Part 5 — Capstone project: AI Research Digest Agent

---

## Why This Exists

Until April 8, 2026, if you wanted a Claude agent that:
- ran for more than one turn,
- used real tools (bash, file I/O, web search),
- recovered from errors,
- managed its own context window,
- and stayed stateful across interactions…

…you had to **build all of that plumbing yourself**. Tool dispatch loops, context compaction, retry logic, sandboxed execution, session persistence. Weeks of work before writing a single line of actual agent logic.

**Claude Managed Agents eliminates that entire layer.** You define what the agent does. Anthropic runs the infrastructure.

This is the AWS RDS moment for AI agents: you no longer manage the database server, you just use the database.

---

## Two Things Anthropic Released Today (Don't Confuse Them)

### 1. Claude Managed Agents (cloud-hosted)
The REST API product. You create Agents, Environments, and Sessions via HTTP calls. Anthropic's cloud runs the containers. You just stream results. Best for production, multi-tenant, or long-running workloads.

**Access:** `platform.claude.com` → API key → beta header `managed-agents-2026-04-01`

### 2. Agent SDK (`claude-agent-sdk` Python/TypeScript package)
A local SDK that gives you **programmatic control** over an agent loop, running from your own machine or container. Backed by the same engine as Claude Code. This is what you write Python code against.

**Access:** `pip install claude-agent-sdk`

**The relationship:** The Agent SDK is the primary developer interface. It can target Anthropic's managed cloud, Amazon Bedrock, Google Vertex AI, or Microsoft Azure. For this tutorial we use the Agent SDK targeting the Anthropic API directly — the fastest path to running code.

---

## The Four Core Concepts

These four concepts are everything. Internalize them and the rest follows.

```
┌─────────────────────────────────────────────────────┐
│                      AGENT                          │
│  model + system_prompt + tools + MCP + skills       │
│  (defined once, reused across many sessions)        │
└─────────────────────────────┬───────────────────────┘
                              │ references
┌─────────────────────────────▼───────────────────────┐
│                   ENVIRONMENT                        │
│  cloud container: packages, network rules, files     │
│  (Python, Node.js, Go pre-installed)                │
└─────────────────────────────┬───────────────────────┘
                              │ runs inside
┌─────────────────────────────▼───────────────────────┐
│                     SESSION                          │
│  one running instance performing one specific task   │
│  stateful: files persist, history persists           │
│  can be paused, resumed, forked                      │
└─────────────────────────────┬───────────────────────┘
                              │ communicates via
┌─────────────────────────────▼───────────────────────┐
│                     EVENTS                           │
│  user messages → agent                               │
│  tool results, status updates → your app             │
│  streamed as Server-Sent Events (SSE)                │
└─────────────────────────────────────────────────────┘
```

### Agent
The blueprint. It specifies:
- Which Claude model to use (Opus 4.6, Sonnet 4.6, Haiku 4.5)
- The system prompt (agent's identity and instructions)
- Which tools are available (Bash, Read, WebSearch, MCP servers…)
- Any custom Skills

Create an Agent **once**. Reference it by ID in every session. You don't recreate the Agent for each task — you just spin up new Sessions against it.

### Environment
The compute substrate. A cloud container pre-loaded with:
- Python, Node.js, Go, and other runtimes
- Network access rules (what external URLs can the agent call?)
- Mounted files (seed data, config, codebases)

Think of the Environment as the "server" the agent runs on. For the Agent SDK path we're using in this tutorial, the environment is Anthropic-managed automatically — you don't configure it manually as a beginner.

### Session
One running agent instance. A Session:
- References an Agent definition
- Runs inside an Environment
- Maintains **persistent state**: files written by the agent survive across turns
- Tracks full conversation history server-side
- Can be interrupted, steered mid-run, paused, and resumed by session ID

Sessions are the unit of work. "Fix the auth bug" is a session. "Research quantum computing papers" is a session. Each gets its own isolated context.

### Events
The communication protocol between your application and the running agent. You send **user events** (messages, instructions). The agent streams back **response events** (text, tool calls, tool results, status). All persisted server-side — you can fetch the full event history at any time.

---

## Messages API vs. Managed Agents — When to Use Which

| Criterion | Messages API | Claude Managed Agents |
|---|---|---|
| Task duration | Seconds to ~1 minute | Minutes to hours |
| Tool execution | You build the loop | Anthropic runs it |
| Infrastructure | You manage | Zero-ops |
| Context management | You handle compaction | Automatic |
| State/persistence | You implement | Built-in |
| Fine-grained control | Maximum | High (via hooks, permissions) |
| Best for | Simple completions, chatbots, RAG | Autonomous agents, agentic workflows |

**Decision rule:** If you're writing a while loop that calls the Messages API until `stop_reason == "end_turn"`, you should be using Managed Agents instead.

---

## The Supported Tool Arsenal

Out of the box, every Managed Agent can use:

| Category | Tools | What it does |
|---|---|---|
| File ops | `Read`, `Edit`, `Write` | Read, modify, create files |
| Search | `Glob`, `Grep` | Find files by pattern, regex search |
| Execution | `Bash` | Shell commands, scripts, git |
| Web | `WebSearch`, `WebFetch` | Search + fetch/parse web pages |
| Discovery | `ToolSearch` | Load tools on-demand from large catalogs |
| Orchestration | `Task`, `Skill`, `AskUserQuestion`, `TodoWrite` | Spawn subagents, invoke skills, track tasks |

Plus **MCP servers** for anything external: databases, browsers, Slack, GitHub, etc.

---

## Pricing Reality Check

No subscription premium. Pure token pricing billed to your API account:

| Model | Input | Output | Best for |
|---|---|---|---|
| Claude Haiku 4.5 | $1/MTok | $5/MTok | High-volume, cost-sensitive |
| Claude Sonnet 4.6 | $3/MTok | $15/MTok | General agents (recommended default) |
| Claude Opus 4.6 | $5/MTok | $25/MTok | Complex reasoning tasks |

**Prompt caching is your best friend for agents.** System prompts and tool definitions are re-sent every turn. With caching enabled (automatic in the SDK), repeated prefixes cost **0.1x** standard input price after the first hit. A 10,000-token system prompt cached costs $0.05 per million cache reads instead of $5.

**Web search:** $10 per 1,000 searches. Plan accordingly.

**Code execution:** Free when used with web search or web fetch. 1,550 free hours/org/month otherwise.

---

## What You'll Build in This Tutorial Series

**Capstone: AI Research Digest Agent**

An agent that, given a research topic:
1. Searches arXiv and the web for the last 30 days of papers/posts
2. Fetches and reads the most relevant ones
3. Extracts key findings from each
4. Synthesizes a structured digest with citations
5. Writes the output to a markdown file

This is a real, useful agent. It demonstrates: web search, web fetch, file writing, multi-step reasoning, and structured output — all the core primitives.

---

## Before You Continue

Make sure you have:
- [ ] An Anthropic account at `platform.claude.com`
- [ ] An API key from `platform.claude.com/settings/keys`
- [ ] Python 3.10+ installed
- [ ] Basic comfort with `async/await` in Python

You do **not** need Claude Code installed. You do **not** need Docker. You do **not** need a Claude.ai subscription.

---

**→ Continue to Part 2: Setup and Your First Agent**
