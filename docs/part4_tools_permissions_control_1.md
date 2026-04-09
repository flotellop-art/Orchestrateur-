# Claude Managed Agents — Zero to Hero Tutorial
## Part 4: Tools, Permissions, and Control

> **Series overview**
> - Part 1 — Mental model, architecture, core concepts
> - Part 2 — Setup, first agent, dissecting the code
> - Part 3 — The agent loop deep dive
> - Part 4 — Tools, permissions, and control ← you are here
> - Part 5 — Capstone project: AI Research Digest Agent

---

## The Tool Arsenal

Tools are what transform Claude from a text generator into an agent that *does things*. Without tools, Claude can only respond with text. With tools, it can read your codebase, run tests, search the web, and write files — autonomously, across as many turns as needed.

### Built-in tools reference

```
CATEGORY: FILE OPERATIONS
  Read       - Read file contents
  Edit       - Modify a file (patch-based — efficient, not full rewrites)
  Write      - Create or overwrite a file

CATEGORY: SEARCH & DISCOVERY  
  Glob       - Find files by pattern (e.g., "**/*.py", "src/**/*.ts")
  Grep       - Search file contents with regex
  ToolSearch - Dynamically load tools from large catalogs on demand

CATEGORY: EXECUTION
  Bash       - Run shell commands, scripts, git operations, tests

CATEGORY: WEB
  WebSearch  - Search the web (returns structured results)
  WebFetch   - Fetch and parse a specific URL

CATEGORY: ORCHESTRATION
  Task          - Spawn a subagent to handle a subtask
  Skill         - Invoke a pre-defined Skill bundle
  AskUserQuestion - Pause and ask the user for input
  TodoWrite     - Maintain a structured task list
```

### Enabling tools

You control which tools Claude can use via `allowed_tools`:

```python
# Read-only agent (safe for untrusted environments)
ClaudeAgentOptions(
    allowed_tools=["Read", "Glob", "Grep"],
    model="claude-haiku-4-5",   # $1/$5 per MTok
)

# Code modification agent
ClaudeAgentOptions(
    allowed_tools=["Read", "Edit", "Glob", "Grep"],
    model="claude-haiku-4-5",   # $1/$5 per MTok
)

# Full local automation
ClaudeAgentOptions(
    allowed_tools=["Read", "Edit", "Write", "Glob", "Grep", "Bash"],
    model="claude-haiku-4-5",   # $1/$5 per MTok
)

# Web research agent
ClaudeAgentOptions(
    allowed_tools=["WebSearch", "WebFetch", "Write"],
    model="claude-haiku-4-5",   # $1/$5 per MTok — upgrade to Sonnet if synthesis is weak
)

# Full-stack agent
ClaudeAgentOptions(
    allowed_tools=[
        "Read", "Edit", "Write", "Glob", "Grep", "Bash",
        "WebSearch", "WebFetch"
    ],
    model="claude-haiku-4-5",   # $1/$5 per MTok
)
```

Tools not in `allowed_tools` are not automatically blocked — they're just not pre-approved. What happens to them depends on `permission_mode` (see below).

To **explicitly block** a tool regardless of other settings:

```python
ClaudeAgentOptions(
    allowed_tools=["Read", "Edit", "Glob", "Bash"],
    disallowed_tools=["Bash"],  # Bash is in allowed but overridden here — blocked
    model="claude-haiku-4-5",   # $1/$5 per MTok
)
```

`disallowed_tools` always wins. Even if Bash is in `allowed_tools`, listing it in `disallowed_tools` blocks it.

---

## Permission Modes — The Safety Dial

`permission_mode` controls what happens when Claude wants to use a tool that isn't explicitly pre-approved or blocked:

```python
ClaudeAgentOptions(
    model="claude-haiku-4-5",   # $1/$5 per MTok
    permission_mode="acceptEdits",
)
```

| Mode | Behavior | Use when |
|---|---|---|
| `"default"` | Claude prompts for approval via your callback; no callback = deny | Interactive apps, human-in-the-loop workflows |
| `"acceptEdits"` | File edits auto-approved; other tools follow default rules | Development automation on your machine |
| `"plan"` | No tools execute; Claude produces a plan only | Pre-flight review before execution |
| `"dontAsk"` | Tools in `allowed_tools` run; everything else denied silently | Locked-down headless agents |
| `"bypassPermissions"` | All tools run without any prompting | Fully sandboxed CI, isolated containers |

### Choosing the right mode

**For learning/development on your own machine:** use `"acceptEdits"`. File edits are auto-approved (the most common operation), and Bash will prompt you for approval (appropriate since bash can do anything).

**For production API agents:** use `"dontAsk"` with an explicit `allowed_tools` list. No surprises — only what you listed runs.

**For CI/CD pipelines:** use `"bypassPermissions"` inside an isolated Docker container. Never use this on a non-sandboxed machine.

**For demos or interactive apps:** use `"default"` with a `canUseTool` callback that shows the user what the agent wants to do.

```python
# Example: default mode with approval callback
from claude_agent_sdk import ClaudeAgentOptions

async def my_approval_callback(tool_name: str, tool_input: dict) -> bool:
    """Return True to approve, False to deny."""
    print(f"\nAgent wants to run: {tool_name}")
    print(f"Input: {tool_input}")
    response = input("Approve? (y/n): ").strip().lower()
    return response == "y"

options = ClaudeAgentOptions(
    allowed_tools=["Read", "Glob"],
    model="claude-haiku-4-5",   # $1/$5 per MTok
    permission_mode="default",
    can_use_tool=my_approval_callback,
)
```

---

## Scoped Tool Permissions — Fine-Grained Control

Beyond binary allow/deny, you can scope individual tools to specific operations:

```python
# Allow Bash, but only for npm and git commands
ClaudeAgentOptions(
    allowed_tools=["Read", "Edit", "Bash(npm:*)", "Bash(git:*)"],
    model="claude-haiku-4-5",   # $1/$5 per MTok
    permission_mode="dontAsk",
)

# Allow WebFetch, but only your own domain
ClaudeAgentOptions(
    allowed_tools=["Read", "WebFetch(https://api.yourdomain.com:*)"],
    model="claude-haiku-4-5",   # $1/$5 per MTok
    permission_mode="dontAsk",
)
```

Rule syntax: `"ToolName(pattern)"`. Check the [Permissions doc](https://platform.claude.com/docs/en/agent-sdk/permissions) for the full syntax — this is critical for production security.

---

## Tool-by-Tool Guide

### Bash — the power tool

Bash is the most powerful and the most dangerous. It can run anything the host process can run.

```python
# WRONG: give Bash unrestricted in production
ClaudeAgentOptions(
    allowed_tools=["Bash"],
    model="claude-haiku-4-5",
)

# RIGHT: scope what Bash can run
ClaudeAgentOptions(
    allowed_tools=[
        "Bash(npm:*)",
        "Bash(python:*)",
        "Bash(pytest:*)",
        "Bash(git log:*)",
        "Bash(git status:*)",
        "Bash(git diff:*)",
    ],
    model="claude-haiku-4-5",   # $1/$5 per MTok
)
```

Bash adds **245 input tokens** to every API call. For very high-volume agents, prefer `Read`/`Edit`/`Glob` over equivalent Bash commands when possible.

### WebSearch — the knowledge tool

```python
ClaudeAgentOptions(
    allowed_tools=["WebSearch", "WebFetch"],
    model="claude-haiku-4-5",   # $1/$5 per MTok
)
```

Billing: **$10 per 1,000 searches** plus token costs for results.

Best practices:
- Set `max_budget_usd` when using WebSearch — it's easy to rack up costs with many searches
- Combine WebSearch (discover URLs) + WebFetch (read full content) for research tasks
- WebFetch is **free** (tokens only) — use it liberally

Typical costs:
- 10 web searches + 5 fetches for a research task: ~$0.10–0.25 total
- 100 web searches in a loop: ~$1.00 just in search costs

### Read / Edit / Write

```
Read:  Low token cost. Reads file content into context.
       A 500-line file ≈ 3,000-5,000 input tokens.

Edit:  Patch-based. Only sends the diff, not the whole file.
       Much more token-efficient than Write for modifications.

Write: Overwrites entire file. Use for creating new files.
       For existing files, prefer Edit.
```

### Glob and Grep

```python
# Glob: find files by path pattern
# Claude will call: Glob("**/*.py") → [list of paths]

# Grep: search file contents
# Claude will call: Grep("import numpy", "*.py") → [matches]
```

These are read-only and can run in parallel. Very cheap. Always include them in coding agents.

---

## The Python SDK API — Key Options Reference

This is the complete `ClaudeAgentOptions` you'll use most:

```python
from claude_agent_sdk import ClaudeAgentOptions

options = ClaudeAgentOptions(
    # ── TOOLS ─────────────────────────────────────────
    # Tools Claude is pre-approved to use
    allowed_tools=["Read", "Edit", "Glob", "Bash", "WebSearch", "WebFetch"],
    # Tools Claude cannot use, regardless of other settings
    disallowed_tools=[],

    # ── PERMISSIONS ───────────────────────────────────
    # "default" | "acceptEdits" | "plan" | "dontAsk" | "bypassPermissions"
    permission_mode="acceptEdits",

    # ── MODEL ─────────────────────────────────────────
    # Always set this explicitly — never rely on SDK default.
    model="claude-haiku-4-5",   # DEFAULT in this tutorial — $1/$5 per MTok in/out
    # model="claude-sonnet-4-6", # $3/$15 per MTok — better reasoning & synthesis
    # model="claude-opus-4-6",   # $5/$25 per MTok — best reasoning, use sparingly

    # ── REASONING ─────────────────────────────────────
    # "low" | "medium" | "high" | "max"
    effort="high",

    # ── LIMITS ────────────────────────────────────────
    max_turns=20,           # max tool-use turns
    max_budget_usd=0.50,    # max total cost

    # ── INSTRUCTIONS ──────────────────────────────────
    system_prompt="You are a...",  # appends to default system prompt

    # ── ENVIRONMENT ───────────────────────────────────
    cwd="/path/to/work/dir",  # agent's working directory

    # ── SESSIONS ──────────────────────────────────────
    # session_id="..."  # to resume a previous session (coming soon)
)
```

---

## Common Agent Archetypes with Full Config

### 1. Safe Code Reviewer (read-only)

```python
reviewer_opts = ClaudeAgentOptions(
    allowed_tools=["Read", "Glob", "Grep"],
    permission_mode="dontAsk",
    model="claude-haiku-4-5",   # $1/$5 per MTok — fine for read-only analysis
    effort="high",
    max_turns=15,
    max_budget_usd=0.10,
    system_prompt=(
        "You are an expert code reviewer. "
        "Identify bugs, security issues, and style violations. "
        "Never modify files. Produce a structured report."
    ),
)
```

### 2. Automated Refactoring Agent

```python
refactor_opts = ClaudeAgentOptions(
    allowed_tools=["Read", "Edit", "Glob", "Grep", "Bash(pytest:*)"],
    permission_mode="acceptEdits",
    model="claude-sonnet-4-6",  # complex multi-file reasoning → Sonnet minimum
    # model="claude-opus-4-6",  # upgrade to Opus for very large codebases
    effort="max",
    max_turns=40,
    max_budget_usd=2.00,
    system_prompt=(
        "You are a senior engineer. "
        "Run tests before and after every change. "
        "Only commit changes when all tests pass. "
        "Add type hints to every function you modify."
    ),
)
```

### 3. Web Research Agent

```python
research_opts = ClaudeAgentOptions(
    allowed_tools=["WebSearch", "WebFetch", "Write"],
    permission_mode="dontAsk",
    model="claude-haiku-4-5",   # $1/$5 per MTok — upgrade to Sonnet if synthesis quality is poor
    # model="claude-sonnet-4-6", # $3/$15 per MTok — better cross-source synthesis
    effort="high",
    max_turns=30,
    max_budget_usd=0.75,    # web searches add up — watch this
    system_prompt=(
        "You are a research analyst. "
        "Always verify claims across multiple sources. "
        "Cite every source with URL and access date. "
        "Produce markdown output."
    ),
)
```

### 4. DevOps / CI Agent

```python
devops_opts = ClaudeAgentOptions(
    allowed_tools=[
        "Read", "Edit", "Write", "Glob", "Grep",
        "Bash(docker:*)", "Bash(kubectl:*)", "Bash(git:*)",
        "Bash(npm:*)", "Bash(pip:*)",
    ],
    permission_mode="bypassPermissions",   # fully sandboxed CI container
    model="claude-haiku-4-5",   # $1/$5 per MTok — CI tasks are well-scoped
    # model="claude-sonnet-4-6", # upgrade if agent needs to reason about complex failures
    effort="high",
    max_turns=50,
    max_budget_usd=3.00,
)
```

---

## Cost Management in Practice

### Estimating costs before you run

Back-of-envelope for a research agent using **Haiku 4.5**:
```
10 web searches:           $0.10
Fetching 5 pages:          ~5 × 2,500 tokens = 12,500 input tokens
                           = 12,500 × $1/1M = $0.0125
20 turns × ~1,000 tok/turn = 20,000 output tokens
                           = 20,000 × $5/1M = $0.10
System prompt (cached):    ~$0.001
──────────────────────────────────
Estimated total (Haiku):   ~$0.21

Same task with Sonnet 4.6: ~$0.45  (3x more)
Same task with Opus 4.6:   ~$0.75  (5x more)
```

Set `max_budget_usd=0.30` for a Haiku research agent (40% buffer).

### Tracking actual costs

```python
from claude_agent_sdk import ResultMessage

async def run_and_report_cost(prompt: str):
    total_cost = 0.0

    async for message in query(prompt=prompt, options=...):
        if isinstance(message, ResultMessage):
            total_cost = message.total_cost_usd or 0.0
            print(f"\nCost breakdown:")
            print(f"  Total: ${total_cost:.6f}")
            if message.usage:
                u = message.usage
                print(f"  Input tokens:       {u.input_tokens:,}")
                print(f"  Output tokens:      {u.output_tokens:,}")
                print(f"  Cache reads:        {u.cache_read_input_tokens:,}")
                print(f"  Cache writes:       {u.cache_creation_input_tokens:,}")

    return total_cost
```

### Reducing costs

1. **Use prompt caching** — automatic in the SDK. System prompts and tool definitions are cached after the first turn. A 5,000-token system prompt cached costs $0.0005/MTok on hits vs $3/MTok without.

2. **Use Haiku for simple tasks** — Haiku 4.5 at $1/$5 MTok is 3x cheaper than Sonnet for agents that don't need deep reasoning (file listing, format conversion, simple grep).

3. **Scope tool lists tightly** — each extra tool definition adds tokens to every request.

4. **Use Edit not Write** for file modifications — patch-based edits use far fewer tokens than sending the whole file.

5. **Set max_turns conservatively** — a runaway agent doing 100 turns of web searching can cost $10+.

---

## Cost Guardrails — Safe Experimentation Protocol

Experimentation hesitation is rational but the actual numbers don't justify it — **if you follow this protocol**.

### The One Rule That Makes Learning Safe

Put this in every agent while you are learning:

```python
ClaudeAgentOptions(
    model="claude-haiku-4-5",
    max_budget_usd=0.05,   # hard stop at 5 cents
    max_turns=5,            # hard stop at 5 turns
)
```

Five cents. Five turns. The agent physically cannot cost more. When the limit hits you get a clean `ResultMessage(subtype="error_max_budget_usd")` — no surprise charges, no runaway execution. Once you understand what an agent does, bump the limits deliberately.

### What Things Actually Cost on Haiku

```
Hello-world agent (5 turns, file listing):
  ~500 input + ~200 output tokens
  = $0.0005 + $0.0001 = $0.0006   ← less than 1/10th of a cent

Bug-fixer agent (10 turns, reads 3 files, edits 2):
  ~5,000 input + ~1,000 output tokens
  = $0.005 + $0.005 = $0.01       ← one cent

Full research digest (30 turns, 10 searches, 6 web fetches):
  = ~$0.25 worst case

100 hello-world experiments: under $1 total
A full week of learning: under $5 total
```

### The Two Real Cost Risks

These are the only ways to get an unexpectedly large bill:

**1. WebSearch in a loop without a budget cap**
Each search costs $0.01 (at $10/1,000 searches). An agent that searches 100 times = $1 just in search fees, before any token costs. Always pair `WebSearch` with `max_budget_usd`.

**2. Unbounded agents on large codebases**
An agent without `max_turns` reading a large repo can run 50+ turns reading hundreds of files. Fix: set both limits together.

```python
# ALWAYS use both guards together when using WebSearch or large codebases
ClaudeAgentOptions(
    allowed_tools=["WebSearch", "WebFetch", "Write"],
    model="claude-haiku-4-5",
    max_budget_usd=0.25,   # dollar ceiling
    max_turns=20,           # turn ceiling — whichever hits first wins
)
```

### Suggested Learning Progression

Follow this week-by-week to keep total spend under $10 while covering everything in this tutorial:

**Week 1 — File-only agents (no web, no bash)**
```python
ClaudeAgentOptions(
    allowed_tools=["Read", "Glob", "Grep"],
    model="claude-haiku-4-5",
    max_budget_usd=0.05,
    max_turns=5,
)
# Estimated weekly spend: under $1
```

**Week 2 — Add file editing and Bash**
```python
ClaudeAgentOptions(
    allowed_tools=["Read", "Edit", "Glob", "Bash"],
    model="claude-haiku-4-5",
    max_budget_usd=0.10,
    max_turns=10,
)
# Estimated weekly spend: under $2
```

**Week 3 — Add web search**
```python
ClaudeAgentOptions(
    allowed_tools=["WebSearch", "WebFetch", "Write"],
    model="claude-haiku-4-5",
    max_budget_usd=0.25,
    max_turns=20,
)
# Estimated weekly spend: $3–5 (searches dominate)
```

**Week 4 — Run the capstone research agent**
```python
# Use the ResearchConfig defaults from config.py
# max_budget_usd=0.75, model="claude-haiku-4-5"
# Estimated per-run: $0.20–0.40
# Estimated weekly spend: $2–5
```

### Account-Level Circuit Breaker

Set a hard monthly spending limit on your Anthropic account itself — independent of anything in your code. This is your last line of defense if you have a bug that bypasses your in-code limits.

Go to: `platform.claude.com/settings/limits`

Set it to **$10 or $20** while learning. Even a runaway agent with a code bug cannot exceed your account limit. This costs nothing to set and takes 30 seconds.

### Cost Guardrail Checklist

Before running any agent, verify:

- [ ] `model="claude-haiku-4-5"` unless you have a specific reason for Sonnet/Opus
- [ ] `max_budget_usd` is set (start at $0.05 for experiments, $0.25 for web agents)
- [ ] `max_turns` is set (start at 5, increase deliberately)
- [ ] Account-level spend limit is set at `platform.claude.com/settings/limits`
- [ ] If using `WebSearch`: budget is at least $0.10 to cover search fees

---

## Debugging Your Agent

### Print all message types

```python
async for message in query(prompt=..., options=...):
    print(f"[{type(message).__name__}]", end=" ")

    if isinstance(message, SystemMessage):
        print(f"subtype={message.subtype}")

    elif isinstance(message, AssistantMessage):
        for block in message.content:
            if hasattr(block, "text"):
                print(f"text={block.text[:80]}...")
            elif hasattr(block, "name"):
                print(f"tool={block.name} input={str(block.input)[:60]}")

    elif isinstance(message, UserMessage):
        print(f"(tool results)")

    elif isinstance(message, ResultMessage):
        print(f"subtype={message.subtype} turns={message.num_turns} cost=${message.total_cost_usd}")
```

### Common failure modes

**Agent loops without progress**
- Cause: tool is always failing and Claude keeps retrying
- Fix: add `max_turns=5` and inspect the error message in `ResultMessage`

**Agent ignores instructions**
- Cause: instructions were in the prompt and got compacted away
- Fix: put critical instructions in `system_prompt` in `ClaudeAgentOptions`

**Cost higher than expected**
- Cause: many web searches, or large files being read repeatedly
- Fix: add `max_budget_usd`, use Grep instead of Read for searching

**Agent asks for permission interactively when you want headless**
- Cause: using `"default"` or `"acceptEdits"` in a non-interactive script
- Fix: switch to `"dontAsk"` with explicit `allowed_tools`

**ResultMessage has no `result` field**
- Cause: non-success subtype
- Fix: always check `message.subtype == "success"` before accessing `message.result`

---

**→ Continue to Part 5: Capstone Project — AI Research Digest Agent**
