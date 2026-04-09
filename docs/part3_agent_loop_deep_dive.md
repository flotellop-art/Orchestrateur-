# Claude Managed Agents — Zero to Hero Tutorial
## Part 3: The Agent Loop Deep Dive

> **Series overview**
> - Part 1 — Mental model, architecture, core concepts
> - Part 2 — Setup, first agent, dissecting the code
> - Part 3 — The agent loop deep dive ← you are here
> - Part 4 — Tools, permissions, and control
> - Part 5 — Capstone project: AI Research Digest Agent

---

## What Actually Happens Inside the Loop

When you call `query()`, you're not making a single API call. You're starting an autonomous execution cycle that runs until Claude finishes the task or hits a limit you set.

Here's the full picture:

```
┌─────────────────────────────────────────────────────────────────┐
│                        YOUR CODE                                │
│  async for message in query(prompt=..., options=...):           │
└─────────────────────────────┬───────────────────────────────────┘
                              │ starts
┌─────────────────────────────▼───────────────────────────────────┐
│                    SDK INITIALIZATION                            │
│  1. Emit SystemMessage(subtype="init") with session metadata     │
│  2. Load system prompt + tool definitions → context window      │
│  3. Send prompt to Claude                                        │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                      TURN N (repeating)                          │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Claude evaluates: prompt + history + tool definitions    │   │
│  │ → Produces: text blocks AND/OR tool_call blocks          │   │
│  └──────────────────────────┬──────────────────────────────┘   │
│                             │                                   │
│                 ┌───────────▼───────────┐                       │
│                 │   Has tool calls?      │                       │
│                 └───────┬───────┬───────┘                       │
│                        YES      NO                              │
│                         │        └──────────────────────────┐  │
│  ┌──────────────────────▼──────────────────────────┐        │  │
│  │ Emit AssistantMessage (text + tool calls)        │        │  │
│  │ Execute tools (parallel if read-only)            │        │  │
│  │ Emit UserMessage (tool results)                  │        │  │
│  │ Feed results back → next turn                    │        │  │
│  └──────────────────────────────────────────────────┘        │  │
│                                                               │  │
│                         ┌─────────────────────────────────┐  │  │
│                         │  Emit final AssistantMessage     │  │  │
│                         │  Emit ResultMessage (subtype=    │  │  │
│                         │  "success" + result + cost)      │  │  │
│                         └─────────────────────────────────┘  │  │
└─────────────────────────────────────────────────────────────────┘
```

The key insight: **each tool-call round trip is one "turn."** A turn is not one API call — it's one complete cycle of: Claude decides → tools execute → results feed back.

---

## Turns in Practice

Let's trace a real example. Prompt: "Fix the failing tests in auth.ts"

```
SDK init → emit SystemMessage(subtype="init")

TURN 1:
  Claude → calls Bash("npm test")
  SDK    → emits AssistantMessage [tool_call: Bash]
  SDK    → executes Bash, 3 failures in output
  SDK    → emits UserMessage [tool_result: 3 failures]

TURN 2:
  Claude → calls Read("auth.ts"), Read("auth.test.ts")
  SDK    → emits AssistantMessage [tool_call: Read x2]
  SDK    → executes both Reads in PARALLEL (read-only tools)
  SDK    → emits UserMessage [tool_results: file contents]

TURN 3:
  Claude → calls Edit("auth.ts", patch), then Bash("npm test")
  SDK    → emits AssistantMessage [tool_call: Edit, then Bash]
  SDK    → executes Edit FIRST (state-modifying), then Bash
  SDK    → emits UserMessage [tool_results: edited, 0 failures]

FINAL TURN (no tool calls):
  Claude → "Fixed the null check bug in validateToken(). All 3 tests pass."
  SDK    → emits final AssistantMessage [text only]
  SDK    → emits ResultMessage(subtype="success", result="Fixed...", cost=0.0031)
```

That's **4 turns** from one `query()` call. Your `async for` loop received 7+ messages during this time.

---

## Controlling the Loop

### max_turns — prevent runaway execution

```python
options = ClaudeAgentOptions(
    allowed_tools=["Read", "Edit", "Bash"],
    model="claude-haiku-4-5",   # $1/$5 per MTok
    max_turns=10,  # stop after 10 tool-call turns
)
```

When hit: `ResultMessage(subtype="error_max_turns")`. The `result` field is absent. You get cost and session_id so you can resume.

Rule of thumb:
- Simple file tasks: `max_turns=5`
- Multi-file refactors: `max_turns=20`
- Research/web tasks: `max_turns=30`
- Unbounded: don't set it (only in monitored production)

### max_budget_usd — cost ceiling

```python
options = ClaudeAgentOptions(
    allowed_tools=["WebSearch", "WebFetch", "Write"],
    model="claude-haiku-4-5",   # $1/$5 per MTok
    max_budget_usd=0.50,  # stop at 50 cents
)
```

When hit: `ResultMessage(subtype="error_max_budget_usd")`.

This is your safety valve in production. Set it always for any agent that can run web searches or long Bash commands.

### effort — reasoning depth per turn

```python
options = ClaudeAgentOptions(
    model="claude-haiku-4-5",   # $1/$5 per MTok
    effort="high",  # "low" | "medium" | "high" | "max"
)
```

| Level | Use case | Token cost |
|---|---|---|
| `"low"` | Directory listing, simple lookups | Minimal |
| `"medium"` | Standard edits, structured tasks | Moderate |
| `"high"` | Debugging, multi-file reasoning (default TypeScript) | Higher |
| `"max"` | Complex refactors, deep analysis | Maximum |

`effort` ≠ extended thinking. They're independent. You can combine:
```python
# Maximum reasoning WITH visible chain-of-thought
options = ClaudeAgentOptions(
    model="claude-haiku-4-5",   # $1/$5 per MTok
    effort="max",
    # extended thinking configured separately in model options
)
```

For most tasks: `"high"` is the right default. Use `"low"` for agents doing mechanical work (file listing, simple grep, format conversion).

---

## Message Types — Complete Reference

```python
from claude_agent_sdk import (
    SystemMessage,
    AssistantMessage,
    UserMessage,
    StreamEvent,
    ResultMessage,
)
```

### SystemMessage
```python
if isinstance(message, SystemMessage):
    if message.subtype == "init":
        # First message of every session
        # Contains: session_id, model, tools loaded
        print(f"Session started")
    elif message.subtype == "compact_boundary":
        # Context was automatically compacted
        # Older conversation history was summarized
        print("Context compacted — older history summarized")
```

### AssistantMessage
```python
if isinstance(message, AssistantMessage):
    for block in message.content:
        # Text blocks: Claude's reasoning/output
        if hasattr(block, "text"):
            print(f"Claude: {block.text}")

        # Tool call blocks: what Claude wants to do
        elif hasattr(block, "name"):
            print(f"Tool: {block.name}({block.input})")
```

### UserMessage
```python
if isinstance(message, UserMessage):
    # Tool results being fed back to Claude
    # Usually you don't need to handle this — it's informational
    for block in message.content:
        if hasattr(block, "content"):
            print(f"Tool result: {str(block.content)[:100]}...")
```

### ResultMessage — the only one you must handle
```python
if isinstance(message, ResultMessage):
    print(f"Status: {message.subtype}")

    if message.subtype == "success":
        print(f"Output:\n{message.result}")

    # Always available regardless of subtype:
    print(f"Cost: ${message.total_cost_usd:.6f}")
    print(f"Turns: {message.num_turns}")
    print(f"Session: {message.session_id}")

    # Usage details
    if message.usage:
        print(f"Input tokens: {message.usage.input_tokens}")
        print(f"Output tokens: {message.usage.output_tokens}")
        print(f"Cache reads: {message.usage.cache_read_input_tokens}")
```

---

## The Context Window — What It Is and Why It Matters

Claude's context window is the total working memory available during a session. Everything accumulates there across turns:

```
Context window contents (grows each turn):
┌───────────────────────────────────────────────────┐
│ System prompt           (fixed size, prompt cached)│
│ Tool definitions        (fixed, prompt cached)     │
│ Turn 1: User prompt                                │
│ Turn 1: Claude response + tool calls               │
│ Turn 1: Tool results                               │
│ Turn 2: Claude response + tool calls               │
│ Turn 2: Tool results                               │
│ ...                                                │
│ Turn N: (current)                                  │
└───────────────────────────────────────────────────┘
```

**This never resets within a session.** Every file Claude reads, every bash output it sees, every response it generates — all of it stays in the context.

### What consumes the most context

| Source | Impact |
|---|---|
| System prompt + CLAUDE.md | Fixed cost, prompt-cached after first turn |
| Tool definitions | Each tool adds its schema; 10 tools ≈ 2,000 tokens overhead |
| File reads | A 500-line Python file ≈ 3,000–5,000 tokens |
| Bash output | Verbose commands (npm install, test output) ≈ 1,000–10,000 tokens |
| Web fetch | Average page ≈ 2,500 tokens; large docs ≈ 25,000 tokens |

For a long agent session with 30 turns doing file reads and web searches, you can consume 100,000+ tokens of context. With Sonnet 4.6 at $3/MTok input, that's $0.30 just in context accumulation — before output tokens.

### Automatic compaction

When the context approaches its limit (~80% full), the SDK **automatically compacts** it:
1. Summarizes the older conversation history
2. Replaces old messages with a compressed summary
3. Emits `SystemMessage(subtype="compact_boundary")` to notify you
4. The agent continues with the summary in place of the original history

**Important consequence:** Instructions given early in the conversation may not survive compaction. Persistent rules and preferences should be in the system prompt, not in the initial prompt message.

```python
# BAD: important rule in the prompt (may be lost after compaction)
async for msg in query(
    prompt="IMPORTANT: always write tests. Now refactor the entire codebase.",
    options=...,
):
    ...

# GOOD: important rules in the system prompt
async for msg in query(
    prompt="Refactor the entire codebase.",
    options=ClaudeAgentOptions(
        system_prompt="CRITICAL: Always write unit tests for every function you create or modify.",
        ...
    ),
):
    ...
```

---

## Parallel vs Sequential Tool Execution

When Claude requests multiple tools in one turn:

```
Claude requests: [Read("file_a.py"), Read("file_b.py"), Read("file_c.py")]
```

Read-only tools run **in parallel**:
```
Time →
Read("file_a.py") ████████
Read("file_b.py") ████████
Read("file_c.py") ████████
Total: 1x latency instead of 3x
```

State-modifying tools run **sequentially**:
```
Claude requests: [Edit("auth.py", ...), Bash("python -m pytest")]

Edit("auth.py") ████████
                         Bash("pytest") ████████████
Total: 2x latency (required for correctness)
```

This is handled automatically by the SDK. You don't configure it.

Custom tools default to sequential. Mark them read-only to enable parallelism:
```python
# Python
options = ClaudeAgentOptions(
    # readOnlyHint=True in your custom tool definition
)
```

---

## Practical Loop Control Patterns

### Pattern: Timeout + cost guard

```python
import asyncio
from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage

async def run_guarded_agent(prompt: str, max_cost: float = 0.25):
    """Run agent with hard cost and time limits."""
    results = []

    try:
        async with asyncio.timeout(300):  # 5 minute wall-clock timeout
            async for message in query(
                prompt=prompt,
                options=ClaudeAgentOptions(
                    allowed_tools=["Read", "Glob", "WebSearch", "WebFetch"],
                    permission_mode="acceptEdits",
                    model="claude-haiku-4-5",   # $1/$5 per MTok
                    max_budget_usd=max_cost,
                    max_turns=20,
                    effort="high",
                ),
            ):
                if isinstance(message, ResultMessage):
                    results.append(message)
    except asyncio.TimeoutError:
        print("Agent timed out after 5 minutes")

    return results[0] if results else None
```

### Pattern: Progress callback

```python
from claude_agent_sdk import AssistantMessage, ResultMessage

async def run_with_progress(prompt: str, on_progress=None):
    """Run agent and call on_progress with each turn summary."""
    async for message in query(prompt=prompt, options=...):

        if isinstance(message, AssistantMessage) and on_progress:
            tool_calls = [
                b.name for b in message.content if hasattr(b, "name")
            ]
            if tool_calls:
                on_progress(f"Using tools: {', '.join(tool_calls)}")

        elif isinstance(message, ResultMessage):
            return message

def my_progress_handler(status: str):
    print(f"[Agent] {status}")

result = asyncio.run(
    run_with_progress("Analyze codebase", on_progress=my_progress_handler)
)
```

### Pattern: Result validation

```python
from claude_agent_sdk import ResultMessage

def handle_result(message: ResultMessage) -> str | None:
    """Extract result or raise descriptive error."""

    match message.subtype:
        case "success":
            return message.result

        case "error_max_turns":
            raise RuntimeError(
                f"Agent ran out of turns. "
                f"Used {message.num_turns} turns, cost ${message.total_cost_usd:.4f}. "
                f"Session {message.session_id} can be resumed."
            )

        case "error_max_budget_usd":
            raise RuntimeError(
                f"Agent hit budget limit. "
                f"Cost ${message.total_cost_usd:.4f}. "
                f"Session {message.session_id} can be resumed."
            )

        case "error_during_execution":
            raise RuntimeError(
                f"Agent execution error. Session: {message.session_id}"
            )

        case _:
            raise RuntimeError(f"Unknown result: {message.subtype}")
```

---

## The Compaction Boundary: What to Expect

When you see:
```
[SystemMessage: subtype=compact_boundary]
```

This means:
- Context was getting full
- Older messages were summarized
- The agent continues — no action needed from you
- Specific details from early in the conversation are now compressed

For most agents this is transparent. For precision-critical workflows (e.g., agents that must remember exact values from turn 1), keep critical data in the system prompt or have the agent write it to a file it can re-read later.

---

**→ Continue to Part 4: Tools, Permissions, and Control**
