# Claude Managed Agents — Zero to Hero Tutorial
## Part 2: Setup and Your First Agent

> **Series overview**
> - Part 1 — Mental model, architecture, core concepts
> - Part 2 — Setup, first agent, dissecting the code ← you are here
> - Part 3 — The agent loop deep dive
> - Part 4 — Tools, permissions, and control
> - Part 5 — Capstone project: AI Research Digest Agent

---

## Step 1: Install the SDK

```bash
mkdir my-agents && cd my-agents
pip install claude-agent-sdk python-dotenv
```

The `claude-agent-sdk` package includes:
- The `query()` function — your main entry point
- `ClaudeAgentOptions` — configuration object
- All message type classes (`AssistantMessage`, `ResultMessage`, etc.)
- The `ClaudeSDKClient` class for multi-turn sessions

No other dependencies needed for basic use.

---

## Step 2: API Key

Create a `.env` file:

```bash
# .env
ANTHROPIC_API_KEY=sk-ant-xxxxxxxxxxxxxxxxxxxxxxxx
```

Get your key from: `https://platform.claude.com/settings/keys`

The SDK reads `ANTHROPIC_API_KEY` automatically from the environment. No other configuration needed for the default setup.

**Third-party cloud backends** (if you need them):
```bash
# Amazon Bedrock
CLAUDE_CODE_USE_BEDROCK=1
# (plus AWS credentials via standard AWS config)

# Google Vertex AI
CLAUDE_CODE_USE_VERTEX=1
# (plus GCP credentials)

# Microsoft Azure
CLAUDE_CODE_USE_FOUNDRY=1
```

For this tutorial: just the `ANTHROPIC_API_KEY`.

---

## Step 3: Your First Agent — Hello World

Create `agent_hello.py`:

```python
import asyncio
from dotenv import load_dotenv
from claude_agent_sdk import query, ClaudeAgentOptions, AssistantMessage, ResultMessage

load_dotenv()

async def main():
    print("Starting agent...\n")

    async for message in query(
        prompt="List the files in the current directory and tell me what you see.",
        options=ClaudeAgentOptions(
            allowed_tools=["Bash", "Glob"],
            permission_mode="acceptEdits",
            model="claude-haiku-4-5",   # cheapest model — $1/$5 per MTok in/out
        ),
    ):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if hasattr(block, "text"):
                    print(block.text)
                elif hasattr(block, "name"):
                    print(f"[Tool called: {block.name}]")

        elif isinstance(message, ResultMessage):
            print(f"\n--- Done ---")
            print(f"Status: {message.subtype}")
            print(f"Cost: ${message.total_cost_usd:.6f}")
            print(f"Turns: {message.num_turns}")
            print(f"Session ID: {message.session_id}")

asyncio.run(main())
```

Run it:
```bash
python agent_hello.py
```

You'll see Claude call `Glob` or `Bash`, observe the results, and respond with a description. That's a complete agent loop.

---

## Dissecting Every Line

### The `query()` function

```python
async for message in query(
    prompt="...",
    options=ClaudeAgentOptions(...),
):
```

`query()` is the primary entry point. It:
1. Starts a new agent session
2. Runs the full agent loop autonomously
3. Returns an **async iterator** that yields messages as they happen

You use `async for` because the loop is streaming — messages arrive as Claude works, not all at once after it finishes. This is critical for long-running agents where you want to show progress.

### `ClaudeAgentOptions`

This is your control panel. Key fields:

```python
ClaudeAgentOptions(
    # Which tools Claude can use (auto-approved)
    allowed_tools=["Bash", "Glob", "Read", "Edit", "WebSearch", "WebFetch"],

    # What happens with tools not in allowed_tools
    permission_mode="acceptEdits",

    # Model selection — always set this explicitly.
    # Haiku is the default in this tutorial (cheapest, good for most tasks).
    # Use Sonnet or Opus only when Haiku's reasoning is insufficient.
    model="claude-haiku-4-5",   # $1/$5 per MTok in/out
    # model="claude-sonnet-4-6", # $3/$15 per MTok — better reasoning
    # model="claude-opus-4-6",   # $5/$25 per MTok — best reasoning

    # Custom instructions on top of the default system prompt
    system_prompt="You are a meticulous researcher. Always cite sources.",

    # Working directory (defaults to current directory)
    cwd="/path/to/project",

    # Max turns before stopping (prevents runaway agents)
    max_turns=10,

    # Max spend before stopping
    max_budget_usd=0.50,

    # Reasoning depth
    effort="high",  # "low", "medium", "high", "max"
)
```

### Message types

The async iterator yields five types of messages:

```python
from claude_agent_sdk import (
    SystemMessage,    # session lifecycle (init, compact_boundary)
    AssistantMessage, # Claude's output each turn (text + tool calls)
    UserMessage,      # tool results fed back to Claude
    StreamEvent,      # raw streaming events (if partial messages enabled)
    ResultMessage,    # FINAL message — always last
)
```

**The most important pattern:**

```python
async for message in query(prompt=..., options=...):

    # See what Claude is doing turn by turn
    if isinstance(message, AssistantMessage):
        for block in message.content:
            if hasattr(block, "text"):
                print(block.text)        # Claude's reasoning
            elif hasattr(block, "name"):
                print(f"Tool: {block.name}")  # tool being called

    # Get the final result
    elif isinstance(message, ResultMessage):
        if message.subtype == "success":
            print(f"Result: {message.result}")
        else:
            print(f"Failed: {message.subtype}")
        print(f"Cost: ${message.total_cost_usd:.6f}")
```

### `ResultMessage` subtypes — always check this

| Subtype | Meaning | `result` field? |
|---|---|---|
| `"success"` | Task completed normally | Yes |
| `"error_max_turns"` | Hit `max_turns` limit | No |
| `"error_max_budget_usd"` | Hit `max_budget_usd` limit | No |
| `"error_during_execution"` | API or runtime error | No |
| `"error_max_structured_output_retries"` | Structured output failed | No |

**Never read `message.result` without checking `message.subtype == "success"` first.**

---

## Step 4: A More Useful Agent — Bug Fixer

Create `bugfix_demo/utils.py`:

```python
# utils.py — intentionally buggy
def calculate_average(numbers):
    total = 0
    for num in numbers:
        total += num
    return total / len(numbers)   # BUG: crashes on empty list

def get_user_name(user):
    return user["name"].upper()   # BUG: crashes if user is None

def parse_config(config_str):
    parts = config_str.split("=")
    return {parts[0]: parts[1]}   # BUG: crashes if no "=" in string
```

Create `agent_bugfix.py`:

```python
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from claude_agent_sdk import query, ClaudeAgentOptions, AssistantMessage, ResultMessage

load_dotenv()

async def main():
    project_dir = str(Path("bugfix_demo").resolve())

    print("Bug-finding agent starting...\n")
    print("=" * 50)

    async for message in query(
        prompt=(
            "Review utils.py carefully. "
            "Find ALL bugs that would cause runtime crashes. "
            "Fix every bug with proper defensive code. "
            "After fixing, add a brief comment next to each fix explaining what you changed and why."
        ),
        options=ClaudeAgentOptions(
            allowed_tools=["Read", "Edit", "Glob"],
            permission_mode="acceptEdits",
            model="claude-haiku-4-5",   # $1/$5 per MTok — fine for focused file tasks
            cwd=project_dir,
            max_turns=10,
            max_budget_usd=0.10,
        ),
    ):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if hasattr(block, "text") and block.text.strip():
                    print(block.text)
                elif hasattr(block, "name"):
                    print(f"\n[→ Tool: {block.name}]")

        elif isinstance(message, ResultMessage):
            print("\n" + "=" * 50)
            if message.subtype == "success":
                print("✓ Agent completed successfully")
            else:
                print(f"✗ Agent stopped: {message.subtype}")
            print(f"Turns used: {message.num_turns}")
            print(f"Total cost: ${message.total_cost_usd:.6f}")
            print(f"Session ID: {message.session_id}")
            print("\nCheck bugfix_demo/utils.py — it should be fixed.")

asyncio.run(main())
```

Run it:
```bash
mkdir bugfix_demo
# copy utils.py into bugfix_demo/
python agent_bugfix.py
```

The agent will: read the file, analyze all three bugs, edit the file with fixes and comments — all autonomously.

---

## Step 5: Multi-Turn Sessions (ClaudeSDKClient)

For interactive or stateful workflows, use `ClaudeSDKClient` instead of `query()`. It maintains session state across multiple calls automatically.

```python
import asyncio
from dotenv import load_dotenv
from claude_agent_sdk import ClaudeSDKClient, ClaudeAgentOptions, AssistantMessage, TextBlock

load_dotenv()

async def main():
    options = ClaudeAgentOptions(
        allowed_tools=["Read", "Glob", "Bash"],
        permission_mode="acceptEdits",
        model="claude-haiku-4-5",   # $1/$5 per MTok
    )

    async with ClaudeSDKClient(options=options) as client:

        # First turn
        await client.query("What Python files are in this directory?")
        async for message in client.receive_response():
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        print(f"Claude: {block.text}")

        # Second turn — same session, Claude remembers the first turn
        await client.query("Now count the total number of functions across all those files.")
        async for message in client.receive_response():
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        print(f"Claude: {block.text}")

asyncio.run(main())
```

`ClaudeSDKClient`:
- Is an async context manager (`async with`)
- Maintains the session across multiple `.query()` calls
- Claude remembers everything from previous turns within the session
- **Important:** do not use `break` inside the `receive_response()` iterator — it causes asyncio cleanup issues. Use flags to stop processing early instead.

---

## Session IDs — Your Key to Resumability

Every `ResultMessage` contains a `session_id`. Save it. You can resume any session:

```python
import asyncio
from dotenv import load_dotenv
from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage

load_dotenv()

SAVED_SESSION_ID = None  # will be set after first run

async def run_first():
    global SAVED_SESSION_ID
    async for message in query(
        prompt="Start analyzing the codebase. Begin with the directory structure.",
        options=ClaudeAgentOptions(
            allowed_tools=["Read", "Glob", "Grep"],
            permission_mode="acceptEdits",
            model="claude-haiku-4-5",   # $1/$5 per MTok
        ),
    ):
        if isinstance(message, ResultMessage):
            SAVED_SESSION_ID = message.session_id
            print(f"Session ID saved: {SAVED_SESSION_ID}")

async def resume():
    # Resume the same session — Claude picks up where it left off
    async for message in query(
        prompt="Continue the analysis. Now look at the main module's imports.",
        options=ClaudeAgentOptions(
            allowed_tools=["Read", "Glob", "Grep"],
            permission_mode="acceptEdits",
            model="claude-haiku-4-5",   # $1/$5 per MTok
            # resume_session_id=SAVED_SESSION_ID,  # coming in full SDK
        ),
    ):
        if isinstance(message, ResultMessage):
            print(f"Resumed session result: {message.subtype}")

asyncio.run(run_first())
# later...
# asyncio.run(resume())
```

---

## Common Patterns Cheat Sheet

```python
# Pattern 1: Simple one-shot agent (most common)
async for message in query(prompt="...", options=opts):
    if isinstance(message, ResultMessage) and message.subtype == "success":
        do_something(message.result)

# Pattern 2: Progress reporting
async for message in query(prompt="...", options=opts):
    if isinstance(message, AssistantMessage):
        show_progress(message)
    elif isinstance(message, ResultMessage):
        handle_result(message)

# Pattern 3: Cost-guarded agent
opts = ClaudeAgentOptions(
    allowed_tools=["Read", "Glob"],
    model="claude-haiku-4-5",   # $1/$5 per MTok
    max_budget_usd=0.05,        # hard stop at 5 cents
    max_turns=5,                # hard stop at 5 turns
)

# Pattern 4: Custom system prompt
opts = ClaudeAgentOptions(
    allowed_tools=["Read", "Edit"],
    model="claude-haiku-4-5",   # $1/$5 per MTok
    system_prompt="You are a senior Python engineer. "
                  "Always follow PEP 8. "
                  "Add type hints to every function you write.",
)

# Pattern 5: Web-enabled agent
opts = ClaudeAgentOptions(
    allowed_tools=["WebSearch", "WebFetch", "Write"],
    permission_mode="acceptEdits",
    model="claude-haiku-4-5",   # $1/$5 per MTok — upgrade to Sonnet if synthesis quality is low
)
```

---

## What You Can Safely Ignore Right Now

The docs mention these — skip them until you need them:
- `StreamEvent` / `include_partial_messages` — for real-time token streaming UI
- `Hooks` — for intercepting tool calls before they run
- MCP servers — for external service integration
- Skills — for reusable agent capability bundles
- `bypassPermissions` mode — for fully sandboxed CI environments
- Subagents — for multi-agent orchestration

Master `query()`, `ClaudeAgentOptions`, and `ResultMessage` first. The rest layers on top cleanly.

---

**→ Continue to Part 3: The Agent Loop Deep Dive**
