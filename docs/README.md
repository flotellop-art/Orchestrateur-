# Claude Managed Agents — Zero to Hero Tutorial Series
**Written April 8, 2026 — Day of Launch**

---

## Files in This Series

| File | Topic | Est. Read Time |
|---|---|---|
| `part1_mental_model.md` | Architecture, core concepts, mental model | 15 min |
| `part2_setup_and_first_agent.md` | Install, first working agent, code walkthrough | 30 min |
| `part3_agent_loop_deep_dive.md` | Turns, messages, context window, compaction | 25 min |
| `part4_tools_permissions_control.md` | All tools, permission modes, cost management | 25 min |
| `part5_capstone_research_agent.md` | Full project: AI Research Digest Agent | 45 min |

Total: ~2.5 hours to read + build

---

## What You Need Before Starting

- Python 3.10+
- Anthropic API key from `https://platform.claude.com/settings/keys`
- `pip install claude-agent-sdk python-dotenv`

---

## The One Thing to Know Before Reading Anything Else

Claude Managed Agents = you define what the agent does, Anthropic runs the infrastructure.

The SDK (`claude-agent-sdk`) is what you write Python code against. It handles the agent loop, tool execution, context management, and streaming — so you don't have to.

---

## Quick Start (skip the tutorial, get running in 5 minutes)

```python
# install.sh
pip install claude-agent-sdk python-dotenv
echo "ANTHROPIC_API_KEY=sk-ant-your-key" > .env
```

```python
# run.py
import asyncio
from dotenv import load_dotenv
from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage

load_dotenv()

async def main():
    async for msg in query(
        prompt="Search the web for the top 3 AI papers this week and summarize them.",
        options=ClaudeAgentOptions(
            allowed_tools=["WebSearch", "WebFetch"],
            permission_mode="dontAsk",
            model="claude-haiku-4-5",   # cheapest model — $1/$5 per MTok in/out
            max_budget_usd=0.30,
            max_turns=15,
        ),
    ):
        if isinstance(msg, ResultMessage):
            if msg.subtype == "success":
                print(msg.result)
            print(f"\nCost: ${msg.total_cost_usd:.4f}")

asyncio.run(main())
```

```bash
python run.py
```

That's it. You're running a Managed Agent.

---

## Source Documentation

All content derived from official Anthropic documentation:
- `https://platform.claude.com/docs/en/managed-agents/overview`
- `https://platform.claude.com/docs/en/agent-sdk/quickstart`
- `https://platform.claude.com/docs/en/agent-sdk/agent-loop`
- `https://platform.claude.com/docs/en/agent-sdk/python`
