# Claude Managed Agents — Zero to Hero Tutorial
## Part 5: Capstone Project — AI Research Digest Agent

> **Series overview**
> - Part 1 — Mental model, architecture, core concepts
> - Part 2 — Setup, first agent, dissecting the code
> - Part 3 — The agent loop deep dive
> - Part 4 — Tools, permissions, and control
> - Part 5 — Capstone project: AI Research Digest Agent ← you are here

---

## What You're Building

An agent that takes a research topic and autonomously:
1. Searches arXiv, Google Scholar mentions, and the open web for the last 30 days
2. Fetches and reads the top 5–8 most relevant sources
3. Extracts key findings from each source
4. Synthesizes a structured digest with full citations
5. Writes the output to a timestamped markdown file in `~/digests/`

This is a **real, production-quality agent**. It demonstrates every concept from the tutorial:
- Web search + web fetch tool combination
- File writing
- Multi-turn reasoning (search → read → synthesize)
- Proper cost guarding
- Result validation and error handling
- Structured output via system prompt

---

## Project Structure

```
research_agent/
├── .env                    # ANTHROPIC_API_KEY
├── agent.py                # main agent runner
├── config.py               # topic configs + cost settings
├── output_formatter.py     # result processing
└── digests/                # output directory (created automatically)
    └── YYYY-MM-DD_topic.md
```

---

## Step 1: Create the Project

```bash
mkdir research_agent && cd research_agent
pip install claude-agent-sdk python-dotenv
echo "ANTHROPIC_API_KEY=sk-ant-your-key-here" > .env
mkdir -p digests
```

---

## Step 2: `config.py` — Research Configuration

```python
# config.py
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ResearchConfig:
    """Configuration for a research digest run."""

    # The research topic
    topic: str

    # Optional: constrain to specific domains
    domains: list[str] = field(default_factory=lambda: [
        "arxiv.org", "nature.com", "science.org",
        "scholar.google.com", "openai.com", "anthropic.com",
        "deepmind.com", "semanticscholar.org"
    ])

    # How many sources to read in depth (balance quality vs cost)
    max_sources: int = 6

    # Days to look back
    recency_days: int = 30

    # Output settings
    output_dir: str = "digests"

    # Cost controls
    max_budget_usd: float = 0.75
    max_turns: int = 35

    # Model choice — default is Haiku (cheapest).
    # Upgrade to Sonnet if synthesis quality is insufficient.
    model: str = "claude-haiku-4-5"   # $1/$5 per MTok in/out
    # model: str = "claude-sonnet-4-6"  # $3/$15 per MTok — better reasoning
    effort: str = "high"


# Pre-built research profiles
PROFILES = {
    "ai_agents": ResearchConfig(
        topic="autonomous AI agents, multi-agent systems, agentic AI",
        max_sources=8,
        model="claude-haiku-4-5",   # $1/$5 per MTok
        max_budget_usd=1.00,
    ),
    "llm_reasoning": ResearchConfig(
        topic="LLM reasoning, chain-of-thought, test-time compute scaling",
        max_sources=6,
        model="claude-haiku-4-5",   # $1/$5 per MTok
    ),
    "rl_from_feedback": ResearchConfig(
        topic="RLHF, RLAIF, DPO, reward modeling for language models",
        max_sources=6,
        model="claude-haiku-4-5",   # $1/$5 per MTok
    ),
    "diffusion_models": ResearchConfig(
        topic="diffusion models, image generation, video generation",
        max_sources=5,
        model="claude-haiku-4-5",   # $1/$5 per MTok
        max_budget_usd=0.50,
    ),
    "ai_safety": ResearchConfig(
        topic="AI alignment, interpretability, mechanistic interpretability, AI safety",
        max_sources=7,
        model="claude-haiku-4-5",   # $1/$5 per MTok
    ),
}
```

---

## Step 3: `agent.py` — The Main Agent

```python
# agent.py
import asyncio
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from claude_agent_sdk import (
    query,
    ClaudeAgentOptions,
    AssistantMessage,
    ResultMessage,
    SystemMessage,
)

from config import ResearchConfig, PROFILES

load_dotenv()


def build_system_prompt(config: ResearchConfig) -> str:
    """Build the agent's system prompt from config."""
    return f"""You are an expert AI research analyst producing a structured literature digest.

TASK:
Search for and synthesize recent research on: {config.topic}
Focus on content published in the last {config.recency_days} days.

PROCESS (follow in order):
1. SEARCH: Use WebSearch to find 10-15 relevant recent sources.
   - Search queries should be specific and varied.
   - Include at least 3 different search queries.
   - Prioritize: arXiv papers, major lab blogs, Nature/Science, peer-reviewed venues.

2. EVALUATE: From your search results, identify the {config.max_sources} most significant sources.
   Prioritize: novelty, credibility of source, direct relevance to the topic.

3. FETCH: Use WebFetch to read the full content of each selected source.
   Extract: title, authors/org, date, key findings (3-5 bullet points), methodology summary.

4. SYNTHESIZE: Identify cross-cutting themes and contradictions across sources.

5. WRITE: Write the complete digest to a file using the Write tool.
   File path: {config.output_dir}/{datetime.now().strftime('%Y-%m-%d')}_{config.topic.split(',')[0].replace(' ', '_')[:40]}.md

OUTPUT FORMAT (write this exact structure):
---
# Research Digest: {config.topic}
Generated: {datetime.now().strftime('%B %d, %Y')}

## Executive Summary
[2-3 paragraph synthesis of the field's current state and major themes]

## Key Findings by Source

### [Paper/Article Title]
**Source:** [URL]
**Date:** [Publication date]
**Authors/Organization:** [Names or org]
**Significance:** [Why this matters — 1-2 sentences]
**Key Points:**
- [Finding 1]
- [Finding 2]
- [Finding 3]

[repeat for each source]

## Cross-Cutting Themes
[3-5 themes that appear across multiple sources, with evidence]

## Contradictions and Open Questions
[Points of disagreement or unresolved questions in the literature]

## What to Watch Next
[3-5 specific follow-up areas, papers to look for, or researchers to track]

## Full Source List
[numbered list: Title | URL | Date]
---

RULES:
- Every claim must trace to a specific source in your source list.
- Include exact URLs — no paraphrased or approximate URLs.
- If a page fails to load, note it and move to the next source.
- Do not speculate beyond what sources say.
- The file MUST be written to disk using the Write tool before you finish.
"""


def build_prompt(config: ResearchConfig) -> str:
    """Build the user prompt."""
    return (
        f"Research the following topic and produce a complete digest:\n\n"
        f"Topic: {config.topic}\n\n"
        f"Time range: last {config.recency_days} days\n"
        f"Minimum sources: {config.max_sources}\n\n"
        f"Begin with your search strategy, then proceed systematically."
    )


async def run_research_agent(config: ResearchConfig) -> dict:
    """
    Run the research digest agent.
    Returns a dict with status, result, cost, session_id.
    """

    options = ClaudeAgentOptions(
        allowed_tools=["WebSearch", "WebFetch", "Write", "Read"],
        permission_mode="dontAsk",
        model=config.model,
        effort=config.effort,
        max_turns=config.max_turns,
        max_budget_usd=config.max_budget_usd,
        system_prompt=build_system_prompt(config),
        cwd=str(Path.cwd()),
    )

    print(f"\n{'='*60}")
    print(f"Research Agent Starting")
    print(f"Topic: {config.topic}")
    print(f"Budget: ${config.max_budget_usd:.2f} | Max turns: {config.max_turns}")
    print(f"{'='*60}\n")

    turn_count = 0
    search_count = 0
    fetch_count = 0

    async for message in query(
        prompt=build_prompt(config),
        options=options,
    ):

        # Session initialized
        if isinstance(message, SystemMessage):
            if message.subtype == "init":
                print("[Session initialized]\n")
            elif message.subtype == "compact_boundary":
                print("[Context compacted — continuing...]\n")

        # Claude is working
        elif isinstance(message, AssistantMessage):
            for block in message.content:

                # Print Claude's reasoning
                if hasattr(block, "text") and block.text.strip():
                    # Trim long text for display
                    text = block.text.strip()
                    if len(text) > 300:
                        print(f"{text[:300]}...\n")
                    else:
                        print(f"{text}\n")

                # Track tool usage
                elif hasattr(block, "name"):
                    tool = block.name
                    if tool == "WebSearch":
                        search_count += 1
                        query_text = str(block.input.get("query", ""))[:60]
                        print(f"  [Search #{search_count}] {query_text}")
                    elif tool == "WebFetch":
                        fetch_count += 1
                        url = str(block.input.get("url", ""))[:70]
                        print(f"  [Fetch #{fetch_count}] {url}")
                    elif tool == "Write":
                        filepath = block.input.get("path", "unknown")
                        print(f"  [Writing output → {filepath}]")
                    elif tool == "Read":
                        print(f"  [Reading: {block.input.get('path', '')}]")

            turn_count += 1

        # Final result
        elif isinstance(message, ResultMessage):
            print(f"\n{'='*60}")
            print(f"Agent Complete")
            print(f"Status: {message.subtype}")
            print(f"Turns: {message.num_turns}")
            print(f"Searches: {search_count} | Fetches: {fetch_count}")

            cost = message.total_cost_usd or 0.0
            print(f"Total cost: ${cost:.4f}")

            if message.usage:
                u = message.usage
                print(f"Tokens — in: {u.input_tokens:,} | out: {u.output_tokens:,} | cache_hits: {u.cache_read_input_tokens:,}")

            print(f"Session ID: {message.session_id}")
            print(f"{'='*60}\n")

            return {
                "status": message.subtype,
                "result": message.result if message.subtype == "success" else None,
                "cost": cost,
                "turns": message.num_turns,
                "searches": search_count,
                "fetches": fetch_count,
                "session_id": message.session_id,
            }

    return {"status": "unknown", "result": None, "cost": 0.0}


def verify_output(config: ResearchConfig) -> Path | None:
    """Check that the digest file was actually written."""
    output_dir = Path(config.output_dir)
    today = datetime.now().strftime("%Y-%m-%d")
    topic_slug = config.topic.split(",")[0].replace(" ", "_")[:40]
    expected_path = output_dir / f"{today}_{topic_slug}.md"

    if expected_path.exists():
        size_kb = expected_path.stat().st_size / 1024
        print(f"Output verified: {expected_path} ({size_kb:.1f} KB)")
        return expected_path

    # Search for any file written today
    if output_dir.exists():
        todays_files = list(output_dir.glob(f"{today}_*.md"))
        if todays_files:
            latest = sorted(todays_files)[-1]
            size_kb = latest.stat().st_size / 1024
            print(f"Output found: {latest} ({size_kb:.1f} KB)")
            return latest

    print("WARNING: No output file found. Agent may not have written the digest.")
    return None


async def main():
    # Parse topic from command line or use default
    if len(sys.argv) > 1:
        topic_arg = sys.argv[1]
        if topic_arg in PROFILES:
            config = PROFILES[topic_arg]
            print(f"Using profile: {topic_arg}")
        else:
            # Custom topic from command line
            config = ResearchConfig(topic=topic_arg)
    else:
        # Default: AI agents (most relevant to this tutorial)
        config = PROFILES["ai_agents"]

    # Run the agent
    result = await run_research_agent(config)

    # Verify output was written
    if result["status"] == "success":
        output_path = verify_output(config)
        if output_path:
            print(f"\nDigest ready: {output_path}")
            print(f"Open with: open '{output_path}'")
        else:
            print("\nNote: Agent reported success but file not found.")
            print("The agent may have used a different filename.")
    else:
        print(f"\nAgent did not complete successfully: {result['status']}")
        if result["status"] == "error_max_budget_usd":
            print(f"Budget exhausted. Increase max_budget_usd in config.")
        elif result["status"] == "error_max_turns":
            print(f"Turn limit hit. Increase max_turns in config.")

    print(f"\nSession ID for resume: {result.get('session_id', 'N/A')}")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Step 4: Run It

```bash
# Run with default profile (AI agents)
python agent.py

# Run with a specific profile
python agent.py llm_reasoning
python agent.py ai_safety

# Run with a custom topic
python agent.py "transformer architecture improvements 2026"
python agent.py "protein structure prediction AlphaFold"
python agent.py "quantum error correction"
```

Expected runtime: 3–8 minutes. Expected cost: $0.40–$1.20 depending on topic and sources.

---

## Step 5: Understand What the Agent Is Doing

Watch the output and map it to the agent loop from Part 3:

```
[Session initialized]

I'll research recent developments in autonomous AI agents...

  [Search #1] autonomous AI agents multi-agent systems 2026       ← Turn 1: WebSearch
  [Search #2] agentic AI frameworks papers April 2026
  [Search #3] multi-agent coordination LLM recent

Based on these results, I'll fetch the most relevant sources...

  [Fetch #1] https://arxiv.org/abs/2604.xxxxx                    ← Turn 2+: WebFetch (parallel)
  [Fetch #2] https://openai.com/research/...
  [Fetch #3] https://anthropic.com/research/...

I've now read 6 sources. Let me synthesize the key themes...    ← Claude reasoning

  [Writing output → digests/2026-04-08_autonomous_AI_agents.md] ← Final turn: Write

[Agent Complete]
Status: success
Turns: 12
Searches: 3 | Fetches: 6
Total cost: $0.2341    ← Haiku 4.5 pricing ($1/$5 per MTok)
```

**What's happening under the hood:**
- Turn 1: 3 WebSearch calls (potentially parallel)
- Turns 2-4: WebFetch calls (parallel where possible)
- Turns 5-11: More fetches, reading, analysis
- Turn 12: Write tool — saves the digest
- Final turn (no tools): "I've completed the research digest." → ResultMessage

---

## Extending the Capstone

### Extension 1: Scheduled daily digest

```python
# run_daily.py
import asyncio
import schedule
import time
from agent import run_research_agent
from config import PROFILES

async def daily_job():
    for name, config in PROFILES.items():
        print(f"\nRunning digest: {name}")
        await run_research_agent(config)
        await asyncio.sleep(60)  # 1 min between runs

def run():
    asyncio.run(daily_job())

schedule.every().day.at("06:00").do(run)

while True:
    schedule.run_pending()
    time.sleep(60)
```

### Extension 2: Email the digest

```python
# After agent completes, email the output:
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

def email_digest(digest_path: Path, to_email: str):
    content = digest_path.read_text()
    # ... standard SMTP send
```

### Extension 3: Multi-topic parallel agents

```python
# Run multiple research agents in parallel
async def run_all_profiles():
    tasks = [
        run_research_agent(config)
        for config in PROFILES.values()
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for name, result in zip(PROFILES.keys(), results):
        print(f"{name}: {result.get('status')} | ${result.get('cost', 0):.4f}")

asyncio.run(run_all_profiles())
```

**Note:** Running agents in parallel multiplies your API costs. Each agent runs independently. Be careful with your budget.

### Extension 4: Add a custom system prompt for your domain

```python
from config import ResearchConfig

my_config = ResearchConfig(
    topic="CRISPR gene editing, base editing, prime editing",
    max_sources=8,
    model="claude-haiku-4-5",   # $1/$5 per MTok — default throughout tutorial
    # model="claude-sonnet-4-6", # upgrade if synthesis depth is insufficient
    max_budget_usd=0.75,
    recency_days=60,
)

# Then override the system prompt in agent.py's build_system_prompt()
# to include domain-specific instructions
```

---

## What You've Learned

By completing this tutorial series, you can now:

**Conceptual:**
- Explain the 4 core concepts: Agent, Environment, Session, Events
- Describe when to use Managed Agents vs the Messages API
- Explain what happens inside the agent loop turn by turn
- Understand context window accumulation and automatic compaction

**Practical:**
- Install and configure the Agent SDK
- Write `query()` calls with proper `async for` handling
- Configure `ClaudeAgentOptions` for different use cases
- Use all major built-in tools: Bash, Read, Edit, WebSearch, WebFetch
- Set appropriate permission modes for different security contexts
- Control costs with `max_budget_usd` and `max_turns`
- Handle all `ResultMessage` subtypes including errors
- Build multi-turn sessions with `ClaudeSDKClient`
- Structure system prompts for consistent agent behavior

**Production patterns:**
- Cost estimation before running
- Cost monitoring during execution
- Result validation
- Timeout and error handling
- Parallel agent orchestration

---

## What to Learn Next

You now have the foundation. When you're ready to go deeper:

1. **Hooks** (`/agent-sdk/hooks`) — intercept and modify tool calls before they execute. Critical for logging, auditing, and custom guardrails.

2. **MCP Servers** (`/agent-sdk/mcp`) — connect agents to GitHub, Slack, databases, browsers. This is where agents become genuinely powerful for enterprise workflows.

3. **Subagents** (`/agent-sdk/subagents`) — spawn child agents to handle parallel subtasks. Each subagent gets a fresh context. The parent gets a summary result. This is how you scale beyond a single agent's context window.

4. **Skills** (`/agents-and-tools/agent-skills/overview`) — package reusable agent capabilities as files. Your SKILL.md system is already a production-ready implementation of this concept.

5. **Structured Outputs in SDK** (`/agent-sdk/structured-outputs`) — force Claude to return JSON matching a schema. Essential for agents that feed data into downstream systems.

6. **Session Resume and Fork** (`/agent-sdk/sessions`) — pick up long-running tasks where they left off, or branch into alternative execution paths.

---

## Quick Reference Card

```python
# Minimal working agent — copy-paste and run
import asyncio
from dotenv import load_dotenv
from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage

load_dotenv()

async def run(prompt: str) -> str | None:
    async for msg in query(
        prompt=prompt,
        options=ClaudeAgentOptions(
            allowed_tools=["Read", "Glob", "WebSearch", "WebFetch", "Write"],
            permission_mode="dontAsk",
            model="claude-haiku-4-5",   # DEFAULT: $1/$5 per MTok in/out
            # model="claude-sonnet-4-6", # upgrade: $3/$15 per MTok — better reasoning
            # model="claude-opus-4-6",   # premium: $5/$25 per MTok — best reasoning
            effort="high",
            max_turns=20,
            max_budget_usd=0.25,        # Haiku budget — adjust upward for Sonnet/Opus
        ),
    ):
        if isinstance(msg, ResultMessage):
            return msg.result if msg.subtype == "success" else None

asyncio.run(run("Your task here"))
```

Save this. It's 90% of what you'll ever need.

---

*Tutorial complete. You are now a Claude Managed Agents practitioner.*

*Sources: platform.claude.com/docs/en/managed-agents/overview,*
*platform.claude.com/docs/en/agent-sdk/quickstart,*
*platform.claude.com/docs/en/agent-sdk/agent-loop,*
*platform.claude.com/docs/en/agent-sdk/python — April 8, 2026*
