"""
Quick Start example from docs/README.md
Runs a Claude Managed Agent that searches the web for top AI papers.
"""
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


if __name__ == "__main__":
    asyncio.run(main())
