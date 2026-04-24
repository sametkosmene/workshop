"""Database Operations AI Agent — CLI interface with run and creator modes."""
import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

import boto3
from mcp import StdioServerParameters, stdio_client
from strands import Agent
from strands.hooks import (
    HookProvider, HookRegistry, BeforeModelCallEvent,
    BeforeToolCallEvent, AfterToolCallEvent,
)
from strands.models.bedrock import BedrockModel

# ANSI colors
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_CYAN = "\033[36m"
_DIM = "\033[2m"
_RED = "\033[31m"
_RESET = "\033[0m"


class ThrottleHook(HookProvider):
    """Enforce minimum delay between Bedrock model calls to stay under TPS limit."""

    def __init__(self, delay: float = 1.2):
        self._delay = delay
        self._last_call: float = 0.0

    def register_hooks(self, registry: HookRegistry) -> None:
        registry.add_callback(BeforeModelCallEvent, self._throttle)

    async def _throttle(self, event: BeforeModelCallEvent) -> None:
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._delay:
            await asyncio.sleep(self._delay - elapsed)
        self._last_call = time.monotonic()


class ToolCallLoggerHook(HookProvider):
    """Log tool calls with name and arguments, show in color on CLI."""

    def __init__(self):
        self._count = 0

    def register_hooks(self, registry: HookRegistry) -> None:
        registry.add_callback(BeforeToolCallEvent, self._before)
        registry.add_callback(AfterToolCallEvent, self._after)

    def _before(self, event: BeforeToolCallEvent) -> None:
        self._count += 1
        name = event.tool_use["name"]
        args = event.tool_use["input"]
        args_str = json.dumps(args, default=str)
        if len(args_str) > 200:
            args_str = args_str[:200] + "..."
        print(f"\n{_CYAN}⚙ Tool #{self._count}: {name}{_RESET}")
        print(f"{_DIM}  args: {args_str}{_RESET}")

    def _after(self, event: AfterToolCallEvent) -> None:
        name = event.tool_use["name"]
        if event.exception:
            print(f"{_RED}  ✗ {name} failed: {event.exception}{_RESET}")
        else:
            status = event.result.get("status", "unknown")
            print(f"{_DIM}  ✓ {name} → {status}{_RESET}")


class ColorCallbackHandler:
    """Color-coded CLI output: green for responses, dim for metadata."""

    def __call__(self, **kwargs):
        data = kwargs.get("data")
        if data:
            print(f"{_GREEN}{data}{_RESET}", end="", flush=True)
            return
        result = kwargs.get("result")
        if result:
            print(f"\n{_DIM}[done]{_RESET}")
            return
from strands.tools.mcp import MCPClient

AGENT_DIR = Path(__file__).parent
CATALOG_DIR = AGENT_DIR / "catalog"
MCP_SERVER = AGENT_DIR / "mcp_server.py"


def _get_db_targets() -> dict:
    """Look up database targets from CloudFormation outputs or env vars."""
    mssql_host = os.environ.get("MSSQL_HOST")
    aurora_endpoint = os.environ.get("AURORA_CLUSTER_ENDPOINT")
    if mssql_host and aurora_endpoint:
        return {"mssql_host": mssql_host, "aurora_endpoint": aurora_endpoint}
    try:
        region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        cfn = boto3.client("cloudformation", region_name=region)
        stacks = cfn.list_stacks(StackStatusFilter=["CREATE_COMPLETE", "UPDATE_COMPLETE"])
        for s in stacks["StackSummaries"]:
            outputs = {o["OutputKey"]: o["OutputValue"] for o in cfn.describe_stacks(StackName=s["StackName"])["Stacks"][0].get("Outputs", [])}
            if not mssql_host and "SQLServerPrivateIP" in outputs:
                mssql_host = outputs["SQLServerPrivateIP"]
            if not aurora_endpoint and "ClusterEndpoint" in outputs:
                aurora_endpoint = outputs["ClusterEndpoint"]
    except Exception:
        pass
    return {"mssql_host": mssql_host or "UNKNOWN", "aurora_endpoint": aurora_endpoint or "UNKNOWN"}

RUN_SYSTEM_PROMPT = """You are a database operations assistant.

## Tools
Use `list_catalog_tools` to see available SQL tools. Use `run_catalog_tool` to execute them.

You cannot run arbitrary SQL. Only catalog tools are available. If a needed tool does not exist, tell the user to switch to creator mode to build it as a YAML catalog tool.

## Database Targets
- SQL Server: target={mssql_host}, database="CRM" (customer data)
- PostgreSQL: target={aurora_endpoint}, database="workshop", schema="hr" (employee and project data)

## Rules
- Be concise. Answer only what was asked. Do not over-explain.
- No emojis.
- Identify the correct database type and target before running tools.
- Do not guess tool names. Call `list_catalog_tools` first if you are unsure of exact names. Only invoke tools that exist in the catalog with their correct definitions.
- Use the minimum number of tool calls needed to answer the question. Start with the most relevant tool and only call additional tools if the results are insufficient.
- Present results clearly.
"""

CREATOR_SYSTEM_PROMPT = """You are a database operations tool creator. You help users convert operational playbooks into reusable SQL catalog tools.

## Your Capabilities
You can read playbooks (markdown files) and create YAML tool definitions that the run-mode agent can use.

## YAML Tool Format
Each tool is a YAML file with front-matter metadata and SQL body separated by `---`:

```yaml
name: tool_name
description: What this tool does
db_type: mssql
default_database: master
params:
  param_name:
    type: string|integer|float|boolean
    description: What this parameter controls
    required: true|false
    default: default_value
---
SELECT ... SQL query with {{param_name}} placeholders ...
```

The `name` field must NOT include the db_type prefix — the system adds it automatically. For example, use `name: wait_statistics` (not `name: mssql_wait_statistics`).

For multi-step tools, separate SQL blocks with `---`:
```yaml
queries:
  - step_one
  - step_two
---
SQL for step one
---
SQL for step two
```

## Workflow
1. Read the playbook the user provides
2. Identify SQL procedures that can become tools
3. Present a plan: which tools to create, what each does
4. Wait for user approval before creating files
5. Create YAML files in the catalog directory
6. Call `reload_catalog` to make them available
7. If you generated or significantly modified the SQL (not copied directly from a playbook), confirm with the user and test each tool with `run_catalog_tool`. If a tool fails, diagnose the error, fix the YAML, reload, and retest until it works

## Important
- Always present your plan and wait for user approval
- Create tools that are reusable and well-documented
- Use parameterized queries where values might change
- Place MSSQL tools in catalog/mssql/ and PostgreSQL tools in catalog/postgres/
- For SQL Server file output paths (e.g. Extended Events .xel files), use SERVERPROPERTY('ErrorLogFileName') to resolve the log directory dynamically via sp_executesql. Never hardcode paths like C:\\temp.

## Database Targets (for testing)
- SQL Server: target={mssql_host}, database="CRM"
- PostgreSQL: target={aurora_endpoint}, database="workshop"
"""


def create_agent(mode: str) -> tuple[MCPClient, Agent]:
    """Create an agent with the MCP catalog server."""
    model = BedrockModel(
        model_id=os.environ.get("MODEL_ID", "global.anthropic.claude-sonnet-4-6"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        max_tokens=4096,
    )

    mcp_client = MCPClient(
        lambda: stdio_client(
            StdioServerParameters(
                command=sys.executable,
                args=[str(MCP_SERVER)],
                env={**os.environ, "CATALOG_DIR": str(CATALOG_DIR)},
            )
        )
    )

    system_prompt = RUN_SYSTEM_PROMPT.format(**_get_db_targets()) if mode == "run" else CREATOR_SYSTEM_PROMPT.format(**_get_db_targets())

    # In creator mode, add file read/write tools
    extra_tools = []
    if mode == "creator":
        from strands import tool

        @tool
        def read_file(filepath: str) -> str:
            """Read a file from the filesystem. Use for reading playbooks."""
            p = Path(filepath).expanduser()
            if not p.exists():
                return f"Error: File not found: {filepath}"
            return p.read_text()

        @tool
        def write_catalog_file(db_type: str, filename: str, content: str) -> str:
            """Write a YAML tool definition to the catalog.

            Args:
                db_type: Database type — 'mssql' or 'postgres'
                filename: Filename (e.g. 'wait_statistics.yaml')
                content: Full YAML content including front-matter and SQL
            """
            target_dir = CATALOG_DIR / db_type
            target_dir.mkdir(parents=True, exist_ok=True)
            target_path = target_dir / filename
            target_path.write_text(content)
            return f"Created {target_path}"

        @tool
        def list_catalog_files() -> str:
            """List all YAML files in the catalog directory."""
            files = sorted(CATALOG_DIR.rglob("*.yaml"))
            return "\n".join(str(f.relative_to(CATALOG_DIR)) for f in files) or "No catalog files found."

        extra_tools = [read_file, write_catalog_file, list_catalog_files]

    return mcp_client, model, system_prompt, extra_tools


def main():
    parser = argparse.ArgumentParser(description="Database Operations AI Agent")
    parser.add_argument("--mode", choices=["run", "creator"], default="run", help="Agent mode")
    args = parser.parse_args()

    print(f"🤖 Database Ops Agent — {args.mode.upper()} mode")
    print(f"   Model: {os.environ.get('MODEL_ID', 'global.anthropic.claude-sonnet-4-6')}")
    print(f"   Catalog: {CATALOG_DIR}")
    print("   Type 'quit' or 'exit' to stop.\n")

    mcp_client, model, system_prompt, extra_tools = create_agent(args.mode)

    agent = Agent(
        model=model,
        system_prompt=system_prompt,
        tools=[mcp_client] + extra_tools,
        hooks=[ThrottleHook(1.2), ToolCallLoggerHook()],
        callback_handler=ColorCallbackHandler(),
    )

    while True:
        try:
            user_input = input("You: ").strip()
            if not user_input:
                continue
            if user_input.lower() in ("quit", "exit"):
                print("Goodbye!")
                break

            print(f"\n{_GREEN}Agent:{_RESET} ", end="", flush=True)
            response = agent(user_input)
            print()  # newline after streamed response

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"\nError: {e}\n")


if __name__ == "__main__":
    main()
