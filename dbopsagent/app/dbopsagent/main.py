"""AgentCore entry point for Database Operations Agent."""
from bedrock_agentcore.runtime import BedrockAgentCoreApp
import asyncio, json, os, sys, time
from pathlib import Path
from mcp import StdioServerParameters, stdio_client
from strands import Agent
from strands.hooks import (
    HookProvider, HookRegistry, BeforeModelCallEvent,
    BeforeToolCallEvent, AfterToolCallEvent,
)
from strands.models.bedrock import BedrockModel
from strands.tools.mcp import MCPClient

app = BedrockAgentCoreApp()
log = app.logger


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
    """Log tool calls to CloudWatch."""

    def register_hooks(self, registry: HookRegistry) -> None:
        registry.add_callback(BeforeToolCallEvent, self._before)
        registry.add_callback(AfterToolCallEvent, self._after)

    def _before(self, event: BeforeToolCallEvent) -> None:
        name = event.tool_use["name"]
        args = json.dumps(event.tool_use["input"], default=str)
        if len(args) > 500:
            args = args[:500] + "..."
        log.info(f"[ToolCall] {name} | args: {args}")

    def _after(self, event: AfterToolCallEvent) -> None:
        name = event.tool_use["name"]
        if event.exception:
            log.error(f"[ToolCall] {name} failed: {event.exception}")
        else:
            log.info(f"[ToolCall] {name} → {event.result.get('status', 'unknown')}")


AGENT_DIR = Path(__file__).parent
CATALOG_DIR = AGENT_DIR / "catalog"
MCP_SERVER = AGENT_DIR / "mcp_server.py"

# Base system prompt (same as local CLI agent)
BASE_SYSTEM_PROMPT = """You are a database operations assistant.

## Tools
Use `list_catalog_tools` to see available SQL tools. Use `run_catalog_tool` to execute them.

You cannot run arbitrary SQL. Only catalog tools are available. If a needed tool does not exist, state what is missing.

## Rules
- Be concise. Answer only what was asked. Do not over-explain.
- No emojis.
- Identify the correct database type and target before running tools.
- Do not guess tool names. Call `list_catalog_tools` first if you are unsure of exact names. Only invoke tools that exist in the catalog with their correct definitions.
- Use the minimum number of tool calls needed to answer the question. Start with the most relevant tool and only call additional tools if the results are insufficient.
- Present results clearly.
"""

# Auto-mode addendum: the deployed agent acts autonomously (no follow-up questions)
AUTO_MODE_ADDENDUM = f"""
## Auto Mode (Cloud Deployment)
You are running as an autonomous background agent. Prompts come from automated systems
(ticketing, alerting, ChatOps) — there is no human in the loop for follow-up questions.

Rules:
- NEVER ask follow-up questions. Act on the information provided in the prompt.
- If critical information is missing, respond with: "Insufficient context: [what is missing]"
- Use the database targets below unless the prompt specifies otherwise.

## Database Targets
- SQL Server: target={os.environ.get('MSSQL_HOST', 'NOT_CONFIGURED')}, database={os.environ.get('MSSQL_DATABASE', 'CRM')} (customer data)
- PostgreSQL: target={os.environ.get('AURORA_CLUSTER_ENDPOINT', 'NOT_CONFIGURED')}, database=workshop, schema=hr (employee and project data)
"""

SYSTEM_PROMPT = BASE_SYSTEM_PROMPT + AUTO_MODE_ADDENDUM


def create_mcp_client():
    return MCPClient(lambda: stdio_client(
        StdioServerParameters(command=sys.executable, args=[str(MCP_SERVER)],
                              env={**os.environ, "CATALOG_DIR": str(CATALOG_DIR)})
    ))


@app.entrypoint
def handler(payload, context):
    user_input = payload.get("prompt", "Hello!")
    log.info(f"Received prompt: {user_input}")
    model = BedrockModel(
        model_id=os.environ.get("MODEL_ID", "global.anthropic.claude-sonnet-4-6"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )
    mcp_client = create_mcp_client()
    with mcp_client:
        tools = mcp_client.list_tools_sync()
        agent = Agent(model=model, system_prompt=SYSTEM_PROMPT, tools=tools, hooks=[ThrottleHook(1.2), ToolCallLoggerHook()])
        result = agent(user_input)
        for block in result.message.get("content", []):
            if "text" in block:
                return {"result": block["text"]}
    return {"result": "No response generated."}


if __name__ == "__main__":
    app.run()