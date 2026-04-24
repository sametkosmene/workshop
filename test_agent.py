"""Non-interactive agent test — sends a single prompt and prints the response."""
import os, sys
from pathlib import Path
from mcp import StdioServerParameters, stdio_client
from strands import Agent
from strands.models.bedrock import BedrockModel
from strands.tools.mcp import MCPClient

AGENT_DIR = Path(__file__).parent
CATALOG_DIR = AGENT_DIR / "catalog"
MCP_SERVER = AGENT_DIR / "mcp_server.py"

prompt = sys.argv[1] if len(sys.argv) > 1 else "List all available catalog tools"

model = BedrockModel(
    model_id=os.environ.get("MODEL_ID", "us.anthropic.claude-sonnet-4-20250514-v1:0"),
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

agent = Agent(model=model, system_prompt="You are a database operations assistant. Use catalog tools to help users.", tools=[mcp_client])
response = agent(prompt)
if hasattr(response, "message") and response.message:
    for block in response.message.get("content", []):
        if "text" in block:
            print(block["text"])
mcp_client.stop(None, None, None)
