"""Lightweight MCP SQL Catalog Server — reads YAML tool definitions from local filesystem."""
import os
import sys
import yaml
import json
import re
import logging
from pathlib import Path
from mcp.server.fastmcp import FastMCP

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)

CATALOG_DIR = os.environ.get("CATALOG_DIR", os.path.join(os.path.dirname(__file__), "catalog"))

mcp = FastMCP("SQL Catalog")


def parse_yaml_tool(filepath: str) -> dict | None:
    """Parse a YAML tool file with front-matter + SQL body separated by ---."""
    try:
        with open(filepath) as f:
            content = f.read()
        parts = content.split("\n---\n")
        if len(parts) < 2:
            logger.warning(f"Skipping {filepath}: no SQL body found after ---")
            return None
        meta = yaml.safe_load(parts[0])
        queries = [p.strip() for p in parts[1:] if p.strip()]
        meta["queries_sql"] = queries
        meta["filepath"] = filepath
        return meta
    except Exception as e:
        logger.warning(f"Error parsing {filepath}: {e}")
        return None


def substitute_params(sql: str, params: dict, param_defs: dict) -> str:
    """Replace {{param}} placeholders with validated values."""
    for name, value in params.items():
        defn = param_defs.get(name, {})
        ptype = defn.get("type", "string")
        if ptype == "string":
            # Basic SQL injection check
            if any(c in str(value) for c in [";", "--", "/*", "*/", "'", '"']):
                raise ValueError(f"Invalid characters in parameter '{name}'")
        sql = sql.replace(f"{{{{{name}}}}}", str(value))
    return sql


def load_catalog() -> list[dict]:
    """Load all YAML tool definitions from the catalog directory."""
    tools = []
    catalog_path = Path(CATALOG_DIR)
    if not catalog_path.exists():
        logger.warning(f"Catalog directory not found: {CATALOG_DIR}")
        return tools
    for yaml_file in sorted(catalog_path.rglob("*.yaml")):
        tool = parse_yaml_tool(str(yaml_file))
        if tool:
            tools.append(tool)
            logger.info(f"Loaded tool: {tool.get('db_type', 'unknown')}_{tool['name']}")
    return tools


# Load catalog at startup
catalog_tools = load_catalog()


@mcp.tool()
def list_catalog_tools(db_type: str = "") -> str:
    """List all available SQL catalog tools, optionally filtered by database type (mssql, postgres)."""
    filtered = catalog_tools if not db_type else [t for t in catalog_tools if t.get("db_type") == db_type]
    result = []
    for t in filtered:
        params_info = ""
        if t.get("params"):
            params_info = " | params: " + ", ".join(
                f"{k}({v.get('type', 'string')})" for k, v in t["params"].items()
            )
        result.append(f"- {t['db_type']}_{t['name']}: {t['description']}{params_info}")
    return "\n".join(result) if result else "No tools found."


@mcp.tool()
def run_catalog_tool(tool_name: str, target: str, database: str = "", params_json: str = "{}") -> str:
    """Run a catalog tool by name against a target database.

    Args:
        tool_name: Tool name in format db_type_name (e.g. mssql_active_sessions)
        target: Connection target — for MSSQL: host IP, for Postgres: cluster endpoint
        database: Database name (uses tool default if empty)
        params_json: JSON string of additional parameters
    """
    # Find the tool
    matched = None
    for t in catalog_tools:
        full_name = f"{t['db_type']}_{t['name']}"
        if full_name == tool_name:
            matched = t
            break
    if not matched:
        return f"Error: Tool '{tool_name}' not found. Use list_catalog_tools to see available tools."

    db = database or matched.get("default_database", "")
    params = json.loads(params_json) if params_json else {}

    # Apply defaults for missing params
    for pname, pdef in (matched.get("params") or {}).items():
        if pname not in params and "default" in pdef:
            params[pname] = pdef["default"]

    # Execute queries
    results = []
    for i, sql in enumerate(matched["queries_sql"]):
        sql = substitute_params(sql, params, matched.get("params") or {})
        try:
            if matched["db_type"] == "mssql":
                result = _exec_mssql(target, db, sql)
            elif matched["db_type"] == "postgres":
                result = _exec_postgres(target, db, sql)
            else:
                result = f"Unsupported db_type: {matched['db_type']}"
            results.append(result)
        except Exception as e:
            results.append(f"Error executing query {i+1}: {e}")

    return "\n---\n".join(results)


def execute_sql(db_type: str, target: str, database: str, sql: str) -> str:
    """Execute a custom SQL query directly against a database. Internal use only — not exposed as an MCP tool.

    Args:
        db_type: Database type — 'mssql' or 'postgres'
        target: Connection target — for MSSQL: host IP, for Postgres: cluster endpoint
        database: Database name
        sql: SQL query to execute
    """
    try:
        if db_type == "mssql":
            return _exec_mssql(target, database, sql)
        elif db_type == "postgres":
            return _exec_postgres(target, database, sql)
        else:
            return f"Unsupported db_type: {db_type}"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
def reload_catalog() -> str:
    """Reload the SQL catalog from the filesystem. Use after adding new YAML tool files."""
    global catalog_tools
    catalog_tools = load_catalog()
    return f"Catalog reloaded. {len(catalog_tools)} tools available."


def _exec_mssql(host: str, database: str, sql: str) -> str:
    """Execute SQL against SQL Server using pymssql."""
    import pymssql
    import boto3

    # Get credentials from Secrets Manager
    region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    sm = boto3.client("secretsmanager", region_name=region)
    # Find the SQL Server secret by stack name pattern
    secrets = sm.list_secrets(Filters=[{"Key": "name", "Values": ["AdminPasswordSecret"]}])
    secret_name = None
    for s in secrets.get("SecretList", []):
        if "AdminPasswordSecret" in s["Name"]:
            secret_name = s["Name"]
            break
    if not secret_name:
        return "Error: Could not find SQL Server credentials in Secrets Manager"

    password = sm.get_secret_value(SecretId=secret_name)["SecretString"]

    conn = pymssql.connect(server=host, user="sa", password=password, database=database, autocommit=True)
    cursor = conn.cursor(as_dict=True)
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return "Query returned no results."
    return json.dumps(rows, default=str, indent=2)


def _exec_postgres(endpoint: str, database: str, sql: str) -> str:
    """Execute SQL against Aurora PostgreSQL using boto3 rds-data API."""
    import boto3

    region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    # Get credentials from Secrets Manager
    sm = boto3.client("secretsmanager", region_name=region)
    secrets = sm.list_secrets(Filters=[{"Key": "name", "Values": ["DBSecret"]}])
    secret_arn = None
    for s in secrets.get("SecretList", []):
        if "DBSecret" in s["Name"]:
            secret_arn = s["ARN"]
            break
    if not secret_arn:
        return "Error: Could not find Aurora PostgreSQL credentials in Secrets Manager"

    # Find the cluster ARN
    rds = boto3.client("rds", region_name=region)
    clusters = rds.describe_db_clusters()
    cluster_arn = None
    for c in clusters["DBClusters"]:
        if endpoint in c.get("Endpoint", ""):
            cluster_arn = c["DBClusterArn"]
            break
    if not cluster_arn:
        return f"Error: Could not find cluster for endpoint {endpoint}"

    rds_data = boto3.client("rds-data", region_name=region)
    response = rds_data.execute_statement(
        resourceArn=cluster_arn,
        secretArn=secret_arn,
        database=database,
        sql=sql,
    )

    # Format results
    if "records" not in response or not response["records"]:
        return "Query returned no results."

    columns = [col.get("label", col.get("name", f"col{i}")) for i, col in enumerate(response.get("columnMetadata", []))]
    rows = []
    for record in response["records"]:
        row = {}
        for i, field in enumerate(record):
            col_name = columns[i] if i < len(columns) else f"col{i}"
            # Extract value from the typed field
            val = next(iter(field.values()))
            row[col_name] = val
        rows.append(row)

    return json.dumps(rows, default=str, indent=2)


if __name__ == "__main__":
    mcp.run(transport="stdio")
