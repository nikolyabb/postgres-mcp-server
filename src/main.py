from fastmcp import FastMCP, Context
import pydantic
import json
from utils.db import get_connection
from utils.validation import check_query
from utils.logger import get_logger
from utils.tables_parser import parse_tables


log = get_logger()

# Defining MCP Server v2
mcp = FastMCP(
    name="Database Connector", 
    instructions="""
        This MCP server provides access to the data in the database. If {user} asks any data-related question it is a good idea to use this
        tool to check available tables and see if there might be any relevant information and then make a query. Don't worry, it is safe.
    """,   
)

# Resource for Database Status check
@mcp.resource(
    uri="data://status",
    name="DatabaseStatusCheck",
    description="Provides status about database and connectivity. Required if something doesn't work to provide additional info.",
    tags={"database", "status"}
)
def database_status(ctx: Context) -> pydantic.BaseModel:
    """
    Check the status of the PostgreSQL database.

    This function retrieves the PostgreSQL version and the current database name.
    It handles connection errors and returns appropriate error messages if any.

    Args:
        ctx (Context): The context object containing client_id and request_id.

    Returns:
        pydantic.BaseModel: A model containing the PostgreSQL version information.
                            If an error occurs, returns a model with an error message and exception details.
    """
    _query1 = "SELECT version();"
    _query2 = "SELECT current_database();"
    response = {
        "client_id": ctx.client_id,
        "request_id": ctx.request_id,
    }
    try:
        with get_connection() as con:
            with con.cursor() as cur:
                response["pg_database_name"] = cur.execute(_query2).fetchone()
                response["pg_version"] = cur.execute(_query1).fetchone()
                log.debug(pydantic.BaseModel(**response))
                return pydantic.BaseModel(**response)
    except Exception as e:
        err_response = {"message": "❌ Connection failed", "error": e}
        log.debug(pydantic.BaseModel(**err_response))
        return pydantic.BaseModel(**err_response)

# Resource for providing information on existing tables
@mcp.resource(
    uri="data://schema/tables/columns",
    name="DatabaseTables",
    description="Return all available tables from the database and information about their structure.",
    tags={"database", "tables", "schema", "columns", "metadata"}
)
def get_schema_tables(ctx: Context) -> pydantic.BaseModel:
    """
    Retrieve a list of all tables in the database.

    This function retrieves metadata for all user-defined tables from the
    information_schema.columns table. It excludes system schemas like pg_catalog and
    information_schema, and orders results by schema, table name, and column position.
    
    Args:
        ctx (Context): The context object containing client_id and request_id.

    Returns:
        pydantic.BaseModel: A model containing the list of tables with their columns,
                            data types, nullability, and maximum character length.
                            If an error occurs, returns a model with an error message
                            and exception details.
    """
    _query = """
        SELECT table_schema, table_name, column_name, data_type, is_nullable, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name, ordinal_position;
    """
    response = {
        "client_id": ctx.client_id,
        "request_id": ctx.request_id,
    }
    try:
        with get_connection() as con:
            with con.cursor() as cur:
                log.debug(f"Client {ctx.client_id} called get_schema_tables() with request {ctx.request_id} : {ctx.request_context}")
                cur.execute(_query)
                query_results = cur.fetchall()
                response = parse_tables(query_results)
                log.debug("Response:\n", pydantic.BaseModel(**response))
                return pydantic.BaseModel(**response)
    except Exception as e:
        err_response = {"message": "❌ Connection failed", "error": e}
        log.debug(pydantic.BaseModel(**err_response))
        return pydantic.BaseModel(**err_response)

@mcp.tool()
def execute_query(ctx: Context, query: str) -> pydantic.BaseModel:
    """
    Executes a read-only SQL query.

    This method executes any provided SQL query that is considered safe based on the
    check_query() function. The method retrieves and returns the results of the query,
    or an appropriate error message if execution fails.

    Args:
        ctx (Context): The context object containing client_id and request_id.
        query (str): The SQL query to be executed.

    Returns:
        pydantic.BaseModel: A model containing the results of the query execution.
                            If an error occurs, returns a model with an error message and exception details.

    Note:
        This method is designed to execute read-only queries for safety purposes. Only
        queries starting with certain allowed keywords (SELECT, SHOW, DESCRIBE, DESC,
        EXPLAIN) are permitted by the check_query function.
    """
    if check_query(query):
        try:
            with get_connection() as con:
                with con.cursor() as cur:
                    log.debug(f"Client {ctx.client_id} called execure_query() with request {ctx.request_id} : {ctx.request_context}")
                    cur.execute(query)
                    # Get column names from cursor description
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    results = [dict(zip(columns, row)) for row in rows]
                    response = json.dumps(results, indent=2, default=str) # default=str handles dates, decimals, etc.
                    log.debug("Response:\n", pydantic.BaseModel(**response))
                    return pydantic.BaseModel(**response)
        except Exception as e:
            err_response = {"message": "❌ Connection failed", "error": e}
            log.debug(pydantic.BaseModel(**err_response))
            return pydantic.BaseModel(**err_response)
    else:
        return {"message": "This query use non-safe first word", "error": True}
      
    
if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8030)
