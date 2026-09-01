"""
FastAPI Natural Language Query Agent
Translates natural language questions to SQL, queries the data marts warehouse,
and returns structured data alongside the generated SQL.
"""

import os
import sys
import time
import decimal
import datetime
from typing import Any, List, Dict, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from schema_context import get_schema_context_prompt, MART_SCHEMA_DDL
from llm_client import TextToSQLClient

app = FastAPI(
    title="Praxis-Data NL Query Agent",
    description="Natural Language Text-to-SQL interface querying the Praxis-Data Warehouse Data Marts.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize LLM Client
llm_client = TextToSQLClient(provider=os.getenv("LLM_PROVIDER", "auto"))


class QueryRequest(BaseModel):
    query: str = Field(..., description="Natural language analytical question", example="What are the total sales by product category?")


class QueryResponse(BaseModel):
    question: str
    generated_sql: str
    provider: str
    execution_time_ms: float
    row_count: int
    results: List[Dict[str, Any]]
    explanation: Optional[str] = None


def get_db_connection():
    """Create a connection to the PostgreSQL Warehouse."""
    return psycopg2.connect(
        host=os.getenv("DBT_HOST", os.getenv("POSTGRES_HOST", "localhost")),
        port=int(os.getenv("DBT_PORT", os.getenv("POSTGRES_PORT", "5432"))),
        dbname=os.getenv("DBT_DATABASE", os.getenv("WAREHOUSE_DB", "warehouse")),
        user=os.getenv("DBT_USER", os.getenv("POSTGRES_USER", "postgres")),
        password=os.getenv("DBT_PASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres")),
        connect_timeout=5
    )


def serialize_value(v: Any) -> Any:
    """Helper to convert date, datetime, and Decimal values into JSON-serializable types."""
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    return v


@app.get("/", response_class=HTMLResponse)
def index_page():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Praxis-Data NL Query Agent</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 800px; margin: 40px auto; padding: 20px; line-height: 1.6; background: #0f172a; color: #f8fafc; }
            h1 { color: #38bdf8; }
            a { color: #38bdf8; text-decoration: none; }
            a:hover { text-decoration: underline; }
            .card { background: #1e293b; padding: 20px; border-radius: 8px; margin-bottom: 20px; border: 1px solid #334155; }
            code { background: #0f172a; padding: 2px 6px; border-radius: 4px; color: #f43f5e; }
        </style>
    </head>
    <body>
        <h1>Praxis-Data Natural Language Query Agent</h1>
        <div class="card">
            <h3>Interactive API Documentation</h3>
            <p>Explore and test the API directly using Swagger UI: <a href="/docs"><b>/docs</b></a> or ReDoc: <a href="/redoc"><b>/redoc</b></a></p>
        </div>
        <div class="card">
            <h3>Available Endpoints</h3>
            <ul>
                <li><code>GET /health</code> — Check DB connectivity and mart table record counts.</li>
                <li><code>GET /schema</code> — View the data mart DDL and business rules.</li>
                <li><code>POST /query</code> — Send a natural language query and receive generated SQL + warehouse results.</li>
            </ul>
        </div>
    </body>
    </html>
    """


@app.get("/health")
def health_check():
    """Verify database connectivity and check table row counts across layers."""
    status = {
        "status": "healthy",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "llm_provider": llm_client.active_provider,
        "warehouse_tables": {}
    }
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            tables = [
                ("raw", "orders"),
                ("staging", "stg_orders"),
                ("analytics", "dim_customers"),
                ("analytics", "dim_products"),
                ("analytics", "fct_orders")
            ]
            for schema, tbl in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {schema}.{tbl};")
                    count = cur.fetchone()[0]
                    status["warehouse_tables"][f"{schema}.{tbl}"] = count
                except Exception:
                    status["warehouse_tables"][f"{schema}.{tbl}"] = "table_not_found"
                    conn.rollback()
        conn.close()
    except Exception as e:
        status["status"] = "degraded"
        status["error"] = str(e)
    return status


@app.get("/schema")
def get_schema():
    """Return the dimensional data mart schema definitions and business rules."""
    return {
        "schema_context": get_schema_context_prompt(),
        "ddl": MART_SCHEMA_DDL
    }


@app.post("/query", response_model=QueryResponse)
def query_warehouse(request: QueryRequest):
    """
    Translate a natural language question into SQL, execute it against the data marts,
    and return the generated SQL and results.
    """
    start_time = time.time()

    # 1. Generate SQL from question
    try:
        sql, provider = llm_client.generate_sql(request.query)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to generate SQL: {str(e)}")

    # 2. Execute SQL on PostgreSQL Warehouse (Read-Only)
    try:
        conn = get_db_connection()
        conn.set_session(readonly=True)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Set statement timeout to prevent runaway queries (5 seconds)
            cur.execute("SET statement_timeout = '5000ms';")
            cur.execute(sql)
            raw_results = cur.fetchall()
            
            # Serialize results
            serialized_results = []
            for row in raw_results:
                serialized_results.append({k: serialize_value(v) for k, v in dict(row).items()})
        conn.close()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to execute generated SQL against warehouse",
                "sql": sql,
                "db_message": str(e)
            }
        )

    exec_time_ms = round((time.time() - start_time) * 1000, 2)

    return QueryResponse(
        question=request.query,
        generated_sql=sql,
        provider=provider,
        execution_time_ms=exec_time_ms,
        row_count=len(serialized_results),
        results=serialized_results,
        explanation=f"Query executed successfully in {exec_time_ms}ms using {provider} provider. Returned {len(serialized_results)} rows."
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
