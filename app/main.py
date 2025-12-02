import os 
from fastapi import FastAPI, Query, HTTPException
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

app = FastAPI(title="Analytics API (POC)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can restrict this to specific domains in production
    allow_credentials=True,
    allow_methods=["*"],  # or specify: ["GET", "POST", "PUT", "DELETE"]
    allow_headers=["*"],  # or specify: ["Content-Type", "Authorization"]
)

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    # 'sslmode': 'require'
}

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

def build_filter_conditions(
    terminal_id: Optional[str],
    start: Optional[str],
    end: Optional[str]
) -> tuple[str, list]:
    """Build WHERE clause and parameters for SQL queries"""
    conditions = []
    params = []
    
    if terminal_id:
        conditions.append("terminal_id = %s")
        params.append(int(terminal_id))
    
    if start and end:
        conditions.append("transaction_datetime >= %s AND transaction_datetime <= %s")
        params.extend([start, end])
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    return where_clause, params

@app.get("/")
def root():
    """Health check endpoint"""
    return {"status": "healthy", "service": "Analytics API"}

@app.get("/analytics/summary")
def summary(
    terminal_id: Optional[str] = Query(None, description="Filter by terminal ID"),
    start: Optional[str] = Query(None, description="Start datetime (YYYY-MM-DD HH:MM:SS)"),
    end: Optional[str] = Query(None, description="End datetime (YYYY-MM-DD HH:MM:SS)"),
) -> Dict[str, Any]:
    """
    Dashboard summary: total transactions, favorite operation, peak hour with growth metrics
    """
    where_clause, params = build_filter_conditions(terminal_id, start, end)
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Current period metrics
            current_query = f"""
            WITH metrics AS (
                SELECT 
                    COUNT(*) as total_txns,
                    SUM(cant_trx) as sum_txns,
                    MODE() WITHIN GROUP (ORDER BY operation) as fav_op,
                    MODE() WITHIN GROUP (ORDER BY hour) as peak_hour
                FROM transactions
                WHERE {where_clause}
            )
            SELECT * FROM metrics
            """
            
            cur.execute(current_query, params)
            current = cur.fetchone()
            
            if not current or current['total_txns'] == 0:
                raise HTTPException(status_code=404, detail="No data found for the filters provided")
            
            # Calculate growth if date range provided
            txn_growth = None
            hour_growth = None
            
            if start and end:
                start_dt = datetime.fromisoformat(start.replace(' ', 'T'))
                end_dt = datetime.fromisoformat(end.replace(' ', 'T'))
                period_days = (end_dt - start_dt).days
                
                prev_start = start_dt - timedelta(days=period_days)
                prev_end = start_dt - timedelta(days=1)
                
                prev_where = where_clause.replace(
                    "transaction_datetime >= %s AND transaction_datetime <= %s",
                    "transaction_datetime >= %s AND transaction_datetime <= %s"
                )
                prev_params = params.copy()
                
                # Replace date params
                if start and end:
                    prev_params[-2] = prev_start.isoformat()
                    prev_params[-1] = prev_end.isoformat()
                
                prev_query = f"""
                SELECT 
                    COUNT(*) as total_txns,
                    MODE() WITHIN GROUP (ORDER BY hour) as peak_hour
                FROM transactions
                WHERE {prev_where}
                """
                
                cur.execute(prev_query, prev_params)
                prev = cur.fetchone()
                
                if prev and prev['total_txns'] > 0:
                    txn_growth = ((current['total_txns'] - prev['total_txns']) / prev['total_txns'] * 100)
                    
                    if prev['peak_hour'] and prev['peak_hour'] > 0:
                        hour_growth = ((current['peak_hour'] - prev['peak_hour']) / prev['peak_hour'] * 100)
            
            return {
                "total_transactions": {
                    "value": int(current['total_txns']),
                    "growth": round(txn_growth, 2) if txn_growth is not None else None
                },
                "favorite_operation": current['fav_op'],
                "peak_hour": {
                    "value": int(current['peak_hour']) if current['peak_hour'] is not None else None,
                    "growth": round(hour_growth, 2) if hour_growth is not None else None
                }
            }
            

@app.get("/analytics/group-by")
def group_by(
    dimension: str = Query(..., description="Group by: channel, operation, or entity"),
    terminal_id: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Group transactions by a specific dimension with aggregated metrics
    """
    allowed_dimensions = ["channel", "operation", "entity"]
    if dimension not in allowed_dimensions:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid dimension '{dimension}'. Choose from {allowed_dimensions}"
        )
    
    where_clause, params = build_filter_conditions(terminal_id, start, end)
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = f"""
            SELECT 
                COALESCE({dimension}, 'Unknown') as {dimension},
                SUM(cant_trx) as transactions,
                SUM(transaction_amount) as total_amount
            FROM transactions
            WHERE {where_clause}
            GROUP BY {dimension}
            ORDER BY transactions DESC
            """
            
            cur.execute(query, params)
            results = cur.fetchall()
            
            if not results:
                raise HTTPException(status_code=404, detail="No data found for the filters provided")
            
            return [dict(row) for row in results]


@app.get("/analytics/timeseries")
def timeseries(
    terminal_id: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Return transaction count and total amount aggregated by date
    """
    where_clause, params = build_filter_conditions(terminal_id, start, end)
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = f"""
            SELECT 
                transaction_date::text as date,
                SUM(cant_trx) as transactions,
                SUM(transaction_amount) as total_amount
            FROM transactions
            WHERE {where_clause}
            GROUP BY transaction_date
            ORDER BY transaction_date
            """
            
            cur.execute(query, params)
            results = cur.fetchall()
            
            if not results:
                raise HTTPException(status_code=404, detail="No data found for the filters provided")
            
            return [dict(row) for row in results]


@app.get("/analytics/hourly-distribution")
def hourly_distribution(
    terminal_id: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Get transaction distribution by hour of day
    """
    where_clause, params = build_filter_conditions(terminal_id, start, end)
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = f"""
            SELECT 
                hour,
                SUM(cant_trx) as transactions,
                SUM(transaction_amount) as total_amount
            FROM transactions
            WHERE {where_clause}
            GROUP BY hour
            ORDER BY hour
            """
            
            cur.execute(query, params)
            results = cur.fetchall()
            
            return [dict(row) for row in results]


@app.get("/analytics/terminals")
def list_terminals() -> List[int]:
    """
    Get list of all available terminal IDs
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT terminal_id FROM transactions ORDER BY terminal_id")
            results = cur.fetchall()
            return [row[0] for row in results if row[0] is not None]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)