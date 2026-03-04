import os 
import logging
import uuid
import json
from fastapi import FastAPI, Query, HTTPException
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from pydantic import BaseModel

from app.utils.constants import (
    RECOMMENDATION_TYPES, 
    generate_daily_digest
)

load_dotenv()

logger = logging.getLogger(__name__)


class StatusUpdate(BaseModel):
    """Request model for updating recommendation status"""
    terminal_id: int
    status: str
    username: str
    agent_id: Optional[str] = None
    date: Optional[str] = None


app = FastAPI(title="Analytics API (POC)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can restrict this to specific domains in production
    allow_credentials=True,
    allow_methods=["*"],  # or specify: ["GET", "POST", "PUT", "DELETE"]
    allow_headers=["*"],  # or specify: ["Content-Type", "Authorization"]
)

DB_CONFIG = {
    'host': 'postgres' if os.environ.get("MODE") == "docker" else os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
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


@app.post("/analytics/recommendations")
def recommendations(
    terminal_id: str = Query(..., description="Terminal ID (required)"),
    recommendation_type: str = Query(
        ..., 
        description="Type: short (7d), medium (30d), or long (90d)"
    ),
) -> Dict[str, Any]:
    """
    Generate and store daily digest recommendations based on terminal analytics.
    
    V1: Returns a simple 2-sentence daily digest about yesterday's performance.
    
    Args:
        terminal_id: Terminal identifier to analyze
        recommendation_type: short (7d), medium (30d), or long (90d)
        
    Returns:
        Dictionary containing agent_id, daily_digest with 2 sentences in English and Spanish
        
    Raises:
        HTTPException: If invalid parameters or data gathering fails
    """
    # Validate recommendation type
    if recommendation_type not in RECOMMENDATION_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid recommendation_type '{recommendation_type}'. "
                   f"Choose from: {list(RECOMMENDATION_TYPES.keys())}"
        )
    
    # Calculate date range based on recommendation type
    rec_config = RECOMMENDATION_TYPES[recommendation_type]
    end_date = datetime.now()
    start_date = end_date - timedelta(days=rec_config["period_days"])
    
    start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        logger.info(
            f"Generating {recommendation_type} daily digest for terminal {terminal_id}"
        )

        # Check data availability and adjust date range if needed
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        MIN(transaction_date) as min_date,
                        MAX(transaction_date) as max_date
                    FROM transactions
                    WHERE terminal_id = %s
                """, (int(terminal_id),))
                data_check = cur.fetchone()
                
                if data_check and data_check[0] and data_check[1]:
                    available_max = data_check[1]
                    requested_start = datetime.fromisoformat(start_str.replace(' ', 'T')).date()
                    
                    # If data is older than requested period, use most recent available data
                    if available_max < requested_start:
                        logger.warning(
                            f"No data in requested period. Using most recent {rec_config['period_days']} days "
                            f"of available data (ending {available_max})"
                        )
                        end_date = datetime.combine(available_max, datetime.max.time())
                        start_date = end_date - timedelta(days=rec_config["period_days"])
                        start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
                        end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
        
        # Gather required analytics data for daily digest
        summary_data = summary(
            terminal_id=terminal_id, 
            start=start_str, 
            end=end_str
        )
        hourly_data = hourly_distribution(
            terminal_id=terminal_id,
            start=start_str,
            end=end_str
        )
        timeseries_data = timeseries(
            terminal_id=terminal_id,
            start=start_str,
            end=end_str
        )
        # Generate AI-powered daily digest via OpenAI
        daily_digest = generate_daily_digest(
            timeseries_data=timeseries_data,
            summary_data=summary_data,
            hourly_data=hourly_data,
            terminal_id=terminal_id,
            recommendation_type=recommendation_type,
        )
        
        # Build response
        response_data = {
            "terminal_id": terminal_id,
            "recommendation_type": recommendation_type,
            "analysis_period": {
                "start": start_str,
                "end": end_str,
                "days": rec_config["period_days"]
            },
            "generated_at": datetime.now().isoformat(),
            "daily_digest": daily_digest
        }
        
        # Generate unique agent_id and store in database
        agent_id = str(uuid.uuid4())
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO agent_recommendations 
                    (agent_id, terminal_id, recommendation_type, recommendations, status, updated_by)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    agent_id,
                    int(terminal_id),
                    recommendation_type,
                    Json(response_data),
                    'generated',
                    'AI'
                ))
                conn.commit()
        
        logger.info(f"Stored recommendations with agent_id: {agent_id}")
        
        # Return response with agent_id
        response_data["agent_id"] = agent_id
        return response_data
        
    except HTTPException:
        # Re-raise HTTP exceptions (e.g., 404 from underlying endpoints)
        raise
    except Exception as e:
        logger.error(f"Error generating daily digest: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate daily digest: {str(e)}"
        )


@app.get("/analytics/recommendations/fetch")
def fetch_recommendations(
    terminal_id: str = Query(..., description="Terminal ID (required)"),
    date: Optional[str] = Query(None, description="Specific date (YYYY-MM-DD)"),
    last_n_days: Optional[int] = Query(None, description="Fetch from last N days")
) -> List[Dict[str, Any]]:
    """
    Fetch stored recommendations for a terminal by date or last N days.
    
    Args:
        terminal_id: Terminal identifier
        date: Optional specific date in YYYY-MM-DD format
        last_n_days: Optional number of days to fetch (default: 7)
        
    Returns:
        List of recommendation records ordered by created_at DESC
        
    Raises:
        HTTPException: If no recommendations found
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if date:
                # Fetch for specific date
                query = """
                    SELECT 
                        agent_id, terminal_id, recommendation_type, 
                        recommendations, status, created_at, updated_at, updated_by
                    FROM agent_recommendations
                    WHERE terminal_id = %s 
                    AND DATE(created_at) = %s
                    ORDER BY created_at DESC
                """
                cur.execute(query, (int(terminal_id), date))
            elif last_n_days:
                # Fetch for last N days
                query = """
                    SELECT 
                        agent_id, terminal_id, recommendation_type, 
                        recommendations, status, created_at, updated_at, updated_by
                    FROM agent_recommendations
                    WHERE terminal_id = %s 
                    AND created_at >= NOW() - INTERVAL '%s days'
                    ORDER BY created_at DESC
                """
                cur.execute(query, (int(terminal_id), last_n_days))
            else:
                # Default: last 7 days
                query = """
                    SELECT 
                        agent_id, terminal_id, recommendation_type, 
                        recommendations, status, created_at, updated_at, updated_by
                    FROM agent_recommendations
                    WHERE terminal_id = %s 
                    AND created_at >= NOW() - INTERVAL '7 days'
                    ORDER BY created_at DESC
                """
                cur.execute(query, (int(terminal_id),))
            
            results = cur.fetchall()
            
            if not results:
                raise HTTPException(
                    status_code=404, 
                    detail=f"No recommendations found for terminal {terminal_id}"
                )
            
            return [dict(row) for row in results]


@app.put("/analytics/recommendations/status")
def update_recommendation_status(
    update: StatusUpdate
) -> Dict[str, Any]:
    """
    Update recommendation status for a terminal.
    
    Resolution order (most specific → least specific):
    1. agent_id + terminal_id  — updates exactly one record
    2. terminal_id + date      — updates all records for that terminal on a given date
    3. terminal_id only        — updates all records for that terminal
    
    Args:
        update: StatusUpdate model containing terminal_id, status, username,
                and optional agent_id / date fields
        
    Returns:
        Dictionary with count of updated records and their details
        
    Raises:
        HTTPException: If no recommendations found or invalid status
    """
    valid_statuses = ['generated', 'fetched', 'retrieved', 'archived', 'deleted']
    if update.status not in valid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status '{update.status}'. Must be one of: {valid_statuses}"
        )
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if update.agent_id:
                # Most specific: target a single recommendation by agent_id + terminal_id
                query = """
                    UPDATE agent_recommendations
                    SET status = %s, 
                        updated_by = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE agent_id = %s
                    AND terminal_id = %s
                    RETURNING agent_id, terminal_id, created_at, status, updated_by, updated_at
                """
                cur.execute(query, (update.status, update.username, update.agent_id, update.terminal_id))
            elif update.date:
                # Update all records for terminal_id on a specific date
                query = """
                    UPDATE agent_recommendations
                    SET status = %s, 
                        updated_by = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE terminal_id = %s 
                    AND DATE(created_at) = %s
                    RETURNING agent_id, terminal_id, created_at, status, updated_by, updated_at
                """
                cur.execute(query, (update.status, update.username, update.terminal_id, update.date))
            else:
                # Fallback: update ALL recommendations for terminal_id
                query = """
                    UPDATE agent_recommendations
                    SET status = %s, 
                        updated_by = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE terminal_id = %s
                    RETURNING agent_id, terminal_id, created_at, status, updated_by, updated_at
                """
                cur.execute(query, (update.status, update.username, update.terminal_id))
            
            results = cur.fetchall()
            conn.commit()
            
            if not results:
                detail = f"No recommendations found for terminal_id {update.terminal_id}"
                if update.agent_id:
                    detail += f" with agent_id {update.agent_id}"
                elif update.date:
                    detail += f" on {update.date}"
                raise HTTPException(status_code=404, detail=detail)
            
            return {
                "updated_count": len(results),
                "updated_records": [dict(row) for row in results]
            }


@app.delete("/analytics/recommendations")
def delete_recommendations(
    last_n_days: int = Query(..., description="Delete recommendations older than N days")
) -> Dict[str, Any]:
    """
    Delete recommendations older than N days ago.
    
    Args:
        last_n_days: Delete recommendations with created_at older than N days
        
    Returns:
        Dictionary with count of deleted records and their agent_ids
        
    Raises:
        HTTPException: If last_n_days is invalid
    """
    if last_n_days < 1:
        raise HTTPException(
            status_code=400,
            detail="last_n_days must be at least 1"
        )
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Delete recommendations older than N days
            cur.execute("""
                DELETE FROM agent_recommendations
                WHERE created_at < NOW() - INTERVAL '%s days'
                RETURNING agent_id
            """, (last_n_days,))
            
            deleted_ids = [row[0] for row in cur.fetchall()]
            conn.commit()
            
            return {
                "deleted_count": len(deleted_ids),
                "deleted_agent_ids": deleted_ids,
                "message": f"Deleted {len(deleted_ids)} recommendations older than {last_n_days} days"
            }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)