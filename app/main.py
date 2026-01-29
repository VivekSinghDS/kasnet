import os 
import logging
from fastapi import FastAPI, Query, HTTPException
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor

from app.utils.constants import RECOMMENDATION_TYPES, format_prompt
from app.utils.groq_client import GroqRecommendationClient

load_dotenv()

logger = logging.getLogger(__name__)

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


@app.get("/analytics/recommendations")
def recommendations(
    terminal_id: str = Query(..., description="Terminal ID (required)"),
    recommendation_type: str = Query(
        ..., 
        description="Type: short (7d), medium (30d), or long (90d)"
    ),
) -> Dict[str, Any]:
    """
    Generate AI-powered business recommendations based on terminal analytics.
    
    This endpoint automatically:
    1. Determines the analysis period based on recommendation_type
    2. Gathers data from summary, group-by, hourly-distribution, and timeseries
    3. Sends aggregated data to Groq AI for analysis
    4. Returns structured, actionable recommendations
    
    Args:
        terminal_id: Terminal identifier to analyze
        recommendation_type: short (7d), medium (30d), or long (90d)
        
    Returns:
        Dictionary containing recommendations with priorities and metrics
        
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
            f"Generating {recommendation_type} recommendations for terminal {terminal_id}"
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
        
        # Gather all analytics data
        summary_data = summary(
            terminal_id=terminal_id, 
            start=start_str, 
            end=end_str
        )
        
        channel_data = group_by(
            dimension="channel",
            terminal_id=terminal_id,
            start=start_str,
            end=end_str
        )
        
        operation_data = group_by(
            dimension="operation",
            terminal_id=terminal_id,
            start=start_str,
            end=end_str
        )
        
        entity_data = group_by(
            dimension="entity",
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
        
        # Aggregate all data
        aggregated_data = {
            "terminal_id": terminal_id,
            "analysis_period": {
                "start": start_str,
                "end": end_str,
                "days": rec_config["period_days"]
            },
            "summary": summary_data,
            "distribution_by_channel": channel_data,
            "distribution_by_operation": operation_data,
            "distribution_by_entity": entity_data,
            "hourly_distribution": hourly_data,
            "daily_timeseries": timeseries_data
        }
        
        # Format prompt and call AI
        prompt = format_prompt(
            terminal_id=terminal_id,
            recommendation_type=recommendation_type,
            aggregated_data=aggregated_data
        )
        
        groq_client = GroqRecommendationClient()
        recommendations_result = groq_client.generate_recommendations(prompt)
        
        # Return enriched response
        return {
            "terminal_id": terminal_id,
            "recommendation_type": recommendation_type,
            "analysis_period": aggregated_data["analysis_period"],
            "generated_at": datetime.now().isoformat(),
            **recommendations_result
        }
        
    except HTTPException:
        # Re-raise HTTP exceptions (e.g., 404 from underlying endpoints)
        raise
    except Exception as e:
        logger.error(f"Error generating recommendations: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate recommendations: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)