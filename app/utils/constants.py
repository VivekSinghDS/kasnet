import json
from typing import Dict, Any, List
from decimal import Decimal
from datetime import datetime, timedelta
from statistics import mean, stdev

# Data dictionary and column definitions
DATA_DICTIONARY = {
    "terminal_id": "Unique identifier for each ATM/banking terminal",
    "operation": "Type of transaction (e.g., Withdrawal, Deposit, Transfer, Balance Inquiry)",
    "channel": "Transaction channel (e.g., ATM, Mobile, Web, POS)",
    "entity": "Business entity or branch associated with the terminal",
    "hour": "Hour of day when transaction occurred (0-23)",
    "cant_trx": "Count/number of transactions",
    "transaction_amount": "Total monetary value of transactions",
    "transaction_datetime": "Timestamp of transaction",
    "transaction_date": "Date of transaction (without time)"
}

ENDPOINT_DESCRIPTIONS = {
    "summary": "Provides high-level KPIs including total transactions, favorite operation (most common), and peak hour of activity. Includes growth percentages compared to previous period.",
    "group-by": "Aggregates transactions by a dimension (channel/operation/entity) showing transaction counts and total amounts for each category.",
    "hourly-distribution": "Shows how transactions are distributed across 24 hours of the day, useful for identifying peak times and gaps.",
    "timeseries": "Daily time series showing transaction trends over time, useful for spotting growth patterns or declines."
}

RECOMMENDATION_TYPES = {
    "short": {
        "period_days": 7,
        "description": "Immediate tactical actions for the next 1-2 weeks based on recent patterns",
        "max_recommendations": 3
    },
    "medium": {
        "period_days": 30,
        "description": "Strategic initiatives for the next 1-3 months to improve performance",
        "max_recommendations": 5
    },
    "long": {
        "period_days": 90,
        "description": "Long-term strategic changes for the next 3-6 months based on sustained patterns",
        "max_recommendations": 7
    }
}

RECOMMENDATION_PROMPT = """You are an expert business analyst for a banking/ATM terminal network. Your goal is to analyze transaction data and provide actionable, data-driven recommendations to help terminal operators grow their business.

# DATA DICTIONARY
{data_dictionary}

# AVAILABLE METRICS
You have been provided with the following analytics for terminal {terminal_id} over the last {period_days} days:

## Summary Metrics
- Total transactions count and growth %
- Most frequent operation type
- Peak hour of activity and its change

## Distribution by Channel
- Transaction volume and amounts by channel (ATM, Mobile, Web, etc.)

## Distribution by Operation
- Transaction volume and amounts by operation type (Withdrawal, Deposit, etc.)

## Distribution by Entity
- Transaction volume and amounts by business entity/branch

## Hourly Distribution
- Transaction patterns across 24 hours showing peak and low activity times

## Time Series
- Daily transaction trends showing growth, decline, or volatility patterns

# YOUR TASK
Analyze the provided data to identify {max_recommendations} HIGH-IMPACT opportunities for business growth or operational improvement.

# RECOMMENDATION CRITERIA
1. **Data-Driven**: Every recommendation MUST be based on specific patterns in the provided data
2. **Actionable**: Provide concrete actions that can be taken, not generic advice
3. **Measurable**: Include specific success metrics with target numbers
4. **Prioritized**: Use P0 (critical/urgent), P1 (important), P2 (beneficial) based on potential impact
5. **Contextual**: Consider the recommendation type: {recommendation_type}
   - {recommendation_description}
6. **Business-Focused**: Write recommendations for business operators, NOT technical staff. Avoid technical jargon like "API", "logs", "system architecture", "database queries", etc. Use simple, clear business language that any terminal operator can understand and act upon.

# FOCUS AREAS
- Identify performance gaps (e.g., underperforming hours, channels, or operations)
- Spot declining trends that need intervention
- Find growth opportunities in underutilized channels or operations
- Compare patterns to suggest optimal operation mix
- Identify operational issues (downtime, availability problems)
- Suggest rebalancing strategies for better performance

# OUTPUT FORMAT
Provide ONLY a valid JSON response with no additional text, markdown formatting, or explanations.

Structure:
{{
  "recommendations": {{
    "en": [
      {{
        "priority": "P0|P1|P2",
        "title": "Clear, action-oriented title (max 60 chars)",
        "rationale": "Data-backed explanation with specific numbers and comparisons",
        "data_driven_actions": [
          "Specific, measurable action 1",
          "Specific, measurable action 2",
          "Specific, measurable action 3"
        ],
        "success_metric": "Clear target with specific numbers and timeframe"
      }}
    ],
    "esp": [
      {{
        "priority": "P0|P1|P2",
        "title": "Título claro y orientado a la acción (máx 60 caracteres)",
        "rationale": "Explicación basada en datos con números específicos y comparaciones",
        "data_driven_actions": [
          "Acción específica y medible 1",
          "Acción específica y medible 2",
          "Acción específica y medible 3"
        ],
        "success_metric": "Objetivo claro con números específicos y plazo"
      }}
    ]
  }}
}}

# EXAMPLE OUTPUT
{{
  "recommendations": {{
    "en": [
      {{
        "priority": "P0",
        "title": "Boost Lunch Hour Transactions",
        "rationale": "48% fewer transactions during 12:00-14:00 compared to morning peak - this is prime time when customers need services most",
        "data_driven_actions": [
          "Check if the terminal is fully operational during lunch hours",
          "Verify all services (withdrawals, deposits, transfers) are available at this time",
          "Consider adding staff support during peak lunch demand"
        ],
        "success_metric": "Increase lunch hour transactions by 30% within 30 days"
      }}
    ],
    "esp": [
      {{
        "priority": "P0",
        "title": "Aumentar transacciones en hora de almuerzo",
        "rationale": "48% menos transacciones durante 12:00-14:00 comparado con el pico matutino - este es horario clave cuando los clientes más necesitan servicios",
        "data_driven_actions": [
          "Verificar que el terminal esté completamente operativo durante las horas de almuerzo",
          "Confirmar que todos los servicios (retiros, depósitos, transferencias) estén disponibles en este horario",
          "Considerar agregar personal de apoyo durante la demanda pico del almuerzo"
        ],
        "success_metric": "Aumentar transacciones en hora de almuerzo en 30% en 30 días"
      }}
    ]
  }}
}}

# DATA PROVIDED
{aggregated_data}

# IMPORTANT RULES
- Return ONLY valid JSON, no markdown code blocks or additional text
- Provide exactly {max_recommendations} recommendations (or fewer if insufficient patterns)
- Every metric mentioned must come from the provided data
- Avoid generic advice like "increase marketing" without data-specific context
- Focus on {recommendation_type}-term ({period_days} day analysis) actions aligned with: {recommendation_description}
- Keep language simple and non-technical. The audience is business operators, not IT staff. Instead of "analyze logs" say "check service availability". Instead of "system downtime" say "service interruptions".
"""


def _convert_decimals(obj: Any) -> Any:
    """
    Recursively convert Decimal objects to float for JSON serialization.
    
    Args:
        obj: Object that may contain Decimal values
        
    Returns:
        Object with Decimals converted to floats
    """
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {key: _convert_decimals(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_convert_decimals(item) for item in obj]
    return obj


def format_prompt(
    terminal_id: str, 
    recommendation_type: str, 
    aggregated_data: Dict[str, Any]
) -> str:
    """
    Format the recommendation prompt with actual data.
    
    Args:
        terminal_id: Terminal identifier
        recommendation_type: Type of recommendation (short/medium/long)
        aggregated_data: Dictionary containing all analytics data
        
    Returns:
        Formatted prompt string ready for AI model
    """
    rec_config = RECOMMENDATION_TYPES[recommendation_type]
    
    # Format data dictionary
    data_dict_str = "\n".join([
        f"- **{key}**: {value}" 
        for key, value in DATA_DICTIONARY.items()
    ])
    
    # Convert Decimals to floats and format aggregated data as readable JSON
    aggregated_data_clean = _convert_decimals(aggregated_data)
    aggregated_data_str = json.dumps(aggregated_data_clean, indent=2)
    
    return RECOMMENDATION_PROMPT.format(
        data_dictionary=data_dict_str,
        terminal_id=terminal_id,
        period_days=rec_config["period_days"],
        max_recommendations=rec_config["max_recommendations"],
        recommendation_type=recommendation_type,
        recommendation_description=rec_config["description"],
        aggregated_data=aggregated_data_str
    )


def generate_daily_digest(
    timeseries_data: List[Dict[str, Any]],
    summary_data: Dict[str, Any],
    hourly_data: List[Dict[str, Any]]
) -> Dict[str, List[str]]:
    """
    Generate a simple 2-sentence daily digest about yesterday's performance.
    
    Returns:
        Dictionary with 'en' and 'esp' keys containing 2 digest sentences each
    """
    if not timeseries_data or len(timeseries_data) < 2:
        return {
            "en": ["Insufficient data for daily digest."],
            "esp": ["Datos insuficientes para el resumen diario."]
        }
    
    # Get yesterday's data (most recent) and calculate averages
    yesterday = timeseries_data[-1]
    all_transactions = [float(day.get('transactions', 0)) for day in timeseries_data]
    avg_daily_txns = mean(all_transactions) if all_transactions else 0
    yesterday_txns = float(yesterday.get('transactions', 0))
    
    # Calculate variance from average
    if avg_daily_txns > 0:
        pct_diff = ((yesterday_txns - avg_daily_txns) / avg_daily_txns) * 100
        trend = "above" if pct_diff >= 0 else "below"
        trend_esp = "por encima del" if pct_diff >= 0 else "por debajo del"
    else:
        pct_diff = 0
        trend = "at"
        trend_esp = "igual al"
    
    # Find peak hour
    peak_hour = summary_data.get('peak_hour', {}).get('value', 12)
    
    # Build 2-sentence digest with natural language
    digest_en = [
        f"Yesterday recorded {int(yesterday_txns):,} transactions, which is {abs(pct_diff):.1f}% {trend} the daily average of {int(avg_daily_txns):,}.",
        f"Peak activity occurred at {peak_hour}:00 hours; staffing and service availability should be optimized around this time."
    ]
    
    digest_esp = [
        f"Ayer se registraron {int(yesterday_txns):,} transacciones, un {abs(pct_diff):.1f}% {trend_esp} promedio diario de {int(avg_daily_txns):,}.",
        f"La actividad pico ocurrió a las {peak_hour}:00 horas; se recomienda optimizar el personal y los servicios en este horario."
    ]
    
    return {"en": digest_en, "esp": digest_esp}


def generate_monthly_projections(
    timeseries_data: List[Dict[str, Any]],
    analysis_period: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate basic monthly projections by extrapolating from current patterns.
    
    Returns:
        Dictionary with projections for current month based on past patterns
    """
    if not timeseries_data or len(timeseries_data) < 3:
        return {
            "status": "insufficient_data",
            "message": {
                "en": "Need at least 3 days of data for projections",
                "esp": "Se necesitan al menos 3 días de datos para proyecciones"
            }
        }
    
    # Calculate daily metrics
    daily_txns = [float(day.get('transactions', 0)) for day in timeseries_data]
    daily_amounts = [float(day.get('total_amount', 0)) for day in timeseries_data]
    
    avg_daily_txns = mean(daily_txns)
    avg_daily_amount = mean(daily_amounts)
    
    # Calculate trend (simple linear: compare first half vs second half)
    mid_point = len(daily_txns) // 2
    first_half_avg = mean(daily_txns[:mid_point]) if mid_point > 0 else avg_daily_txns
    second_half_avg = mean(daily_txns[mid_point:]) if mid_point > 0 else avg_daily_txns
    
    if first_half_avg > 0:
        trend_pct = ((second_half_avg - first_half_avg) / first_half_avg) * 100
    else:
        trend_pct = 0
    
    trend_direction = "growing" if trend_pct > 2 else ("declining" if trend_pct < -2 else "stable")
    trend_direction_esp = "creciente" if trend_pct > 2 else ("decreciente" if trend_pct < -2 else "estable")
    
    # Project to end of month (30 days)
    days_in_month = 30
    days_analyzed = len(daily_txns)
    
    # Apply trend adjustment for projection
    trend_multiplier = 1 + (trend_pct / 100) * 0.5  # Dampened trend
    projected_monthly_txns = avg_daily_txns * days_in_month * trend_multiplier
    projected_monthly_amount = avg_daily_amount * days_in_month * trend_multiplier
    
    # Calculate current month actuals so far
    current_total_txns = sum(daily_txns)
    current_total_amount = sum(daily_amounts)
    
    return {
        "status": "ok",
        "current_period": {
            "days_analyzed": days_analyzed,
            "total_transactions": int(current_total_txns),
            "total_amount": round(current_total_amount, 2),
            "avg_daily_transactions": round(avg_daily_txns, 1),
            "avg_daily_amount": round(avg_daily_amount, 2)
        },
        "monthly_projection": {
            "projected_transactions": int(projected_monthly_txns),
            "projected_amount": round(projected_monthly_amount, 2),
            "trend": {
                "direction": {"en": trend_direction, "esp": trend_direction_esp},
                "percentage": round(trend_pct, 1)
            }
        },
        "insight": {
            "en": f"Based on {days_analyzed} days of data, transactions are {trend_direction} ({trend_pct:+.1f}%). "
                  f"Projected monthly volume: {int(projected_monthly_txns):,} transactions.",
            "esp": f"Basado en {days_analyzed} días de datos, las transacciones están {trend_direction_esp} ({trend_pct:+.1f}%). "
                   f"Volumen mensual proyectado: {int(projected_monthly_txns):,} transacciones."
        }
    }


def generate_smart_alerts(
    timeseries_data: List[Dict[str, Any]],
    hourly_data: List[Dict[str, Any]],
    channel_data: List[Dict[str, Any]],
    operation_data: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Generate smart real-time alerts based on threshold analysis.
    Identifies top 3 good (above average) and bad (below average) highlights.
    
    Returns:
        Dictionary with 'good' and 'bad' alerts (up to 3 each)
    """
    good_alerts = []
    bad_alerts = []
    
    # === Analyze Daily Trends ===
    if timeseries_data and len(timeseries_data) >= 3:
        daily_txns = [float(day.get('transactions', 0)) for day in timeseries_data]
        avg_daily = mean(daily_txns)
        
        if len(daily_txns) >= 2:
            try:
                std_daily = stdev(daily_txns)
            except:
                std_daily = 0
            
            # Check recent day vs average
            recent_day = daily_txns[-1]
            if avg_daily > 0:
                recent_pct = ((recent_day - avg_daily) / avg_daily) * 100
                
                if recent_pct > 15:  # Significantly above average
                    good_alerts.append({
                        "metric": "daily_transactions",
                        "value": recent_day,
                        "threshold": avg_daily,
                        "deviation_pct": round(recent_pct, 1),
                        "message": {
                            "en": f"Yesterday's transactions ({int(recent_day):,}) were {recent_pct:.1f}% above your average ({int(avg_daily):,})",
                            "esp": f"Las transacciones de ayer ({int(recent_day):,}) estuvieron {recent_pct:.1f}% por encima de su promedio ({int(avg_daily):,})"
                        }
                    })
                elif recent_pct < -15:  # Significantly below average
                    bad_alerts.append({
                        "metric": "daily_transactions",
                        "value": recent_day,
                        "threshold": avg_daily,
                        "deviation_pct": round(recent_pct, 1),
                        "message": {
                            "en": f"Yesterday's transactions ({int(recent_day):,}) were {abs(recent_pct):.1f}% below your average ({int(avg_daily):,})",
                            "esp": f"Las transacciones de ayer ({int(recent_day):,}) estuvieron {abs(recent_pct):.1f}% por debajo de su promedio ({int(avg_daily):,})"
                        }
                    })
            
            # Check for consecutive growth or decline
            if len(daily_txns) >= 3:
                last_3 = daily_txns[-3:]
                if last_3[0] < last_3[1] < last_3[2]:  # Consecutive growth
                    growth_rate = ((last_3[2] - last_3[0]) / last_3[0] * 100) if last_3[0] > 0 else 0
                    if growth_rate > 10:
                        good_alerts.append({
                            "metric": "growth_streak",
                            "value": 3,
                            "threshold": None,
                            "deviation_pct": round(growth_rate, 1),
                            "message": {
                                "en": f"3-day growth streak detected — transactions up {growth_rate:.1f}% over this period",
                                "esp": f"Racha de crecimiento de 3 días detectada — transacciones aumentaron {growth_rate:.1f}% en este período"
                            }
                        })
                elif last_3[0] > last_3[1] > last_3[2]:  # Consecutive decline
                    decline_rate = ((last_3[0] - last_3[2]) / last_3[0] * 100) if last_3[0] > 0 else 0
                    if decline_rate > 10:
                        bad_alerts.append({
                            "metric": "decline_streak",
                            "value": 3,
                            "threshold": None,
                            "deviation_pct": round(-decline_rate, 1),
                            "message": {
                                "en": f"3-day decline detected — transactions down {decline_rate:.1f}% over this period",
                                "esp": f"Declive de 3 días detectado — transacciones bajaron {decline_rate:.1f}% en este período"
                            }
                        })
    
    # === Analyze Hourly Patterns ===
    if hourly_data and len(hourly_data) >= 6:
        hourly_txns = [(h.get('hour', 0), float(h.get('transactions', 0))) for h in hourly_data]
        txn_values = [t[1] for t in hourly_txns]
        avg_hourly = mean(txn_values)
        
        if avg_hourly > 0:
            # Find peak and low hours
            peak_hour = max(hourly_txns, key=lambda x: x[1])
            low_hour = min(hourly_txns, key=lambda x: x[1])
            
            peak_pct = ((peak_hour[1] - avg_hourly) / avg_hourly) * 100
            low_pct = ((low_hour[1] - avg_hourly) / avg_hourly) * 100
            
            if peak_pct > 50:  # Strong peak hour
                good_alerts.append({
                    "metric": "peak_hour",
                    "value": peak_hour[0],
                    "threshold": avg_hourly,
                    "deviation_pct": round(peak_pct, 1),
                    "message": {
                        "en": f"Strong peak at {peak_hour[0]}:00 with {peak_pct:.0f}% more transactions than average — capitalize on this window",
                        "esp": f"Pico fuerte a las {peak_hour[0]}:00 con {peak_pct:.0f}% más transacciones que el promedio — aproveche esta ventana"
                    }
                })
            
            if low_pct < -60:  # Very low activity period
                bad_alerts.append({
                    "metric": "low_hour",
                    "value": low_hour[0],
                    "threshold": avg_hourly,
                    "deviation_pct": round(low_pct, 1),
                    "message": {
                        "en": f"Very low activity at {low_hour[0]}:00 ({abs(low_pct):.0f}% below average) — potential service availability issue",
                        "esp": f"Actividad muy baja a las {low_hour[0]}:00 ({abs(low_pct):.0f}% por debajo del promedio) — posible problema de disponibilidad"
                    }
                })
    
    # === Analyze Channel Performance ===
    if channel_data and len(channel_data) >= 2:
        channel_txns = [float(c.get('transactions', 0)) for c in channel_data]
        total_channel_txns = sum(channel_txns)
        
        if total_channel_txns > 0:
            top_channel = channel_data[0]
            top_share = (float(top_channel.get('transactions', 0)) / total_channel_txns) * 100
            
            if top_share > 70:  # Dominant channel
                good_alerts.append({
                    "metric": "channel_dominance",
                    "value": top_channel.get('channel', 'Unknown'),
                    "threshold": None,
                    "deviation_pct": round(top_share, 1),
                    "message": {
                        "en": f"{top_channel.get('channel', 'Primary channel')} handles {top_share:.0f}% of all transactions — your main revenue driver",
                        "esp": f"{top_channel.get('channel', 'Canal principal')} maneja {top_share:.0f}% de todas las transacciones — su principal fuente de ingresos"
                    }
                })
            
            # Check for underperforming channels
            if len(channel_data) >= 2:
                lowest_channel = channel_data[-1]
                lowest_share = (float(lowest_channel.get('transactions', 0)) / total_channel_txns) * 100
                if lowest_share < 5 and lowest_share > 0:
                    bad_alerts.append({
                        "metric": "underperforming_channel",
                        "value": lowest_channel.get('channel', 'Unknown'),
                        "threshold": None,
                        "deviation_pct": round(lowest_share, 1),
                        "message": {
                            "en": f"{lowest_channel.get('channel', 'A channel')} has only {lowest_share:.1f}% share — consider promoting or investigating issues",
                            "esp": f"{lowest_channel.get('channel', 'Un canal')} tiene solo {lowest_share:.1f}% de participación — considere promover o investigar problemas"
                        }
                    })
    
    # Sort by absolute deviation and limit to top 3
    good_alerts.sort(key=lambda x: abs(x.get('deviation_pct', 0)), reverse=True)
    bad_alerts.sort(key=lambda x: abs(x.get('deviation_pct', 0)), reverse=True)
    
    return {
        "good": good_alerts[:3],
        "bad": bad_alerts[:3],
        "summary": {
            "en": f"Found {len(good_alerts[:3])} positive highlight(s) and {len(bad_alerts[:3])} area(s) needing attention",
            "esp": f"Se encontraron {len(good_alerts[:3])} punto(s) positivo(s) y {len(bad_alerts[:3])} área(s) que necesitan atención"
        }
    }

