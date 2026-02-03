import json
from typing import Dict, Any
from decimal import Decimal

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

