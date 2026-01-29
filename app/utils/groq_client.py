import os
import json
import logging
from typing import Dict, Any

from groq import Groq
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class GroqRecommendationClient:
    """Client for generating AI-powered recommendations using Groq API."""
    
    def __init__(self):
        """Initialize Groq client with API credentials."""
        self.api_key = os.getenv("GROQ_API_KEY")
        if not self.api_key:
            raise ValueError("GROQ_API_KEY not found in environment variables")
        
        try:
            # Initialize with minimal parameters to avoid compatibility issues
            self.client = Groq(api_key=self.api_key)
        except TypeError as e:
            # Fallback for older SDK versions
            logger.error(f"Groq client initialization error: {e}")
            raise RuntimeError(f"Failed to initialize Groq client. Please check SDK version. Error: {e}")
        
        self.model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
        
    def generate_recommendations(
        self, 
        prompt: str,
        temperature: float = 0.3,
        max_tokens: int = 4096
    ) -> Dict[str, Any]:
        """
        Call Groq API to generate business recommendations.
        
        Args:
            prompt: Formatted prompt with data and instructions
            temperature: Sampling temperature (0-1). Lower = more focused
            max_tokens: Maximum response length
            
        Returns:
            Dictionary containing structured recommendations
            
        Raises:
            ValueError: If response is not valid JSON
            RuntimeError: If API call fails
        """
        try:
            logger.info("Calling Groq API for recommendations generation")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a business analytics expert specializing in banking and ATM terminal operations. Provide only valid JSON responses."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=temperature,
                max_tokens=max_tokens,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            logger.info(f"Received response from Groq API ({len(content)} chars)")
            
            # Parse and validate JSON structure
            recommendations = json.loads(content)
            
            if "recommendations" not in recommendations:
                raise ValueError("Response missing 'recommendations' key")
            
            return recommendations
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Groq response as JSON: {e}")
            raise ValueError(f"Invalid JSON response from AI model: {e}")
            
        except Exception as e:
            logger.error(f"Error calling Groq API: {e}")
            raise RuntimeError(f"Failed to generate recommendations: {e}")

