"""
Configuration management for the SearchableDataset application.
"""
import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Configuration class for the application."""
    
    # API Keys
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    KAGGLE_USERNAME: str = os.getenv("KAGGLE_USERNAME", "")
    KAGGLE_KEY: str = os.getenv("KAGGLE_KEY", "")
    HUGGINGFACE_API_KEY: str = os.getenv("HUGGINGFACE_API_KEY", "")
    
    # Application Settings
    DEBUG: bool = os.getenv("DEBUG", "False").lower() in ("true", "1", "t")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    # Parse CACHE_EXPIRY and strip any comments
    _cache_expiry_value = os.getenv("CACHE_EXPIRY", "3600")
    if "#" in _cache_expiry_value:
        _cache_expiry_value = _cache_expiry_value.split("#")[0].strip()
    CACHE_EXPIRY: int = int(_cache_expiry_value)
    
    @classmethod
    def get_llm_config(cls) -> Dict[str, Any]:
        """Get configuration for the LLM."""
        return {
            "api_key": cls.OPENAI_API_KEY,
            "model": "gpt-4o",  # Default model
            "temperature": 0.7,
            "max_tokens": 1000,
        }
    
    @classmethod
    def get_kaggle_config(cls) -> Dict[str, str]:
        """Get configuration for Kaggle API."""
        return {
            "username": cls.KAGGLE_USERNAME,
            "key": cls.KAGGLE_KEY,
        }
    
    @classmethod
    def get_huggingface_config(cls) -> Dict[str, str]:
        """Get configuration for Hugging Face API."""
        return {
            "api_key": cls.HUGGINGFACE_API_KEY,
        }
    
    @classmethod
    def validate(cls) -> Optional[str]:
        """
        Validate the configuration.
        
        Returns:
            Optional[str]: Error message if validation fails, None otherwise.
        """
        if not cls.OPENAI_API_KEY:
            return "OPENAI_API_KEY is not set. Please set it in the .env file."
        
        # Add more validation as needed
        
        return None


# Create a singleton instance
config = Config()
