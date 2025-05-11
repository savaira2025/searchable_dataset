"""
Logging configuration for the SearchableDataset application.
"""
import logging
import sys
from typing import Optional
from .config import config

# Define log levels
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

def setup_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Set up a logger with the specified name and level.
    
    Args:
        name: The name of the logger.
        level: The log level. If None, the level from the config is used.
        
    Returns:
        logging.Logger: The configured logger.
    """
    # Get the log level from config if not provided
    if level is None:
        level = config.LOG_LEVEL
    
    # Convert string level to logging level
    log_level = LOG_LEVELS.get(level.upper(), logging.INFO)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    
    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    
    # Add handler to logger if it doesn't already have one
    if not logger.handlers:
        logger.addHandler(handler)
    
    # Set propagate to False to avoid duplicate logs
    logger.propagate = False
    
    return logger

# Create a default logger for the application
logger = setup_logger("searchable_dataset")
