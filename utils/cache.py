"""
Caching utilities for the SearchableDataset application.
"""
import time
import json
import os
import pickle
from typing import Any, Dict, Optional, Callable, TypeVar, cast
from functools import wraps
from .logger import logger
from .config import config

# Type variable for generic function
T = TypeVar('T')

class Cache:
    """Simple cache implementation for storing function results."""
    
    def __init__(self, cache_dir: str = ".cache"):
        """
        Initialize the cache.
        
        Args:
            cache_dir: Directory to store cache files.
        """
        self.cache_dir = cache_dir
        self.expiry = config.CACHE_EXPIRY
        
        # Create cache directory if it doesn't exist
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
    
    def _get_cache_key(self, func_name: str, args: tuple, kwargs: Dict[str, Any]) -> str:
        """
        Generate a cache key from function name and arguments.
        
        Args:
            func_name: Name of the function.
            args: Positional arguments.
            kwargs: Keyword arguments.
            
        Returns:
            str: Cache key.
        """
        # Convert args and kwargs to a string representation
        args_str = json.dumps(str(args))
        kwargs_str = json.dumps(str(kwargs), sort_keys=True)
        
        # Combine function name and arguments to create a unique key
        key = f"{func_name}_{args_str}_{kwargs_str}"
        
        # Use a hash of the key as the filename to avoid issues with long filenames
        return f"{hash(key)}"
    
    def _get_cache_path(self, key: str) -> str:
        """
        Get the file path for a cache key.
        
        Args:
            key: Cache key.
            
        Returns:
            str: File path.
        """
        return os.path.join(self.cache_dir, f"{key}.pkl")
    
    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get a value from the cache.
        
        Args:
            key: Cache key.
            
        Returns:
            Optional[Dict[str, Any]]: Cached value or None if not found or expired.
        """
        cache_path = self._get_cache_path(key)
        
        if not os.path.exists(cache_path):
            return None
        
        try:
            with open(cache_path, "rb") as f:
                cached_data = pickle.load(f)
            
            # Check if cache has expired
            if time.time() - cached_data["timestamp"] > self.expiry:
                logger.debug(f"Cache expired for key: {key}")
                return None
            
            logger.debug(f"Cache hit for key: {key}")
            return cached_data["value"]
        except Exception as e:
            logger.error(f"Error reading cache: {e}")
            return None
    
    def set(self, key: str, value: Any) -> None:
        """
        Set a value in the cache.
        
        Args:
            key: Cache key.
            value: Value to cache.
        """
        cache_path = self._get_cache_path(key)
        
        try:
            cached_data = {
                "timestamp": time.time(),
                "value": value,
            }
            
            with open(cache_path, "wb") as f:
                pickle.dump(cached_data, f)
            
            logger.debug(f"Cache set for key: {key}")
        except Exception as e:
            logger.error(f"Error writing cache: {e}")
    
    def clear(self, key: Optional[str] = None) -> None:
        """
        Clear the cache.
        
        Args:
            key: Cache key to clear. If None, clear all cache.
        """
        if key:
            cache_path = self._get_cache_path(key)
            if os.path.exists(cache_path):
                os.remove(cache_path)
                logger.debug(f"Cleared cache for key: {key}")
        else:
            for filename in os.listdir(self.cache_dir):
                if filename.endswith(".pkl"):
                    os.remove(os.path.join(self.cache_dir, filename))
            logger.debug("Cleared all cache")
    
    def cached(self, func: Callable[..., T]) -> Callable[..., T]:
        """
        Decorator to cache function results.
        
        Args:
            func: Function to cache.
            
        Returns:
            Callable: Wrapped function.
        """
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            # Skip cache if DEBUG is True
            if config.DEBUG:
                return func(*args, **kwargs)
            
            # Generate cache key
            key = self._get_cache_key(func.__name__, args, kwargs)
            
            # Try to get from cache
            cached_value = self.get(key)
            if cached_value is not None:
                return cast(T, cached_value)
            
            # Call function and cache result
            result = func(*args, **kwargs)
            self.set(key, result)
            
            return result
        
        return wrapper


# Create a singleton instance
cache = Cache()
