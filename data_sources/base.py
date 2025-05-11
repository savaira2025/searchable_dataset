"""
Base connector class for dataset sources.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Callable
from utils.logger import setup_logger
from utils import cache
from utils.downloader import downloader

class DatasetInfo:
    """Class representing dataset information."""
    
    def __init__(
        self,
        id: str,
        name: str,
        description: str,
        source: str,
        url: Optional[str] = None,
        size: Optional[str] = None,
        format: Optional[str] = None,
        license: Optional[str] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize dataset information.
        
        Args:
            id: Unique identifier for the dataset.
            name: Name of the dataset.
            description: Description of the dataset.
            source: Source of the dataset (e.g., "Kaggle", "Hugging Face").
            url: URL to the dataset.
            size: Size of the dataset.
            format: Format of the dataset.
            license: License of the dataset.
            tags: Tags associated with the dataset.
            metadata: Additional metadata.
        """
        self.id = id
        self.name = name
        self.description = description
        self.source = source
        self.url = url
        self.size = size
        self.format = format
        self.license = license
        self.tags = tags or []
        self.metadata = metadata or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary.
        
        Returns:
            Dict[str, Any]: Dictionary representation.
        """
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "source": self.source,
            "url": self.url,
            "size": self.size,
            "format": self.format,
            "license": self.license,
            "tags": self.tags,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatasetInfo":
        """
        Create from dictionary.
        
        Args:
            data: Dictionary representation.
            
        Returns:
            DatasetInfo: Dataset information.
        """
        return cls(
            id=data["id"],
            name=data["name"],
            description=data["description"],
            source=data["source"],
            url=data.get("url"),
            size=data.get("size"),
            format=data.get("format"),
            license=data.get("license"),
            tags=data.get("tags"),
            metadata=data.get("metadata"),
        )


class BaseConnector(ABC):
    """Base class for dataset source connectors."""
    
    def __init__(self, name: str):
        """
        Initialize the connector.
        
        Args:
            name: Name of the connector.
        """
        self.name = name
        self.logger = setup_logger(f"connector.{name}")
    
    @abstractmethod
    def search(self, query: str, limit: int = 10) -> List[DatasetInfo]:
        """
        Search for datasets.
        
        Args:
            query: Search query.
            limit: Maximum number of results.
            
        Returns:
            List[DatasetInfo]: List of dataset information.
        """
        pass
    
    @abstractmethod
    def get_dataset(self, dataset_id: str) -> Optional[DatasetInfo]:
        """
        Get dataset information by ID.
        
        Args:
            dataset_id: Dataset ID.
            
        Returns:
            Optional[DatasetInfo]: Dataset information or None if not found.
        """
        pass
    
    @cache.cached
    def search_cached(self, query: str, limit: int = 10) -> List[DatasetInfo]:
        """
        Search for datasets with caching.
        
        Args:
            query: Search query.
            limit: Maximum number of results.
            
        Returns:
            List[DatasetInfo]: List of dataset information.
        """
        self.logger.info(f"Searching for '{query}' (limit={limit})")
        return self.search(query, limit)
    
    def get_dataset_cached(self, dataset_id: str) -> Optional[DatasetInfo]:
        """
        Get dataset information by ID with caching.
        
        Args:
            dataset_id: Dataset ID.
            
        Returns:
            Optional[DatasetInfo]: Dataset information or None if not found.
        """
        from utils.cache import cache
        
        self.logger.info(f"Getting dataset '{dataset_id}'")
        
        # Generate cache key
        import inspect
        func_name = inspect.currentframe().f_code.co_name
        key = cache._get_cache_key(func_name, (self.name, dataset_id), {})
        
        # Try to get from cache
        cached_value = cache.get(key)
        if cached_value is not None:
            return cached_value
        
        try:
            # Call the actual method
            result = self.get_dataset(dataset_id)
            
            # Cache the result
            if result is not None:
                cache.set(key, result)
            
            return result
        except Exception as e:
            # If there's an error, clear the cache for this key
            cache.clear(key)
            self.logger.error(f"Error getting dataset '{dataset_id}': {e}")
            return None
    
    def download_dataset(self, dataset_id: str) -> Optional[str]:
        """
        Download a dataset.
        
        Args:
            dataset_id: Dataset ID.
            
        Returns:
            Optional[str]: Download task ID or None if download failed.
        """
        # Get dataset information
        dataset = self.get_dataset_cached(dataset_id)
        if not dataset:
            self.logger.error(f"Dataset not found: {dataset_id}")
            return None
        
        # Check if URL is available
        if not dataset.url:
            self.logger.error(f"Dataset URL not available: {dataset_id}")
            return None
        
        # Start download
        self.logger.info(f"Downloading dataset: {dataset.name} ({dataset_id})")
        
        # Use the downloader to start the download
        download_id = downloader.download(
            dataset_id=dataset_id,
            dataset_name=dataset.name,
            source=self.name,
            url=dataset.url,
            connector_download_func=self._download_dataset_impl,
        )
        
        return download_id
    
    def _download_dataset_impl(
        self, 
        dataset_id: str, 
        target_path: str, 
        progress_callback: Callable[[float], None],
        cancel_event: Any,
    ) -> None:
        """
        Implementation of dataset download.
        
        This method can be overridden by subclasses to provide custom download logic.
        The default implementation uses the dataset URL for direct download.
        
        Args:
            dataset_id: Dataset ID.
            target_path: Path to save the dataset.
            progress_callback: Callback function to report progress (0.0 to 1.0).
            cancel_event: Event to check if download should be cancelled.
        """
        # Default implementation does nothing, as the downloader will use the URL
        # Subclasses can override this method to provide custom download logic
        pass
