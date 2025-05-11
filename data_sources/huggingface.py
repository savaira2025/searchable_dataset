"""
Hugging Face dataset connector.
"""
from typing import List, Optional, Dict, Any
import requests
from huggingface_hub import HfApi
from .base import BaseConnector, DatasetInfo
from utils import config

class HuggingFaceConnector(BaseConnector):
    """Connector for Hugging Face datasets."""
    
    def __init__(self):
        """Initialize the Hugging Face connector."""
        super().__init__("huggingface")
        
        # Set up API
        hf_config = config.get_huggingface_config()
        self.api_key = hf_config["api_key"]
        self.api = HfApi(token=self.api_key)
        self.base_url = "https://huggingface.co/api/datasets"
    
    def search(self, query: str, limit: int = 10) -> List[DatasetInfo]:
        """
        Search for datasets on Hugging Face.
        
        Args:
            query: Search query.
            limit: Maximum number of results.
            
        Returns:
            List[DatasetInfo]: List of dataset information.
        """
        try:
            # Search for datasets
            headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
            params = {"search": query, "limit": limit}
            response = requests.get(self.base_url, headers=headers, params=params)
            response.raise_for_status()
            
            # Parse response
            datasets = response.json()
            
            # Convert to DatasetInfo objects
            results = []
            for dataset in datasets:
                dataset_info = self._convert_to_dataset_info(dataset)
                results.append(dataset_info)
            
            return results
        except Exception as e:
            self.logger.error(f"Error searching Hugging Face datasets: {e}")
            return []
    
    def get_dataset(self, dataset_id: str) -> Optional[DatasetInfo]:
        """
        Get dataset information by ID.
        
        Args:
            dataset_id: Dataset ID.
            
        Returns:
            Optional[DatasetInfo]: Dataset information or None if not found.
        """
        try:
            # Get dataset information
            headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
            response = requests.get(f"{self.base_url}/{dataset_id}", headers=headers)
            response.raise_for_status()
            
            # Parse response
            dataset = response.json()
            
            # Convert to DatasetInfo
            return self._convert_to_dataset_info(dataset)
        except Exception as e:
            self.logger.error(f"Error getting Hugging Face dataset '{dataset_id}': {e}")
            return None
    
    def _convert_to_dataset_info(self, dataset: Dict[str, Any]) -> DatasetInfo:
        """
        Convert Hugging Face dataset to DatasetInfo.
        
        Args:
            dataset: Hugging Face dataset dictionary.
            
        Returns:
            DatasetInfo: Dataset information.
        """
        # Extract metadata
        metadata: Dict[str, Any] = {}
        for key, value in dataset.items():
            if key not in ["id", "name", "description", "url", "size", "license", "tags"]:
                metadata[key] = value
        
        # Create DatasetInfo
        return DatasetInfo(
            id=dataset["id"],
            name=dataset.get("name", dataset["id"]),
            description=dataset.get("description", ""),
            source="Hugging Face",
            url=f"https://huggingface.co/datasets/{dataset['id']}",
            size=dataset.get("size_categories", ["Unknown"])[0],
            format=None,  # Hugging Face API doesn't provide format information
            license=dataset.get("license", "Unknown"),
            tags=dataset.get("tags", []),
            metadata=metadata,
        )
    
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
        from utils.downloader import downloader
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
        progress_callback: callable,
        cancel_event: Any,
    ) -> None:
        """
        Implementation of dataset download for Hugging Face.
        
        Args:
            dataset_id: Dataset ID.
            target_path: Path to save the dataset.
            progress_callback: Callback function to report progress (0.0 to 1.0).
            cancel_event: Event to check if download should be cancelled.
        """
        import os
        import time
        import requests
        from tqdm import tqdm
        
        try:
            # Get dataset information
            dataset = self.get_dataset_cached(dataset_id)
            if not dataset:
                raise ValueError(f"Dataset not found: {dataset_id}")
            
            # Set initial progress
            progress_callback(0.1)
            
            # Check if cancelled
            if cancel_event.is_set():
                return
            
            # Get download URL
            download_url = f"https://huggingface.co/datasets/{dataset_id}/resolve/main/data/dataset.zip"
            
            # Download the dataset
            self.logger.info(f"Downloading Hugging Face dataset: {dataset_id}")
            
            # Make a request to get the file size
            response = requests.head(download_url, headers={"Authorization": f"Bearer {self.api_key}"} if self.api_key else {})
            file_size = int(response.headers.get("content-length", 0))
            
            # Download with progress tracking
            response = requests.get(download_url, stream=True, headers={"Authorization": f"Bearer {self.api_key}"} if self.api_key else {})
            response.raise_for_status()
            
            # Create parent directory if it doesn't exist
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            
            # Download the file
            downloaded_size = 0
            with open(target_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if cancel_event.is_set():
                        # Clean up
                        f.close()
                        if os.path.exists(target_path):
                            os.remove(target_path)
                        return
                    
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Update progress
                        if file_size > 0:
                            progress = 0.1 + 0.9 * (downloaded_size / file_size)
                            progress_callback(min(progress, 0.99))
                        else:
                            # If file size is unknown, update progress based on time
                            progress_callback(min(0.1 + (time.time() % 10) / 100, 0.99))
            
            # Final progress update
            progress_callback(1.0)
            
            self.logger.info(f"Hugging Face dataset downloaded: {dataset_id} -> {target_path}")
            
        except Exception as e:
            self.logger.error(f"Error downloading Hugging Face dataset '{dataset_id}': {e}")
            # Clean up any partial downloads
            if os.path.exists(target_path):
                os.remove(target_path)
            raise
