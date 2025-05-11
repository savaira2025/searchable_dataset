"""
Kaggle dataset connector.
"""
import os
import threading
from typing import List, Optional, Dict, Any, Callable
import kaggle
from .base import BaseConnector, DatasetInfo
from utils import config
from utils.logger import setup_logger

class KaggleConnector(BaseConnector):
    """Connector for Kaggle datasets."""
    
    def __init__(self):
        """Initialize the Kaggle connector."""
        super().__init__("kaggle")
        
        # Set Kaggle credentials from config
        kaggle_config = config.get_kaggle_config()
        os.environ["KAGGLE_USERNAME"] = kaggle_config["username"]
        os.environ["KAGGLE_KEY"] = kaggle_config["key"]
    
    def search(self, query: str, limit: int = 10) -> List[DatasetInfo]:
        """
        Search for datasets on Kaggle.
        
        Args:
            query: Search query.
            limit: Maximum number of results.
            
        Returns:
            List[DatasetInfo]: List of dataset information.
        """
        try:
            # Search for datasets
            datasets = kaggle.api.dataset_list(search=query, max_size=limit)
            
            # Convert to DatasetInfo objects
            results = []
            for dataset in datasets:
                dataset_info = self._convert_to_dataset_info(dataset)
                results.append(dataset_info)
            
            return results
        except Exception as e:
            self.logger.error(f"Error searching Kaggle datasets: {e}")
            return []
    
    def get_dataset(self, dataset_id: str) -> Optional[DatasetInfo]:
        """
        Get dataset information by ID.
        
        Args:
            dataset_id: Dataset ID in the format "username/dataset-name".
            
        Returns:
            Optional[DatasetInfo]: Dataset information or None if not found.
        """
        try:
            # Get dataset information
            # The Kaggle API doesn't have a direct method to get a single dataset by ID
            # So we'll search for it and filter the results
            parts = dataset_id.split('/')
            if len(parts) != 2:
                self.logger.error(f"Invalid Kaggle dataset ID format: {dataset_id}")
                return None
                
            username, dataset_name = parts
            
            # Search for datasets by this user
            datasets = kaggle.api.dataset_list(user=username)
            
            # Find the specific dataset
            dataset = None
            for d in datasets:
                if d.ref == dataset_id:
                    dataset = d
                    break
            
            if not dataset:
                self.logger.error(f"Dataset not found: {dataset_id}")
                return None
            
            # Convert to DatasetInfo
            return self._convert_to_dataset_info(dataset)
        except Exception as e:
            self.logger.error(f"Error getting Kaggle dataset '{dataset_id}': {e}")
            return None
    
    def _convert_to_dataset_info(self, dataset: Any) -> DatasetInfo:
        """
        Convert Kaggle dataset to DatasetInfo.
        
        Args:
            dataset: Kaggle dataset object.
            
        Returns:
            DatasetInfo: Dataset information.
        """
        # Extract metadata
        metadata: Dict[str, Any] = {}
        for key, value in dataset.__dict__.items():
            if key not in ["id", "name", "description", "url", "size", "license", "tags"]:
                metadata[key] = value
        
        # Get size safely
        size = None
        try:
            size = dataset.size
        except AttributeError:
            self.logger.warning(f"Dataset {dataset.ref} does not have a size attribute")
        
        # Create DatasetInfo
        return DatasetInfo(
            id=dataset.ref,
            name=dataset.title,
            description=dataset.subtitle or "",
            source="Kaggle",
            url=f"https://www.kaggle.com/datasets/{dataset.ref}",
            size=self._format_size(size),
            format=None,  # Kaggle API doesn't provide format information
            license=dataset.licenseName if hasattr(dataset, "licenseName") else "Unknown",
            tags=[tag.name for tag in dataset.tags] if hasattr(dataset, "tags") else [],
            metadata=metadata,
        )
    
    def _format_size(self, size_bytes: int) -> str:
        """
        Format size in bytes to human-readable format.
        
        Args:
            size_bytes: Size in bytes.
            
        Returns:
            str: Formatted size.
        """
        if size_bytes is None:
            return "Unknown"
        
        # Convert to appropriate unit
        units = ["B", "KB", "MB", "GB", "TB"]
        size = float(size_bytes)
        unit_index = 0
        
        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1
        
        return f"{size:.2f} {units[unit_index]}"
    
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
        progress_callback: Callable[[float], None],
        cancel_event: threading.Event,
    ) -> None:
        """
        Implementation of dataset download using Kaggle API.
        
        Args:
            dataset_id: Dataset ID in the format "username/dataset-name".
            target_path: Path to save the dataset.
            progress_callback: Callback function to report progress (0.0 to 1.0).
            cancel_event: Event to check if download should be cancelled.
        """
        try:
            # Extract the target directory and filename
            target_dir = os.path.dirname(target_path)
            filename = os.path.basename(target_path)
            
            # Create a temporary directory for the download
            temp_dir = os.path.join(target_dir, f"temp_{dataset_id.replace('/', '_')}")
            os.makedirs(temp_dir, exist_ok=True)
            
            # Set initial progress
            progress_callback(0.1)
            
            # Check if cancelled
            if cancel_event.is_set():
                return
            
            # Download the dataset
            self.logger.info(f"Downloading Kaggle dataset: {dataset_id}")
            kaggle.api.dataset_download_files(
                dataset_id,
                path=temp_dir,
                unzip=True,
            )
            
            # Update progress
            progress_callback(0.8)
            
            # Check if cancelled
            if cancel_event.is_set():
                # Clean up
                if os.path.exists(temp_dir):
                    import shutil
                    shutil.rmtree(temp_dir)
                return
            
            # Find the downloaded files
            downloaded_files = os.listdir(temp_dir)
            
            if not downloaded_files:
                raise Exception(f"No files found in downloaded dataset: {dataset_id}")
            
            # If there's only one file, move it to the target path
            if len(downloaded_files) == 1:
                source_file = os.path.join(temp_dir, downloaded_files[0])
                import shutil
                shutil.move(source_file, target_path)
            else:
                # If there are multiple files, create a zip file
                import zipfile
                with zipfile.ZipFile(target_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file in downloaded_files:
                        file_path = os.path.join(temp_dir, file)
                        # Add the file to the zip
                        zipf.write(file_path, arcname=file)
            
            # Clean up the temporary directory
            import shutil
            shutil.rmtree(temp_dir)
            
            # Final progress update
            progress_callback(1.0)
            
            self.logger.info(f"Kaggle dataset downloaded: {dataset_id} -> {target_path}")
            
        except Exception as e:
            self.logger.error(f"Error downloading Kaggle dataset '{dataset_id}': {e}")
            # Clean up any temporary files
            if 'temp_dir' in locals() and os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir)
            raise
