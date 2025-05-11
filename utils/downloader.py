"""
Dataset downloader module for the SearchableDataset application.
"""
import os
import threading
import time
import requests
from typing import Dict, Any, Optional, Callable, List
from pathlib import Path
import shutil
import uuid

from utils.logger import setup_logger

# Set up logger
log = setup_logger("downloader")

class DownloadStatus:
    """Class representing the status of a download."""
    
    PENDING = "pending"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DownloadTask:
    """Class representing a download task."""
    
    def __init__(
        self,
        dataset_id: str,
        dataset_name: str,
        source: str,
        url: str,
        target_dir: str,
    ):
        """
        Initialize a download task.
        
        Args:
            dataset_id: Dataset ID.
            dataset_name: Dataset name.
            source: Dataset source.
            url: Download URL.
            target_dir: Target directory.
        """
        self.id = str(uuid.uuid4())
        self.dataset_id = dataset_id
        self.dataset_name = dataset_name
        self.source = source
        self.url = url
        self.target_dir = target_dir
        self.status = DownloadStatus.PENDING
        self.progress = 0.0
        self.error = None
        self.start_time = None
        self.end_time = None
        self.file_path = None
        self.file_size = 0
        self.downloaded_size = 0
        self.thread = None
        self.cancel_event = threading.Event()


class DatasetDownloader:
    """Class for downloading datasets."""
    
    def __init__(self, datasets_dir: str = "Datasets"):
        """
        Initialize the dataset downloader.
        
        Args:
            datasets_dir: Directory to store downloaded datasets.
        """
        self.datasets_dir = datasets_dir
        self.downloads: Dict[str, DownloadTask] = {}
        self._ensure_datasets_dir()
    
    def _ensure_datasets_dir(self) -> None:
        """Ensure the datasets directory exists."""
        os.makedirs(self.datasets_dir, exist_ok=True)
        log.info(f"Datasets directory: {self.datasets_dir}")
    
    def download(
        self,
        dataset_id: str,
        dataset_name: str,
        source: str,
        url: str,
        connector_download_func: Optional[Callable] = None,
    ) -> str:
        """
        Start downloading a dataset.
        
        Args:
            dataset_id: Dataset ID.
            dataset_name: Dataset name.
            source: Dataset source.
            url: Download URL.
            connector_download_func: Function to download the dataset using the connector.
                If provided, this function will be used instead of the URL.
                The function should accept (dataset_id, target_path, progress_callback).
        
        Returns:
            str: Download task ID.
        """
        # Create a download task
        task = DownloadTask(
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            source=source,
            url=url,
            target_dir=self.datasets_dir,
        )
        
        # Store the task
        self.downloads[task.id] = task
        
        # Start the download in a separate thread
        task.thread = threading.Thread(
            target=self._download_thread,
            args=(task, connector_download_func),
            daemon=True,
        )
        task.thread.start()
        
        return task.id
    
    def _download_thread(
        self,
        task: DownloadTask,
        connector_download_func: Optional[Callable] = None,
    ) -> None:
        """
        Download thread function.
        
        Args:
            task: Download task.
            connector_download_func: Function to download the dataset using the connector.
        """
        task.start_time = time.time()
        task.status = DownloadStatus.DOWNLOADING
        
        try:
            # Ensure the target directory exists
            self._ensure_datasets_dir()
            
            # Generate a safe filename
            safe_name = self._safe_filename(task.dataset_name)
            
            # Determine file extension from URL or use default
            file_ext = self._get_file_extension(task.url)
            
            # Create the target file path
            task.file_path = os.path.join(
                self.datasets_dir,
                f"{safe_name}_{task.dataset_id.replace('/', '_')}{file_ext}"
            )
            
            # Download the dataset
            if connector_download_func:
                # Use the connector's download function
                connector_download_func(
                    task.dataset_id,
                    task.file_path,
                    lambda progress: self._update_progress(task.id, progress),
                    task.cancel_event,
                )
            else:
                # Use direct download from URL
                self._download_from_url(task)
            
            # Check if the download was cancelled
            if task.cancel_event.is_set():
                task.status = DownloadStatus.CANCELLED
                # Clean up partial download
                if os.path.exists(task.file_path):
                    os.remove(task.file_path)
                log.info(f"Download cancelled: {task.dataset_name}")
            else:
                task.status = DownloadStatus.COMPLETED
                task.progress = 1.0
                log.info(f"Download completed: {task.dataset_name} -> {task.file_path}")
        
        except Exception as e:
            task.status = DownloadStatus.FAILED
            task.error = str(e)
            log.error(f"Download failed: {task.dataset_name} - {e}")
            # Clean up partial download
            if task.file_path and os.path.exists(task.file_path):
                os.remove(task.file_path)
        
        finally:
            task.end_time = time.time()
    
    def _download_from_url(self, task: DownloadTask) -> None:
        """
        Download a file from a URL.
        
        Args:
            task: Download task.
        """
        with requests.get(task.url, stream=True, timeout=30) as response:
            response.raise_for_status()
            
            # Get the file size if available
            if 'content-length' in response.headers:
                task.file_size = int(response.headers['content-length'])
            
            # Download the file
            with open(task.file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    # Check if download should be cancelled
                    if task.cancel_event.is_set():
                        return
                    
                    if chunk:
                        f.write(chunk)
                        task.downloaded_size += len(chunk)
                        
                        # Update progress
                        if task.file_size > 0:
                            progress = task.downloaded_size / task.file_size
                            self._update_progress(task.id, progress)
    
    def _update_progress(self, task_id: str, progress: float) -> None:
        """
        Update the progress of a download task.
        
        Args:
            task_id: Download task ID.
            progress: Progress value (0.0 to 1.0).
        """
        if task_id in self.downloads:
            self.downloads[task_id].progress = min(max(progress, 0.0), 1.0)
    
    def cancel_download(self, task_id: str) -> bool:
        """
        Cancel a download.
        
        Args:
            task_id: Download task ID.
            
        Returns:
            bool: True if the download was cancelled, False otherwise.
        """
        if task_id in self.downloads:
            task = self.downloads[task_id]
            
            # Only cancel if the download is pending or in progress
            if task.status in [DownloadStatus.PENDING, DownloadStatus.DOWNLOADING]:
                task.cancel_event.set()
                return True
        
        return False
    
    def get_download_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a download.
        
        Args:
            task_id: Download task ID.
            
        Returns:
            Optional[Dict[str, Any]]: Download status or None if not found.
        """
        if task_id in self.downloads:
            task = self.downloads[task_id]
            
            # Calculate download speed and ETA
            speed = 0.0
            eta = 0.0
            
            if (task.status == DownloadStatus.DOWNLOADING and 
                task.start_time and task.file_size > 0 and 
                task.downloaded_size > 0):
                
                elapsed = time.time() - task.start_time
                if elapsed > 0:
                    speed = task.downloaded_size / elapsed  # bytes per second
                    remaining_bytes = task.file_size - task.downloaded_size
                    if speed > 0:
                        eta = remaining_bytes / speed  # seconds
            
            return {
                "id": task.id,
                "dataset_id": task.dataset_id,
                "dataset_name": task.dataset_name,
                "source": task.source,
                "status": task.status,
                "progress": task.progress,
                "error": task.error,
                "file_path": task.file_path,
                "file_size": task.file_size,
                "downloaded_size": task.downloaded_size,
                "speed": speed,  # bytes per second
                "eta": eta,  # seconds
                "start_time": task.start_time,
                "end_time": task.end_time,
            }
        
        return None
    
    def get_all_downloads(self) -> List[Dict[str, Any]]:
        """
        Get all downloads.
        
        Returns:
            List[Dict[str, Any]]: List of download statuses.
        """
        return [
            self.get_download_status(task_id) 
            for task_id in self.downloads
            if self.get_download_status(task_id) is not None
        ]
    
    def clean_completed_downloads(self, max_age: int = 3600) -> int:
        """
        Clean up completed downloads from memory (not files).
        
        Args:
            max_age: Maximum age in seconds for completed downloads to keep.
            
        Returns:
            int: Number of downloads cleaned up.
        """
        now = time.time()
        to_remove = []
        
        for task_id, task in self.downloads.items():
            if (task.status in [DownloadStatus.COMPLETED, DownloadStatus.FAILED, DownloadStatus.CANCELLED] and
                task.end_time and now - task.end_time > max_age):
                to_remove.append(task_id)
        
        for task_id in to_remove:
            del self.downloads[task_id]
        
        return len(to_remove)
    
    @staticmethod
    def _safe_filename(filename: str) -> str:
        """
        Create a safe filename.
        
        Args:
            filename: Original filename.
            
        Returns:
            str: Safe filename.
        """
        # Replace invalid characters
        safe_name = "".join(c if c.isalnum() or c in ['-', '_', '.'] else '_' for c in filename)
        
        # Limit length
        if len(safe_name) > 100:
            safe_name = safe_name[:100]
        
        return safe_name
    
    @staticmethod
    def _get_file_extension(url: str) -> str:
        """
        Get file extension from URL.
        
        Args:
            url: URL.
            
        Returns:
            str: File extension.
        """
        # Try to extract extension from URL
        path = url.split('?')[0]  # Remove query parameters
        ext = os.path.splitext(path)[1]
        
        # If no extension found, use .zip as default
        if not ext:
            ext = '.zip'
        
        return ext


# Create a singleton instance
downloader = DatasetDownloader()
