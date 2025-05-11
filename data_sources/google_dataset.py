"""
Google Dataset Search connector.
"""
import re
import requests
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup
from .base import BaseConnector, DatasetInfo

class GoogleDatasetConnector(BaseConnector):
    """Connector for Google Dataset Search."""
    
    def __init__(self):
        """Initialize the Google Dataset Search connector."""
        super().__init__("google_dataset")
        self.base_url = "https://datasetsearch.research.google.com/search"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        }
    
    def search(self, query: str, limit: int = 10) -> List[DatasetInfo]:
        """
        Search for datasets on Google Dataset Search.
        
        Args:
            query: Search query.
            limit: Maximum number of results.
            
        Returns:
            List[DatasetInfo]: List of dataset information.
        """
        try:
            # Prepare search URL
            params = {"query": query}
            response = requests.get(self.base_url, headers=self.headers, params=params)
            response.raise_for_status()
            
            # Parse HTML response
            soup = BeautifulSoup(response.text, "lxml")
            
            # Extract dataset information
            dataset_elements = soup.select(".dataset-card")
            
            # Limit results
            dataset_elements = dataset_elements[:limit]
            
            # Convert to DatasetInfo objects
            results = []
            for element in dataset_elements:
                try:
                    dataset_info = self._extract_dataset_info(element)
                    if dataset_info:
                        results.append(dataset_info)
                except Exception as e:
                    self.logger.error(f"Error extracting dataset info: {e}")
            
            return results
        except Exception as e:
            self.logger.error(f"Error searching Google Dataset Search: {e}")
            return []
    
    def get_dataset(self, dataset_id: str) -> Optional[DatasetInfo]:
        """
        Get dataset information by ID.
        
        Args:
            dataset_id: Dataset ID (URL).
            
        Returns:
            Optional[DatasetInfo]: Dataset information or None if not found.
        """
        try:
            # For Google Dataset Search, the ID is the URL
            response = requests.get(dataset_id, headers=self.headers)
            response.raise_for_status()
            
            # Parse HTML response
            soup = BeautifulSoup(response.text, "lxml")
            
            # Extract dataset information
            # This is a simplified implementation and may need to be adapted
            # based on the actual structure of the dataset page
            name = soup.select_one("h1").text.strip()
            description = soup.select_one("meta[name='description']")["content"]
            
            # Create DatasetInfo
            return DatasetInfo(
                id=dataset_id,
                name=name,
                description=description,
                source="Google Dataset Search",
                url=dataset_id,
                size=None,
                format=None,
                license=None,
                tags=[],
                metadata={},
            )
        except Exception as e:
            self.logger.error(f"Error getting Google dataset '{dataset_id}': {e}")
            return None
    
    def _extract_dataset_info(self, element: Any) -> Optional[DatasetInfo]:
        """
        Extract dataset information from HTML element.
        
        Args:
            element: BeautifulSoup element.
            
        Returns:
            Optional[DatasetInfo]: Dataset information or None if extraction fails.
        """
        try:
            # Extract basic information
            title_element = element.select_one(".dataset-title")
            if not title_element:
                return None
            
            name = title_element.text.strip()
            
            # Extract URL
            url_element = title_element.find("a")
            url = url_element["href"] if url_element else None
            
            # Extract description
            description_element = element.select_one(".dataset-description")
            description = description_element.text.strip() if description_element else ""
            
            # Extract additional information
            metadata: Dict[str, Any] = {}
            info_elements = element.select(".dataset-info-item")
            for info in info_elements:
                label_element = info.select_one(".info-label")
                value_element = info.select_one(".info-value")
                
                if label_element and value_element:
                    label = label_element.text.strip().lower().replace(" ", "_")
                    value = value_element.text.strip()
                    metadata[label] = value
            
            # Extract size
            size = metadata.get("size", None)
            
            # Extract format
            format_value = metadata.get("file_format", None)
            
            # Extract license
            license_value = metadata.get("license", None)
            
            # Extract tags
            tags = []
            tags_element = element.select_one(".dataset-tags")
            if tags_element:
                tag_elements = tags_element.select(".dataset-tag")
                tags = [tag.text.strip() for tag in tag_elements]
            
            # Generate a unique ID if URL is not available
            if not url:
                # Create a hash of the name and description
                id_value = f"google_{hash(name + description)}"
            else:
                # Use the URL as the ID
                id_value = url
            
            # Create DatasetInfo
            return DatasetInfo(
                id=id_value,
                name=name,
                description=description,
                source="Google Dataset Search",
                url=url,
                size=size,
                format=format_value,
                license=license_value,
                tags=tags,
                metadata=metadata,
            )
        except Exception as e:
            self.logger.error(f"Error extracting dataset info: {e}")
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
        Implementation of dataset download for Google Dataset Search.
        
        Args:
            dataset_id: Dataset ID.
            target_path: Path to save the dataset.
            progress_callback: Callback function to report progress (0.0 to 1.0).
            cancel_event: Event to check if download should be cancelled.
        """
        import os
        import time
        import requests
        
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
            
            # Get download URL - for Google Dataset Search, the URL is the dataset page
            # We need to extract the actual download link from the page
            download_url = dataset.url
            
            # Download the dataset page
            self.logger.info(f"Accessing Google dataset page: {dataset_id}")
            response = requests.get(download_url, headers=self.headers)
            response.raise_for_status()
            
            # Parse HTML response to find download link
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, "lxml")
            
            # Look for download links
            download_links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                text = a.text.strip().lower()
                
                # Check if this looks like a download link
                if any(keyword in text for keyword in ["download", "get data", "access data"]):
                    download_links.append(href)
                
                # Also check for common file extensions
                if any(href.endswith(ext) for ext in [".csv", ".json", ".xml", ".zip", ".tar.gz"]):
                    download_links.append(href)
            
            if not download_links:
                raise ValueError(f"No download links found on the dataset page: {download_url}")
            
            # Use the first download link
            file_url = download_links[0]
            
            # If the URL is relative, make it absolute
            if not file_url.startswith("http"):
                from urllib.parse import urljoin
                file_url = urljoin(download_url, file_url)
            
            self.logger.info(f"Found download link: {file_url}")
            
            # Update progress
            progress_callback(0.2)
            
            # Check if cancelled
            if cancel_event.is_set():
                return
            
            # Download the file
            self.logger.info(f"Downloading file from: {file_url}")
            file_response = requests.get(file_url, stream=True, headers=self.headers)
            file_response.raise_for_status()
            
            # Get file size if available
            file_size = int(file_response.headers.get("content-length", 0))
            
            # Create parent directory if it doesn't exist
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            
            # Download the file with progress tracking
            downloaded_size = 0
            with open(target_path, "wb") as f:
                for chunk in file_response.iter_content(chunk_size=8192):
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
                            progress = 0.2 + 0.8 * (downloaded_size / file_size)
                            progress_callback(min(progress, 0.99))
                        else:
                            # If file size is unknown, update progress based on time
                            progress_callback(min(0.2 + (time.time() % 10) / 100, 0.99))
            
            # Final progress update
            progress_callback(1.0)
            
            self.logger.info(f"Google dataset downloaded: {dataset_id} -> {target_path}")
            
        except Exception as e:
            self.logger.error(f"Error downloading Google dataset '{dataset_id}': {e}")
            # Clean up any partial downloads
            if os.path.exists(target_path):
                os.remove(target_path)
            raise
