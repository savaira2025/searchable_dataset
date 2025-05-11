"""
Dataset source connectors for the SearchableDataset application.
"""
import json
import ast
from .base import BaseConnector, DatasetInfo
from .kaggle import KaggleConnector
from .huggingface import HuggingFaceConnector
from .google_dataset import GoogleDatasetConnector

__all__ = [
    "BaseConnector", 
    "DatasetInfo",
    "KaggleConnector",
    "HuggingFaceConnector",
    "GoogleDatasetConnector"
]

# Registry of available connectors
CONNECTORS = {
    "kaggle": KaggleConnector,
    "huggingface": HuggingFaceConnector,
    "google_dataset": GoogleDatasetConnector,
}

from utils.logger import setup_logger

def get_connector(name: str) -> BaseConnector:
    """
    Get a connector by name.
    
    Args:
        name: Name of the connector.
        
    Returns:
        BaseConnector: Connector instance.
        
    Raises:
        ValueError: If connector is not found.
    """
    logger = setup_logger("data_sources")
    
    # Log the input type and value
    logger.info(f"get_connector called with name: {name}, type: {type(name)}")
    
    # Special case: if name is the string representation of a list like "['kaggle']" or '["kaggle"]'
    if isinstance(name, str) and name.startswith("[") and name.endswith("]"):
        logger.warning(f"Name is a string representation of a list: {name}")
        
        # Try to parse as JSON array first
        try:
            parsed_list = json.loads(name)
            if isinstance(parsed_list, list) and parsed_list:
                logger.info(f"Successfully parsed name as JSON array: {parsed_list}")
                name = parsed_list[0]  # Take the first element
            else:
                logger.warning(f"Parsed JSON is not a list or is empty: {parsed_list}")
        except json.JSONDecodeError:
            # Try to parse as Python literal
            try:
                parsed_list = ast.literal_eval(name)
                if isinstance(parsed_list, list) and parsed_list:
                    logger.info(f"Successfully parsed name as Python literal: {parsed_list}")
                    name = parsed_list[0]  # Take the first element
                else:
                    logger.warning(f"Parsed literal is not a list or is empty: {parsed_list}")
            except (SyntaxError, ValueError):
                # Fall back to string extraction
                # Try to extract the actual name
                for connector in CONNECTORS.keys():
                    if connector in name.lower():
                        logger.info(f"Extracted connector name '{connector}' from '{name}'")
                        name = connector
                        break
                
                # If we couldn't extract a connector name, try to parse the string as a list
                if name.startswith("[") and name.endswith("]"):
                    try:
                        # Handle both single and double quotes
                        if "'" in name:
                            # Handle ['kaggle']
                            extracted = name.replace("[", "").replace("]", "").replace("'", "").strip()
                        elif '"' in name:
                            # Handle ["kaggle"]
                            extracted = name.replace("[", "").replace("]", "").replace('"', "").strip()
                        else:
                            # Handle [kaggle]
                            extracted = name.replace("[", "").replace("]", "").strip()
                        
                        logger.info(f"Extracted name from string representation: {extracted}")
                        
                        # Check if the extracted name is a valid connector
                        for connector in CONNECTORS.keys():
                            if connector in extracted.lower():
                                name = connector
                                logger.info(f"Matched extracted name to connector: {name}")
                                break
                    except Exception as e:
                        logger.warning(f"Failed to parse string representation of list: {name}, error: {e}")
    
    # Handle case where name is a list
    if isinstance(name, list):
        if not name:  # Empty list
            logger.error(f"Empty connector name list. Available connectors: {list(CONNECTORS.keys())}")
            raise ValueError(f"Empty connector name list. Available connectors: {list(CONNECTORS.keys())}")
        
        logger.warning(f"Name is a list: {name}, extracting first element: {name[0]}")
        name = name[0]  # Take the first element
        
        # If the first element is still a list, extract from that too
        if isinstance(name, list):
            logger.warning(f"First element is still a list: {name}, extracting from it: {name[0] if name else None}")
            name = name[0] if name else None
    
    # Convert name to string if it's not already
    if not isinstance(name, str):
        logger.warning(f"Converting non-string name to string: {name}")
        name = str(name)
    
    # Clean up name string - remove any additional text like "as per user's preference"
    original_name = name
    name = name.split(" as per ")[0].strip()
    name = name.split(" based on ")[0].strip()
    name = name.split(" according to ")[0].strip()
    name = name.split(" following ")[0].strip()
    
    # Normalize the name - remove spaces and convert to lowercase
    name = name.lower().replace(" ", "")
    
    # Map common variations to the correct connector names
    name_mapping = {
        "huggingface": "huggingface",
        "hugging": "huggingface",
        "hf": "huggingface",
        "kaggle": "kaggle",
        "google": "google_dataset",
        "googledataset": "google_dataset",
        "google_dataset": "google_dataset",
        "googledata": "google_dataset"
    }
    
    if name in name_mapping:
        name = name_mapping[name]
    else:
        # Try to match with any of the available connectors
        for connector in CONNECTORS.keys():
            if connector in name.lower():
                name = connector
                break
    
    if name != original_name:
        logger.info(f"Cleaned name from '{original_name}' to '{name}'")
    
    logger.info(f"Looking up connector with name: {name}, type: {type(name)}")
    
    if name not in CONNECTORS:
        logger.error(f"Connector '{name}' not found. Available connectors: {list(CONNECTORS.keys())}")
        raise ValueError(f"Connector '{name}' not found. Available connectors: {list(CONNECTORS.keys())}")
    
    logger.info(f"Found connector for name: {name}")
    
    # Create the connector instance
    connector_instance = CONNECTORS[name]()
    
    # Ensure the connector has the download_dataset method
    if not hasattr(connector_instance, "download_dataset"):
        logger.warning(f"Adding download_dataset method to {name} connector")
        
        # Add the download_dataset method to the connector instance
        from utils.downloader import downloader
        
        def download_dataset(self, dataset_id):
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
        
        # Add the method to the instance
        import types
        connector_instance.download_dataset = types.MethodType(download_dataset, connector_instance)
    
    return connector_instance
