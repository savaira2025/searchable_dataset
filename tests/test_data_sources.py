"""
Tests for the data sources.
"""
import unittest
from unittest.mock import patch, MagicMock
import json
from typing import Dict, List, Any

from data_sources import (
    BaseConnector,
    DatasetInfo,
    get_connector,
    CONNECTORS,
    KaggleConnector,
    HuggingFaceConnector,
    GoogleDatasetConnector,
)

class TestDatasetInfo(unittest.TestCase):
    """Tests for the DatasetInfo class."""
    
    def test_to_dict(self):
        """Test converting to dictionary."""
        # Create a dataset info
        dataset_info = DatasetInfo(
            id="test_id",
            name="Test Dataset",
            description="Test description",
            source="test_source",
            url="https://example.com",
            size="10 MB",
            format="CSV",
            license="MIT",
            tags=["tag1", "tag2"],
            metadata={"key": "value"},
        )
        
        # Convert to dictionary
        data_dict = dataset_info.to_dict()
        
        # Check the dictionary
        self.assertEqual(data_dict["id"], "test_id")
        self.assertEqual(data_dict["name"], "Test Dataset")
        self.assertEqual(data_dict["description"], "Test description")
        self.assertEqual(data_dict["source"], "test_source")
        self.assertEqual(data_dict["url"], "https://example.com")
        self.assertEqual(data_dict["size"], "10 MB")
        self.assertEqual(data_dict["format"], "CSV")
        self.assertEqual(data_dict["license"], "MIT")
        self.assertEqual(data_dict["tags"], ["tag1", "tag2"])
        self.assertEqual(data_dict["metadata"], {"key": "value"})
    
    def test_from_dict(self):
        """Test creating from dictionary."""
        # Create a dictionary
        data_dict = {
            "id": "test_id",
            "name": "Test Dataset",
            "description": "Test description",
            "source": "test_source",
            "url": "https://example.com",
            "size": "10 MB",
            "format": "CSV",
            "license": "MIT",
            "tags": ["tag1", "tag2"],
            "metadata": {"key": "value"},
        }
        
        # Create from dictionary
        dataset_info = DatasetInfo.from_dict(data_dict)
        
        # Check the dataset info
        self.assertEqual(dataset_info.id, "test_id")
        self.assertEqual(dataset_info.name, "Test Dataset")
        self.assertEqual(dataset_info.description, "Test description")
        self.assertEqual(dataset_info.source, "test_source")
        self.assertEqual(dataset_info.url, "https://example.com")
        self.assertEqual(dataset_info.size, "10 MB")
        self.assertEqual(dataset_info.format, "CSV")
        self.assertEqual(dataset_info.license, "MIT")
        self.assertEqual(dataset_info.tags, ["tag1", "tag2"])
        self.assertEqual(dataset_info.metadata, {"key": "value"})

class TestConnectors(unittest.TestCase):
    """Tests for the connectors."""
    
    def test_get_connector(self):
        """Test getting a connector."""
        # Get connectors
        kaggle_connector = get_connector("kaggle")
        huggingface_connector = get_connector("huggingface")
        google_connector = get_connector("google_dataset")
        
        # Check the connectors
        self.assertIsInstance(kaggle_connector, KaggleConnector)
        self.assertIsInstance(huggingface_connector, HuggingFaceConnector)
        self.assertIsInstance(google_connector, GoogleDatasetConnector)
    
    def test_get_connector_invalid(self):
        """Test getting an invalid connector."""
        # Try to get an invalid connector
        with self.assertRaises(ValueError):
            get_connector("invalid_connector")

class TestKaggleConnector(unittest.TestCase):
    """Tests for the Kaggle connector."""
    
    @patch('data_sources.kaggle.kaggle.api.dataset_list')
    def test_search(self, mock_dataset_list):
        """Test searching for datasets."""
        # Create mock datasets
        mock_dataset = MagicMock()
        mock_dataset.ref = "username/dataset"
        mock_dataset.title = "Test Dataset"
        mock_dataset.subtitle = "Test description"
        mock_dataset.size = 1024 * 1024  # 1 MB
        mock_dataset.licenseName = "MIT"
        mock_dataset.tags = [MagicMock(name="tag1"), MagicMock(name="tag2")]
        
        # Set up the mock
        mock_dataset_list.return_value = [mock_dataset]
        
        # Create the connector
        connector = KaggleConnector()
        
        # Search for datasets
        results = connector.search("test query")
        
        # Check the results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].id, "username/dataset")
        self.assertEqual(results[0].name, "Test Dataset")
        self.assertEqual(results[0].description, "Test description")
        self.assertEqual(results[0].source, "Kaggle")
        self.assertEqual(results[0].url, "https://www.kaggle.com/datasets/username/dataset")
        self.assertEqual(results[0].size, "1.00 MB")
        self.assertEqual(results[0].license, "MIT")
        self.assertEqual(results[0].tags, ["tag1", "tag2"])

class TestHuggingFaceConnector(unittest.TestCase):
    """Tests for the Hugging Face connector."""
    
    @patch('data_sources.huggingface.requests.get')
    def test_search(self, mock_get):
        """Test searching for datasets."""
        # Create mock response
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "id": "test_dataset",
                "name": "Test Dataset",
                "description": "Test description",
                "tags": ["tag1", "tag2"],
                "license": "MIT",
                "size_categories": ["10MB-100MB"],
            }
        ]
        mock_get.return_value = mock_response
        
        # Create the connector
        connector = HuggingFaceConnector()
        
        # Search for datasets
        results = connector.search("test query")
        
        # Check the results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].id, "test_dataset")
        self.assertEqual(results[0].name, "Test Dataset")
        self.assertEqual(results[0].description, "Test description")
        self.assertEqual(results[0].source, "Hugging Face")
        self.assertEqual(results[0].url, "https://huggingface.co/datasets/test_dataset")
        self.assertEqual(results[0].size, "10MB-100MB")
        self.assertEqual(results[0].license, "MIT")
        self.assertEqual(results[0].tags, ["tag1", "tag2"])

if __name__ == "__main__":
    unittest.main()
