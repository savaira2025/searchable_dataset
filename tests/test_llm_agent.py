"""
Tests for the LLM agent.
"""
import unittest
from unittest.mock import patch, MagicMock
import json
from typing import Dict, List, Any

from agents import LLMAgent, PromptTemplates, ResponseProcessor
from data_sources import DatasetInfo

class TestLLMAgent(unittest.TestCase):
    """Tests for the LLM agent."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock the LLMAgent to avoid actual API calls
        self.patcher = patch('agents.llm_agent.ChatOpenAI')
        self.mock_chat = self.patcher.start()
        
        # Create a mock response
        mock_response = MagicMock()
        mock_response.content = """
        Search Terms: machine learning, neural networks, deep learning
        Explanation: These terms are related to artificial intelligence and would help find relevant datasets.
        Data Sources: kaggle, huggingface
        """
        self.mock_chat.return_value.return_value = [mock_response]
        
        # Create the agent
        self.agent = LLMAgent()
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.patcher.stop()
    
    def test_generate_search_terms(self):
        """Test generating search terms."""
        # Call the method
        search_terms, data_sources, explanation = self.agent._generate_search_terms("Find datasets for machine learning")
        
        # Check the results
        self.assertEqual(len(search_terms), 3)
        self.assertIn("machine learning", search_terms)
        self.assertIn("neural networks", search_terms)
        self.assertIn("deep learning", search_terms)
        
        self.assertEqual(len(data_sources), 2)
        self.assertIn("kaggle", data_sources)
        self.assertIn("huggingface", data_sources)
        
        self.assertIn("artificial intelligence", explanation)
    
    @patch('agents.llm_agent.get_connector')
    def test_search_source(self, mock_get_connector):
        """Test searching a source."""
        # Create mock datasets
        mock_datasets = [
            DatasetInfo(
                id="dataset1",
                name="Dataset 1",
                description="Description 1",
                source="kaggle",
                url="https://example.com/dataset1",
                size="10 MB",
                format="CSV",
                license="MIT",
                tags=["tag1", "tag2"],
                metadata={"key1": "value1"},
            ),
            DatasetInfo(
                id="dataset2",
                name="Dataset 2",
                description="Description 2",
                source="kaggle",
                url="https://example.com/dataset2",
                size="20 MB",
                format="JSON",
                license="Apache",
                tags=["tag3", "tag4"],
                metadata={"key2": "value2"},
            ),
        ]
        
        # Mock the connector
        mock_connector = MagicMock()
        mock_connector.search_cached.return_value = mock_datasets
        mock_get_connector.return_value = mock_connector
        
        # Call the method
        results = self.agent._search_source("kaggle", ["machine learning"])
        
        # Check the results
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["id"], "dataset1")
        self.assertEqual(results[0]["name"], "Dataset 1")
        self.assertEqual(results[1]["id"], "dataset2")
        self.assertEqual(results[1]["name"], "Dataset 2")
    
    def test_process_search_terms(self):
        """Test processing search terms."""
        # Sample response
        response = """
        Search Terms: machine learning, neural networks, deep learning
        Explanation: These terms are related to artificial intelligence and would help find relevant datasets.
        Data Sources: kaggle, huggingface
        """
        
        # Process the response
        search_terms, data_sources, explanation = ResponseProcessor.process_search_terms(response)
        
        # Check the results
        self.assertEqual(len(search_terms), 3)
        self.assertIn("machine learning", search_terms)
        self.assertIn("neural networks", search_terms)
        self.assertIn("deep learning", search_terms)
        
        self.assertEqual(len(data_sources), 2)
        self.assertIn("kaggle", data_sources)
        self.assertIn("huggingface", data_sources)
        
        self.assertIn("artificial intelligence", explanation)
    
    def test_process_dataset_analysis(self):
        """Test processing dataset analysis."""
        # Sample response
        response = """
        Ranking:
        1. Dataset 1 - This dataset is highly relevant because it contains machine learning examples.
        2. Dataset 2 - This dataset is somewhat relevant but lacks comprehensive examples.
        
        Detailed Analysis:
        [Dataset 1]:
        - Relevance: High
        - Strengths: Comprehensive examples, well-documented
        - Limitations: Large size, requires significant processing power
        - Recommendation: Highly recommended for machine learning tasks
        
        [Dataset 2]:
        - Relevance: Medium
        - Strengths: Small size, easy to use
        - Limitations: Limited examples, less comprehensive
        - Recommendation: Recommended for quick prototyping
        
        Overall Recommendation:
        Dataset 1 is the best choice for comprehensive machine learning tasks, while Dataset 2 is better for quick prototyping.
        """
        
        # Process the response
        analysis = ResponseProcessor.process_dataset_analysis(response)
        
        # Check the results
        self.assertEqual(len(analysis["ranking"]), 2)
        self.assertEqual(analysis["ranking"][0]["name"], "Dataset 1")
        self.assertEqual(analysis["ranking"][1]["name"], "Dataset 2")
        
        self.assertEqual(len(analysis["analysis"]), 2)
        self.assertEqual(analysis["analysis"]["Dataset 1"]["relevance"], "High")
        self.assertEqual(analysis["analysis"]["Dataset 2"]["relevance"], "Medium")
        
        self.assertIn("Dataset 1 is the best choice", analysis["overall_recommendation"])

if __name__ == "__main__":
    unittest.main()
