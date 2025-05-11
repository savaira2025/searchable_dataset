"""
LLM agent for dataset search and analysis.
"""
import time
from typing import Dict, List, Any, Optional, Tuple, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import openai
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage

from .prompts import PromptTemplates
from .processors import ResponseProcessor
from data_sources import get_connector, DatasetInfo
from utils import config, cache
from utils.logger import setup_logger

class LLMAgent:
    """LLM agent for dataset search and analysis."""
    
    def __init__(self):
        """Initialize the LLM agent."""
        self.logger = setup_logger("llm_agent")
        
        # Set up OpenAI client
        openai.api_key = config.OPENAI_API_KEY
        
        # Set up LangChain models
        self.llm_config = config.get_llm_config()
        self.chat_model = ChatOpenAI(
            model_name=self.llm_config["model"],
            temperature=self.llm_config["temperature"],
            api_key=self.llm_config["api_key"],
        )
    
    def search_datasets(self, query: str, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Search for datasets based on a user query.
        
        Args:
            query: User query.
            context: Additional context.
            
        Returns:
            Dict[str, Any]: Search results.
        """
        self.logger.info(f"Searching datasets for query: {query}")
        
        # Check if user has specified data sources in the context
        user_data_sources = None
        if context and "user_preferences" in context and "data_sources" in context["user_preferences"]:
            user_data_sources = context["user_preferences"]["data_sources"]
            self.logger.info(f"User specified data sources: {user_data_sources}")
        
        # Generate search terms using LLM
        search_terms, llm_data_sources, explanation = self._generate_search_terms(query, context)
        
        # ALWAYS use user-specified data sources if available, and IGNORE LLM-suggested ones
        if user_data_sources:
            data_sources = user_data_sources
            self.logger.info(f"Using ONLY user-specified data sources: {data_sources}")
        else:
            data_sources = llm_data_sources
            self.logger.info(f"No user preferences found, using LLM-suggested data sources: {data_sources}")
        
        # If no data sources specified, use all available
        if not data_sources:
            from data_sources import CONNECTORS
            data_sources = list(CONNECTORS.keys())
            self.logger.info(f"No data sources specified, using all available: {data_sources}")
        
        # Search datasets from multiple sources in parallel
        # STRICTLY use only user-specified data sources if available
        if user_data_sources:
            self.logger.info(f"Strictly using ONLY user-specified data sources: {user_data_sources}")
            search_sources = user_data_sources
            # Update data_sources to reflect what was actually used
            data_sources = user_data_sources
        else:
            search_sources = data_sources
            
        all_datasets = self._search_multiple_sources(search_terms, search_sources)
        
        # Analyze datasets using LLM
        analysis = self._analyze_datasets(query, all_datasets)
        
        return {
            "query": query,
            "search_terms": search_terms,
            "data_sources": data_sources,
            "explanation": explanation,
            "datasets": all_datasets,
            "analysis": analysis,
        }
    
    def get_dataset_recommendation(self, query: str, datasets: List[Dict[str, Any]]) -> str:
        """
        Get a recommendation for the best dataset based on a user query.
        
        Args:
            query: User query.
            datasets: List of datasets.
            
        Returns:
            str: Recommendation.
        """
        self.logger.info(f"Getting dataset recommendation for query: {query}")
        
        # Generate prompt
        prompt = PromptTemplates.dataset_recommendation_prompt(query, datasets)
        
        # Get recommendation from LLM
        response = self._call_llm(prompt)
        
        # Process response
        recommendation = ResponseProcessor.process_dataset_recommendation(response)
        
        return recommendation
    
    def _generate_search_terms(self, query: str, context: Dict[str, Any] = None) -> Tuple[List[str], List[str], str]:
        """
        Generate search terms for a user query.
        
        Args:
            query: User query.
            context: Additional context.
            
        Returns:
            Tuple[List[str], List[str], str]: Search terms, data sources, and explanation.
        """
        # Generate prompt
        prompt = PromptTemplates.dataset_search_prompt(query, context)
        
        # Get response from LLM
        response = self._call_llm(prompt)
        
        # Process response
        search_terms, data_sources, explanation = ResponseProcessor.process_search_terms(response)
        
        # Log the raw types for debugging
        self.logger.info(f"Generated search terms: {search_terms}")
        self.logger.info(f"Recommended data sources: {data_sources}")
        self.logger.debug(f"Search terms types: {[type(term) for term in search_terms]}")
        self.logger.debug(f"Data sources types: {[type(source) for source in data_sources]}")
        
        # Ensure data_sources contains only strings, not lists
        normalized_sources = []
        for source in data_sources:
            if isinstance(source, list):
                self.logger.warning(f"Found nested list in data sources: {source}")
                normalized_sources.extend(source)
            else:
                normalized_sources.append(source)
        
        if normalized_sources != data_sources:
            self.logger.info(f"Normalized data sources: {normalized_sources}")
            data_sources = normalized_sources
        
        return search_terms, data_sources, explanation
    
    def _search_multiple_sources(self, search_terms: List[str], data_sources: List[str]) -> List[Dict[str, Any]]:
        """
        Search for datasets from multiple sources.
        
        Args:
            search_terms: List of search terms.
            data_sources: List of data sources.
            
        Returns:
            List[Dict[str, Any]]: List of datasets.
        """
        all_datasets = []
        
        # Log the input
        self.logger.info(f"_search_multiple_sources called with data_sources: {data_sources}")
        self.logger.info(f"data_sources type: {type(data_sources)}")
        for i, source in enumerate(data_sources):
            self.logger.info(f"data_source[{i}]: {source}, type: {type(source)}")
            if isinstance(source, list):
                for j, inner_source in enumerate(source):
                    self.logger.info(f"data_source[{i}][{j}]: {inner_source}, type: {type(inner_source)}")
        
        # Ensure data_sources is a list of strings, not a list of lists
        normalized_sources = []
        for source in data_sources:
            if isinstance(source, list):
                # If it's a list, add each element individually
                # Extract the string from the list
                for s in source:
                    if isinstance(s, list):
                        self.logger.warning(f"Found nested list in data_sources: {s}")
                        if s:
                            normalized_sources.append(str(s[0]))
                    else:
                        normalized_sources.append(str(s))
                self.logger.warning(f"Found list in data_sources: {source}, normalized to {[str(s) for s in source]}")
            else:
                normalized_sources.append(str(source))
        
        self.logger.info(f"Normalized sources: {normalized_sources}")
        
        # Filter out any sources that are not in the allowed list
        allowed_sources = ["kaggle", "huggingface", "google_dataset"]
        filtered_sources = []
        for source in normalized_sources:
            source_str = str(source).lower()
            for allowed in allowed_sources:
                if allowed in source_str:
                    filtered_sources.append(allowed)
                    break
        
        if filtered_sources != normalized_sources:
            self.logger.info(f"Filtered sources: {filtered_sources}")
            normalized_sources = filtered_sources
        
        # Create a thread pool
        with ThreadPoolExecutor(max_workers=len(normalized_sources)) as executor:
            # Submit search tasks
            future_to_source = {}
            for source in normalized_sources:
                # Ensure source is a string
                source_str = source
                if isinstance(source, list):
                    if source and len(source) > 0:
                        if isinstance(source[0], list):
                            if source[0] and len(source[0]) > 0:
                                source_str = str(source[0][0])
                            else:
                                source_str = "kaggle"  # Default to kaggle if empty nested list
                        else:
                            source_str = str(source[0])
                    else:
                        source_str = "kaggle"  # Default to kaggle if empty list
                    self.logger.warning(f"Source is still a list after normalization: {source}, using {source_str}")
                else:
                    source_str = str(source)
                
                # Clean up source string
                for connector in ["kaggle", "huggingface", "google_dataset"]:
                    if connector in source_str.lower():
                        source_str = connector
                        break
                
                self.logger.info(f"Submitting search task for source: {source_str}")
                future = executor.submit(self._search_source, source_str, search_terms)
                future_to_source[future] = source_str
            
            # Process results as they complete
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    datasets = future.result()
                    self.logger.info(f"Found {len(datasets)} datasets from {source}")
                    all_datasets.extend(datasets)
                except Exception as e:
                    self.logger.error(f"Error searching {source}: {e}")
                    import traceback
                    self.logger.error(f"Traceback: {traceback.format_exc()}")
        
        return all_datasets
    
    def _search_source(self, source: str, search_terms: List[str]) -> List[Dict[str, Any]]:
        """
        Search for datasets from a specific source.
        
        Args:
            source: Data source.
            search_terms: List of search terms.
            
        Returns:
            List[Dict[str, Any]]: List of datasets.
        """
        try:
            # Log the input type and value
            self.logger.info(f"_search_source called with source: {source}, type: {type(source)}")
            
            # Get connector - ensure source is a string, not a list
            if isinstance(source, list):
                self.logger.warning(f"Source is a list: {source}, extracting first element: {source[0] if source else None}")
                source = source[0] if source else ""  # Take the first element if it's a list
                
                # If the first element is still a list, extract from that too
                if isinstance(source, list):
                    self.logger.warning(f"First element is still a list: {source}, extracting from it: {source[0] if source else None}")
                    source = source[0] if source else ""
            
            # Convert source to string if it's not already
            if not isinstance(source, str):
                self.logger.warning(f"Converting non-string source to string: {source}")
                source = str(source)
            
            # Special case: if source is the string representation of a list like "['kaggle']" or '["kaggle"]'
            if source.startswith("[") and source.endswith("]"):
                self.logger.warning(f"Source is a string representation of a list: {source}")
                # Try to extract the actual name
                for connector in ["kaggle", "huggingface", "google_dataset"]:
                    if connector in source.lower():
                        self.logger.info(f"Extracted connector name '{connector}' from '{source}'")
                        source = connector
                        break
                
                # If we couldn't extract a connector name, try to parse the string as a list
                if source.startswith("[") and source.endswith("]"):
                    try:
                        # Handle both single and double quotes
                        if "'" in source:
                            # Handle ['kaggle']
                            extracted = source.replace("[", "").replace("]", "").replace("'", "").strip()
                        elif '"' in source:
                            # Handle ["kaggle"]
                            extracted = source.replace("[", "").replace("]", "").replace('"', "").strip()
                        else:
                            # Handle [kaggle]
                            extracted = source.replace("[", "").replace("]", "").strip()
                        
                        self.logger.info(f"Extracted name from string representation: {extracted}")
                        
                        # Check if the extracted name is a valid connector
                        for connector in ["kaggle", "huggingface", "google_dataset"]:
                            if connector in extracted.lower():
                                source = connector
                                self.logger.info(f"Matched extracted name to connector: {source}")
                                break
                    except Exception as e:
                        self.logger.warning(f"Failed to parse string representation of list: {source}, error: {e}")
            
            # Clean up source string - remove any additional text like "as per user's preference"
            if isinstance(source, str):
                # Common patterns to clean up
                original_source = source
                source = source.split(" as per ")[0].strip()
                source = source.split(" based on ")[0].strip()
                source = source.split(" according to ")[0].strip()
                source = source.split(" following ")[0].strip()
                
                # Remove any remaining text after the connector name
                for connector in ["kaggle", "huggingface", "google_dataset"]:
                    if connector in source.lower():
                        source = connector
                        break
                
                if source != original_source:
                    self.logger.info(f"Cleaned source from '{original_source}' to '{source}'")
            
            # Validate that the source is one of the allowed connectors
            allowed_connectors = ["kaggle", "huggingface", "google_dataset"]
            if source not in allowed_connectors:
                self.logger.warning(f"Source '{source}' is not in the allowed connectors list: {allowed_connectors}")
                # Try to match with any of the allowed connectors
                matched = False
                for connector in allowed_connectors:
                    if connector in source.lower():
                        source = connector
                        matched = True
                        self.logger.info(f"Matched source to connector: {source}")
                        break
                
                if not matched:
                    self.logger.error(f"Could not match source '{source}' to any allowed connector")
                    return []
            
            self.logger.info(f"Getting connector for source: {source}")
            connector = get_connector(source)
            
            # Search for each term
            all_results = []
            for term in search_terms:
                self.logger.info(f"Searching for term: {term} in source: {source}")
                results = connector.search_cached(term)
                all_results.extend(results)
            
            # Remove duplicates
            unique_results = {}
            for dataset in all_results:
                if dataset.id not in unique_results:
                    unique_results[dataset.id] = dataset
            
            # Convert to dictionaries
            return [dataset.to_dict() for dataset in unique_results.values()]
        except Exception as e:
            self.logger.error(f"Error searching {source}: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return []
    
    def _analyze_datasets(self, query: str, datasets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze datasets using LLM.
        
        Args:
            query: User query.
            datasets: List of datasets.
            
        Returns:
            Dict[str, Any]: Analysis results.
        """
        # Limit the number of datasets to analyze
        max_datasets = 10
        if len(datasets) > max_datasets:
            self.logger.info(f"Limiting analysis to {max_datasets} datasets")
            datasets = datasets[:max_datasets]
        
        # Generate prompt
        prompt = PromptTemplates.dataset_analysis_prompt(query, datasets)
        
        # Get response from LLM
        response = self._call_llm(prompt)
        
        # Process response
        analysis = ResponseProcessor.process_dataset_analysis(response)
        
        return analysis
    
    def _call_llm(self, prompt: str) -> str:
        """
        Call the LLM with a prompt.
        
        Args:
            prompt: Prompt for the LLM.
            
        Returns:
            str: LLM response.
        """
        try:
            # Call the LLM
            response = self.chat_model.invoke([HumanMessage(content=prompt)])
            
            # Extract content
            content = response.content
            
            return content
        except Exception as e:
            self.logger.error(f"Error calling LLM: {e}")
            return ""
