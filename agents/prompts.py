"""
Prompt templates for the LLM agent.
"""
from typing import Dict, List, Any

class PromptTemplates:
    """Prompt templates for the LLM agent."""
    
    @staticmethod
    def dataset_search_prompt(query: str, context: Dict[str, Any] = None) -> str:
        """
        Generate a prompt for dataset search.
        
        Args:
            query: User query.
            context: Additional context.
            
        Returns:
            str: Prompt for the LLM.
        """
        context = context or {}
        
        prompt = f"""
        You are a helpful assistant that helps users find datasets. Your task is to understand the user's query and formulate appropriate search terms to find relevant datasets.
        
        User Query: {query}
        
        Based on the user's query, what would be the most effective search terms to find relevant datasets? Consider the following:
        
        1. What are the key concepts or topics in the query?
        2. What specific data types or formats might be relevant?
        3. Are there any domain-specific terms that should be included?
        4. Are there any filters or constraints mentioned in the query?
        
        Provide your response in the following format:
        
        Search Terms: ["term1", "term2", "term3"]
        Explanation: brief explanation of your reasoning
        Data Sources: ["kaggle", "huggingface", "google_dataset"]
        
        Note: The search terms and data sources should be provided as arrays with each item in quotes.
        """
        
        # Add additional context if available
        if context.get("previous_searches"):
            prompt += f"\n\nPrevious searches: {context['previous_searches']}"
        
        if context.get("user_preferences"):
            user_prefs = context["user_preferences"]
            prompt += f"\n\nUser preferences: {user_prefs}"
            
            # If user has specified data sources, make it clear in the prompt
            if "data_sources" in user_prefs and user_prefs["data_sources"]:
                prompt += f"""
                
                CRITICAL INSTRUCTION: The user has specifically selected ONLY the following data sources: {user_prefs["data_sources"]}
                You MUST ONLY suggest these exact data sources in your response. DO NOT suggest any other data sources.
                Your Data Sources response MUST be EXACTLY: {user_prefs["data_sources"]}
                Any other data sources will be ignored, and only the user-selected ones will be used.
                """
        
        return prompt
    
    @staticmethod
    def dataset_analysis_prompt(query: str, datasets: List[Dict[str, Any]]) -> str:
        """
        Generate a prompt for dataset analysis.
        
        Args:
            query: User query.
            datasets: List of datasets.
            
        Returns:
            str: Prompt for the LLM.
        """
        # Format datasets as a string
        datasets_str = ""
        for i, dataset in enumerate(datasets):
            datasets_str += f"""
            Dataset {i+1}:
            - Name: {dataset.get('name', 'Unknown')}
            - Description: {dataset.get('description', 'No description available')}
            - Source: {dataset.get('source', 'Unknown')}
            - Size: {dataset.get('size', 'Unknown')}
            - Format: {dataset.get('format', 'Unknown')}
            - License: {dataset.get('license', 'Unknown')}
            - Tags: {', '.join(dataset.get('tags', []))}
            - URL: {dataset.get('url', 'Unknown')}
            """
        
        prompt = f"""
        You are a helpful assistant that helps users find and analyze datasets. Your task is to analyze the datasets below and determine which ones are most relevant to the user's query.
        
        User Query: {query}
        
        Datasets:
        {datasets_str}
        
        Based on the user's query and the available datasets, please:
        
        1. Rank the datasets in order of relevance to the query.
        2. For each dataset, explain why it is relevant or not relevant to the query.
        3. Highlight any key features or limitations of each dataset.
        4. Recommend the best dataset(s) for the user's needs.
        
        Provide your response in the following format:
        
        Ranking:
        1. [Dataset Name] - [Brief explanation of relevance]
        2. [Dataset Name] - [Brief explanation of relevance]
        ...
        
        Detailed Analysis:
        [Dataset Name 1]:
        - Relevance: [High/Medium/Low]
        - Strengths: [List of strengths]
        - Limitations: [List of limitations]
        - Recommendation: [Recommendation for using this dataset]
        
        [Dataset Name 2]:
        ...
        
        Overall Recommendation:
        [Your overall recommendation based on the user's query and available datasets]
        """
        
        return prompt
    
    @staticmethod
    def dataset_recommendation_prompt(query: str, datasets: List[Dict[str, Any]]) -> str:
        """
        Generate a prompt for dataset recommendation.
        
        Args:
            query: User query.
            datasets: List of datasets.
            
        Returns:
            str: Prompt for the LLM.
        """
        # Format datasets as a string (abbreviated version)
        datasets_str = ""
        for i, dataset in enumerate(datasets[:5]):  # Limit to top 5 for brevity
            datasets_str += f"""
            Dataset {i+1}:
            - Name: {dataset.get('name', 'Unknown')}
            - Description: {dataset.get('description', 'No description available')[:100]}...
            - Source: {dataset.get('source', 'Unknown')}
            - Tags: {', '.join(dataset.get('tags', [])[:5])}
            """
        
        prompt = f"""
        You are a helpful assistant that helps users find datasets. Based on the user's query and the available datasets, provide a concise recommendation.
        
        User Query: {query}
        
        Top Datasets:
        {datasets_str}
        
        Provide a brief recommendation of which dataset(s) would be most suitable for the user's needs and why. Keep your response concise and focused on the most relevant options.
        """
        
        return prompt
