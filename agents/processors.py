"""
Response processors for the LLM agent.
"""
import re
import json
import ast
from typing import Dict, List, Any, Optional, Tuple, Set
from utils.logger import setup_logger

class ResponseProcessor:
    """Process responses from the LLM."""
    
    @staticmethod
    def process_search_terms(response: str) -> Tuple[List[str], List[str], str]:
        """
        Process search terms from LLM response.
        
        Args:
            response: LLM response.
            
        Returns:
            Tuple[List[str], List[str], str]: Search terms, data sources, and explanation.
        """
        logger = setup_logger("processors")
        
        logger.info(f"Processing search terms from response: {response[:100]}...")
        
        # First, try to extract JSON from the response
        try:
            # Look for JSON pattern with code block
            json_match = re.search(r"```json\s*(.+?)\s*```", response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
                logger.info(f"Found JSON string in response: {json_str}")
                
                try:
                    json_data = json.loads(json_str)
                    logger.info(f"Successfully parsed JSON data: {json_data}")
                    
                    # Extract search terms from JSON
                    search_terms = []
                    data_sources = []
                    explanation = ""
                    
                    if "Search Terms" in json_data:
                        search_terms = json_data["Search Terms"]
                        logger.info(f"Extracted search terms from JSON: {search_terms}")
                    
                    # Extract data sources from JSON
                    if "Data Sources" in json_data:
                        data_sources = json_data["Data Sources"]
                        logger.info(f"Extracted data sources from JSON: {data_sources}")
                    
                    # Extract explanation from JSON
                    if "Explanation" in json_data:
                        explanation = json_data["Explanation"]
                        logger.info(f"Extracted explanation from JSON: {explanation}")
                    
                    return search_terms, data_sources, explanation
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON string: {e}")
            
            # Try to find JSON without code block markers
            curly_match = re.search(r"(\{.+\})", response, re.DOTALL)
            if curly_match:
                json_str = curly_match.group(1)
                logger.info(f"Found potential JSON without code block: {json_str}")
                
                try:
                    json_data = json.loads(json_str)
                    logger.info(f"Successfully parsed JSON data without code block: {json_data}")
                    
                    # Extract search terms from JSON
                    search_terms = []
                    data_sources = []
                    explanation = ""
                    
                    if "Search Terms" in json_data:
                        search_terms = json_data["Search Terms"]
                        logger.info(f"Extracted search terms from JSON: {search_terms}")
                    
                    # Extract data sources from JSON
                    if "Data Sources" in json_data:
                        data_sources = json_data["Data Sources"]
                        logger.info(f"Extracted data sources from JSON: {data_sources}")
                    
                    # Extract explanation from JSON
                    if "Explanation" in json_data:
                        explanation = json_data["Explanation"]
                        logger.info(f"Extracted explanation from JSON: {explanation}")
                    
                    return search_terms, data_sources, explanation
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON without code block: {e}")
        except Exception as e:
            logger.warning(f"Failed to extract JSON from response: {e}")
        
        # If JSON parsing fails, fall back to regex extraction
        # Extract search terms
        search_terms_match = re.search(r"Search Terms:\s*(.+?)(?:\n|$)", response, re.DOTALL)
        search_terms = []
        if search_terms_match:
            terms_text = search_terms_match.group(1).strip()
            logger.info(f"Extracted search terms text: {terms_text}")
            
            # Try to parse as JSON array first
            try:
                if terms_text.startswith("[") and terms_text.endswith("]"):
                    # Try to parse as JSON
                    parsed_terms = json.loads(terms_text)
                    if isinstance(parsed_terms, list):
                        search_terms = parsed_terms
                        logger.info(f"Successfully parsed search terms as JSON array: {search_terms}")
                    else:
                        logger.warning(f"Parsed JSON is not a list: {parsed_terms}")
                        search_terms = [terms_text]
            except json.JSONDecodeError:
                # Try to parse as Python literal
                try:
                    if terms_text.startswith("[") and terms_text.endswith("]"):
                        parsed_terms = ast.literal_eval(terms_text)
                        if isinstance(parsed_terms, list):
                            search_terms = parsed_terms
                            logger.info(f"Successfully parsed search terms as Python literal: {search_terms}")
                        else:
                            logger.warning(f"Parsed literal is not a list: {parsed_terms}")
                            search_terms = [terms_text]
                    else:
                        # Fall back to regular parsing
                        if "," in terms_text:
                            search_terms = [term.strip() for term in terms_text.split(",")]
                        elif "\n" in terms_text:
                            search_terms = [term.strip().lstrip("- ") for term in terms_text.split("\n")]
                        else:
                            search_terms = [terms_text]
                except (SyntaxError, ValueError):
                    # Fall back to regular parsing
                    if "," in terms_text:
                        search_terms = [term.strip() for term in terms_text.split(",")]
                    elif "\n" in terms_text:
                        search_terms = [term.strip().lstrip("- ") for term in terms_text.split("\n")]
                    else:
                        search_terms = [terms_text]
        
        # Extract data sources
        data_sources_match = re.search(r"Data Sources:\s*(.+?)(?:\n|$)", response, re.DOTALL)
        data_sources = []
        if data_sources_match:
            sources_text = data_sources_match.group(1).strip()
            logger.info(f"Extracted data sources text: {sources_text}")
            
            # Handle different formats
            if "," in sources_text:
                data_sources = [source.strip().lower() for source in sources_text.split(",")]
            elif "\n" in sources_text:
                data_sources = [source.strip().lstrip("- ").lower() for source in sources_text.split("\n")]
            else:
                data_sources = [sources_text.lower()]
            
            # Clean up data sources - remove any additional text like "as per user's preference"
            cleaned_sources = []
            for source in data_sources:
                # Extract just the connector name
                if isinstance(source, str):
                    # Common patterns to clean up
                    source = source.split(" as per ")[0].strip()
                    source = source.split(" based on ")[0].strip()
                    source = source.split(" according to ")[0].strip()
                    source = source.split(" following ")[0].strip()
                    
                    # Remove any remaining text after the connector name
                    for connector in ["kaggle", "huggingface", "google_dataset"]:
                        if connector in source.lower():
                            source = connector
                            break
                
                cleaned_sources.append(source)
            
            logger.info(f"Cleaned data sources: {cleaned_sources}")
            data_sources = cleaned_sources
            
            logger.info(f"Initial data sources: {data_sources}")
            
            # Ensure each data source is a string, not a list
            normalized_sources = []
            for source in data_sources:
                if isinstance(source, list):
                    logger.warning(f"Found list in data sources: {source}")
                    if source:
                        normalized_sources.append(source[0])
                    else:
                        logger.warning("Empty list in data sources")
                else:
                    normalized_sources.append(source)
            
            if normalized_sources != data_sources:
                logger.info(f"Normalized data sources: {normalized_sources}")
                data_sources = normalized_sources
            
            # Check for any remaining lists and convert to strings
            for i, source in enumerate(data_sources):
                if isinstance(source, list):
                    logger.warning(f"Still found list at index {i}: {source}")
                    data_sources[i] = str(source)
        
        # Extract explanation
        explanation_match = re.search(r"Explanation:\s*(.+?)(?:\n\n|$)", response, re.DOTALL)
        explanation = explanation_match.group(1).strip() if explanation_match else ""
        
        logger.info(f"Final search terms: {search_terms}")
        logger.info(f"Final data sources: {data_sources}")
        
        return search_terms, data_sources, explanation
    
    @staticmethod
    def process_dataset_analysis(response: str) -> Dict[str, Any]:
        """
        Process dataset analysis from LLM response.
        
        Args:
            response: LLM response.
            
        Returns:
            Dict[str, Any]: Processed analysis.
        """
        # Extract ranking
        ranking_match = re.search(r"Ranking:(.*?)(?:Detailed Analysis:|$)", response, re.DOTALL)
        ranking = []
        if ranking_match:
            ranking_text = ranking_match.group(1).strip()
            ranking_lines = [line.strip() for line in ranking_text.split("\n") if line.strip()]
            
            for line in ranking_lines:
                # Extract dataset name and explanation
                rank_match = re.match(r"\d+\.\s*([^-]+)\s*-\s*(.+)", line)
                if rank_match:
                    dataset_name = rank_match.group(1).strip()
                    explanation = rank_match.group(2).strip()
                    ranking.append({"name": dataset_name, "explanation": explanation})
        
        # Extract detailed analysis
        analysis_match = re.search(r"Detailed Analysis:(.*?)(?:Overall Recommendation:|$)", response, re.DOTALL)
        analysis = {}
        if analysis_match:
            analysis_text = analysis_match.group(1).strip()
            
            # Split by dataset names
            dataset_sections = re.split(r"\n\s*\[([^\]]+)\]:\s*\n", analysis_text)
            
            # Process each dataset section
            for i in range(1, len(dataset_sections), 2):
                dataset_name = dataset_sections[i].strip()
                dataset_analysis = dataset_sections[i+1].strip()
                
                # Extract relevance
                relevance_match = re.search(r"Relevance:\s*([^\n]+)", dataset_analysis)
                relevance = relevance_match.group(1).strip() if relevance_match else "Unknown"
                
                # Extract strengths
                strengths_match = re.search(r"Strengths:\s*([^\n]+)", dataset_analysis)
                strengths = strengths_match.group(1).strip() if strengths_match else "Unknown"
                
                # Extract limitations
                limitations_match = re.search(r"Limitations:\s*([^\n]+)", dataset_analysis)
                limitations = limitations_match.group(1).strip() if limitations_match else "Unknown"
                
                # Extract recommendation
                recommendation_match = re.search(r"Recommendation:\s*([^\n]+)", dataset_analysis)
                recommendation = recommendation_match.group(1).strip() if recommendation_match else "Unknown"
                
                analysis[dataset_name] = {
                    "relevance": relevance,
                    "strengths": strengths,
                    "limitations": limitations,
                    "recommendation": recommendation,
                }
        
        # Extract overall recommendation
        recommendation_match = re.search(r"Overall Recommendation:\s*(.+?)(?:\n\n|$)", response, re.DOTALL)
        overall_recommendation = recommendation_match.group(1).strip() if recommendation_match else ""
        
        return {
            "ranking": ranking,
            "analysis": analysis,
            "overall_recommendation": overall_recommendation,
        }
    
    @staticmethod
    def process_dataset_recommendation(response: str) -> str:
        """
        Process dataset recommendation from LLM response.
        
        Args:
            response: LLM response.
            
        Returns:
            str: Processed recommendation.
        """
        # For recommendation, we just return the full response as it should be concise
        return response.strip()
    
    @staticmethod
    def extract_json_from_response(response: str) -> Optional[Dict[str, Any]]:
        """
        Extract JSON from LLM response.
        
        Args:
            response: LLM response.
            
        Returns:
            Optional[Dict[str, Any]]: Extracted JSON or None if not found.
        """
        # Look for JSON pattern
        json_match = re.search(r"```json\s*(.+?)\s*```", response, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                return None
        
        # Try to find JSON without code block markers
        try:
            # Look for patterns that might indicate JSON
            curly_match = re.search(r"(\{.+\})", response, re.DOTALL)
            if curly_match:
                json_str = curly_match.group(1)
                return json.loads(json_str)
        except json.JSONDecodeError:
            pass
        
        return None
