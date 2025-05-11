"""
Main Streamlit application for the SearchableDataset.
"""
import os
import json
import time
import streamlit as st
import pandas as pd
import plotly.express as px
from typing import Dict, List, Any, Optional

from agents import LLMAgent
from data_sources import get_connector, CONNECTORS
from utils import config
from utils.logger import setup_logger
from app.components import download_button, downloads_sidebar

# Set up logger
log = setup_logger("streamlit_app")

# Set page configuration
st.set_page_config(
    page_title="SearchableDataset",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize session state
if "search_results" not in st.session_state:
    st.session_state.search_results = None
if "selected_dataset" not in st.session_state:
    st.session_state.selected_dataset = None
if "agent" not in st.session_state:
    st.session_state.agent = LLMAgent()
if "search_history" not in st.session_state:
    st.session_state.search_history = []

# Sidebar
with st.sidebar:
    st.title("SearchableDataset")
    st.markdown("Search for datasets from multiple sources using LLM as an agent.")
    
    # API key configuration
    st.subheader("API Keys")
    openai_api_key = st.text_input("OpenAI API Key", value=config.OPENAI_API_KEY, type="password")
    kaggle_username = st.text_input("Kaggle Username", value=config.KAGGLE_USERNAME)
    kaggle_key = st.text_input("Kaggle Key", value=config.KAGGLE_KEY, type="password")
    huggingface_api_key = st.text_input("Hugging Face API Key", value=config.HUGGINGFACE_API_KEY, type="password")
    
    # Update config if keys are changed
    if (openai_api_key != config.OPENAI_API_KEY or
        kaggle_username != config.KAGGLE_USERNAME or
        kaggle_key != config.KAGGLE_KEY or
        huggingface_api_key != config.HUGGINGFACE_API_KEY):
        
        config.OPENAI_API_KEY = openai_api_key
        config.KAGGLE_USERNAME = kaggle_username
        config.KAGGLE_KEY = kaggle_key
        config.HUGGINGFACE_API_KEY = huggingface_api_key
        
        # Update environment variables
        os.environ["OPENAI_API_KEY"] = openai_api_key
        os.environ["KAGGLE_USERNAME"] = kaggle_username
        os.environ["KAGGLE_KEY"] = kaggle_key
        os.environ["HUGGINGFACE_API_KEY"] = huggingface_api_key
        
        # Reinitialize agent
        st.session_state.agent = LLMAgent()
    
    # Data source selection
    st.subheader("Data Sources")
    selected_sources = {}
    for source in CONNECTORS.keys():
        selected_sources[source] = st.checkbox(source.capitalize(), value=True)
    
    # Search history
    if st.session_state.search_history:
        st.subheader("Search History")
        for i, query in enumerate(st.session_state.search_history):
            if st.button(f"{query}", key=f"history_{i}"):
                st.session_state.search_query = query
    
    # Display downloads in sidebar
    downloads_sidebar()

# Main content
st.title("Dataset Search")

# Search form
with st.form(key="search_form"):
    search_query = st.text_input("Enter your search query", key="search_query")
    col1, col2 = st.columns([3, 1])
    with col1:
        search_button = st.form_submit_button("Search")
    with col2:
        clear_button = st.form_submit_button("Clear")

# Handle clear button
if clear_button:
    st.session_state.search_results = None
    st.session_state.selected_dataset = None
    st.session_state.search_query = ""

# Handle search button
if search_button and search_query:
    # Add to search history if not already present
    if search_query not in st.session_state.search_history:
        st.session_state.search_history.append(search_query)
        # Keep only the last 10 searches
        if len(st.session_state.search_history) > 10:
            st.session_state.search_history.pop(0)
    
    # Show loading spinner
    with st.spinner("Searching for datasets..."):
        try:
            # Get selected data sources
            data_sources = [source for source, selected in selected_sources.items() if selected]
            
            # Create context with user preferences
            context = {
                "user_preferences": {
                    "data_sources": data_sources,
                },
                "previous_searches": st.session_state.search_history,
            }
            
            # Search datasets
            results = st.session_state.agent.search_datasets(search_query, context)
            st.session_state.search_results = results
        except Exception as e:
            st.error(f"Error searching datasets: {e}")
            log.error(f"Error searching datasets: {e}")

# Display search results
if st.session_state.search_results:
    results = st.session_state.search_results
    
    # Display search terms and explanation
    st.subheader("Search Analysis")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Search Terms:**")
        for term in results["search_terms"]:
            st.markdown(f"- {term}")
    with col2:
        st.markdown("**Data Sources:**")
        for source in results["data_sources"]:
            st.markdown(f"- {source.capitalize()}")
    
    st.markdown("**Explanation:**")
    st.markdown(results["explanation"])
    
    # Display dataset analysis
    if "analysis" in results and results["analysis"]:
        st.subheader("Dataset Analysis")
        
        # Display ranking
        if "ranking" in results["analysis"] and results["analysis"]["ranking"]:
            st.markdown("**Ranking:**")
            for i, item in enumerate(results["analysis"]["ranking"]):
                st.markdown(f"{i+1}. **{item['name']}** - {item['explanation']}")
        
        # Display overall recommendation
        if "overall_recommendation" in results["analysis"] and results["analysis"]["overall_recommendation"]:
            st.markdown("**Overall Recommendation:**")
            st.markdown(results["analysis"]["overall_recommendation"])
    
    # Display datasets
    st.subheader("Datasets")
    
    # Create a DataFrame for easier display
    if results["datasets"]:
        df = pd.DataFrame(results["datasets"])
        
        # Select columns to display
        display_columns = ["name", "source", "description"]
        if "size" in df.columns:
            display_columns.append("size")
        if "license" in df.columns:
            display_columns.append("license")
        
        # Display as a table
        st.dataframe(df[display_columns], use_container_width=True)
        
        # Display datasets with download buttons
        st.subheader("Quick Actions")
        
        # Create rows of datasets with download buttons
        num_cols = 3  # Number of columns in the grid
        datasets_list = results["datasets"]
        
        # Create rows of columns
        for i in range(0, len(datasets_list), num_cols):
            cols = st.columns(num_cols)
            
            # Fill each column with a dataset
            for j in range(num_cols):
                idx = i + j
                if idx < len(datasets_list):
                    with cols[j]:
                        dataset_item = datasets_list[idx]
                        st.markdown(f"**{dataset_item['name']}**")
                        download_button(dataset_item, key=f"search_result_{idx}")
        
        # Dataset selection
        selected_index = st.selectbox(
            "Select a dataset for more details",
            options=range(len(df)),
            format_func=lambda x: df.iloc[x]["name"],
        )
        
        if selected_index is not None:
            st.session_state.selected_dataset = df.iloc[selected_index].to_dict()
    else:
        st.info("No datasets found. Try a different search query or select different data sources.")

# Display selected dataset details
if st.session_state.selected_dataset:
    dataset = st.session_state.selected_dataset
    
    # Display dataset title and download button
    col1, col2 = st.columns([3, 1])
    with col1:
        st.subheader(f"Dataset Details: {dataset['name']}")
    with col2:
        # Add download button
        download_button(dataset, key="main_page")
    
    # Create tabs for different sections
    tab1, tab2, tab3 = st.tabs(["Overview", "Metadata", "Analysis"])
    
    with tab1:
        # Display basic information
        st.markdown(f"**Description:** {dataset['description']}")
        st.markdown(f"**Source:** {dataset['source']}")
        
        if "url" in dataset and dataset["url"]:
            st.markdown(f"**URL:** [{dataset['url']}]({dataset['url']})")
        
        if "size" in dataset and dataset["size"]:
            st.markdown(f"**Size:** {dataset['size']}")
        
        if "format" in dataset and dataset["format"]:
            st.markdown(f"**Format:** {dataset['format']}")
        
        if "license" in dataset and dataset["license"]:
            st.markdown(f"**License:** {dataset['license']}")
        
        if "tags" in dataset and dataset["tags"]:
            st.markdown("**Tags:**")
            st.write(", ".join(dataset["tags"]))
    
    with tab2:
        # Display metadata
        if "metadata" in dataset and dataset["metadata"]:
            for key, value in dataset["metadata"].items():
                if isinstance(value, (dict, list)):
                    st.markdown(f"**{key.capitalize()}:**")
                    st.json(value)
                else:
                    st.markdown(f"**{key.capitalize()}:** {value}")
        else:
            st.info("No additional metadata available.")
    
    with tab3:
        # Display analysis
        if (st.session_state.search_results and 
            "analysis" in st.session_state.search_results and 
            "analysis" in st.session_state.search_results["analysis"]):
            
            analysis = st.session_state.search_results["analysis"]["analysis"]
            
            # Find the analysis for this dataset
            dataset_analysis = None
            for name, data in analysis.items():
                if name.strip() == dataset["name"].strip():
                    dataset_analysis = data
                    break
            
            if dataset_analysis:
                st.markdown(f"**Relevance:** {dataset_analysis['relevance']}")
                st.markdown(f"**Strengths:** {dataset_analysis['strengths']}")
                st.markdown(f"**Limitations:** {dataset_analysis['limitations']}")
                st.markdown(f"**Recommendation:** {dataset_analysis['recommendation']}")
            else:
                st.info("No specific analysis available for this dataset.")
        else:
            st.info("No analysis available.")

# Footer
st.markdown("---")
st.markdown("SearchableDataset - Powered by LLM and Streamlit")
