"""
Dataset details page for the Streamlit application.
"""
import os
import streamlit as st
import pandas as pd
import plotly.express as px
from typing import Dict, Any, Optional

from utils import setup_logger
from app.components import download_button, downloads_sidebar

# Set up logger
log = setup_logger("dataset_details")

# Set page configuration
st.set_page_config(
    page_title="Dataset Details",
    page_icon="📊",
    layout="wide",
)

# Function to display dataset details
def display_dataset_details(dataset: Dict[str, Any]) -> None:
    """
    Display dataset details.
    
    Args:
        dataset: Dataset information.
    """
    # Display downloads in sidebar
    downloads_sidebar()
    
    # Display dataset title and download button
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title(f"Dataset: {dataset['name']}")
    with col2:
        # Add download button
        download_button(dataset, key="details_page")
    
    # Create tabs for different sections
    tab1, tab2, tab3 = st.tabs(["Overview", "Metadata", "Visualization"])
    
    with tab1:
        # Display basic information
        st.subheader("Overview")
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
        st.subheader("Metadata")
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
        # Display visualization options
        st.subheader("Visualization")
        st.info("To visualize this dataset, you would need to download it first.")
        
        # Add download button
        download_button(dataset, key="viz_page")
        
        # Show a sample visualization (placeholder)
        st.markdown("### Sample Visualization")
        st.markdown("This is a placeholder visualization. The actual visualization would depend on the dataset content.")
        
        # Create a sample chart
        data = {
            'Category': ['A', 'B', 'C', 'D', 'E'],
            'Value': [5, 7, 3, 9, 6]
        }
        df = pd.DataFrame(data)
        fig = px.bar(df, x='Category', y='Value', title="Sample Chart")
        st.plotly_chart(fig)

# Main function
def main():
    # Check if dataset is selected in session state
    if "selected_dataset" in st.session_state and st.session_state.selected_dataset:
        display_dataset_details(st.session_state.selected_dataset)
    else:
        st.info("No dataset selected. Please go back to the main page and select a dataset.")
        
        # Add a button to go back to the main page
        if st.button("Go to Main Page"):
            st.switch_page("app/main.py")

if __name__ == "__main__":
    main()
