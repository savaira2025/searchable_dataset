"""
Dataset card component for the Streamlit application.
"""
import streamlit as st
from typing import Dict, Any, Optional

def dataset_card(dataset: Dict[str, Any], show_details: bool = False) -> None:
    """
    Display a dataset card.
    
    Args:
        dataset: Dataset information.
        show_details: Whether to show details.
    """
    # Create a card-like container
    with st.container():
        # Add a border and padding
        st.markdown(
            """
            <style>
            .dataset-card {
                border: 1px solid #ddd;
                border-radius: 5px;
                padding: 15px;
                margin-bottom: 15px;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        
        # Start the card
        st.markdown('<div class="dataset-card">', unsafe_allow_html=True)
        
        # Display dataset name and source
        st.markdown(f"### {dataset['name']}")
        st.markdown(f"**Source:** {dataset['source']}")
        
        # Display description (truncated if not showing details)
        description = dataset.get('description', 'No description available')
        if not show_details and len(description) > 200:
            st.markdown(f"**Description:** {description[:200]}...")
        else:
            st.markdown(f"**Description:** {description}")
        
        # Display additional information if available
        if show_details:
            if dataset.get('url'):
                st.markdown(f"**URL:** [{dataset['url']}]({dataset['url']})")
            
            if dataset.get('size'):
                st.markdown(f"**Size:** {dataset['size']}")
            
            if dataset.get('format'):
                st.markdown(f"**Format:** {dataset['format']}")
            
            if dataset.get('license'):
                st.markdown(f"**License:** {dataset['license']}")
            
            if dataset.get('tags'):
                st.markdown(f"**Tags:** {', '.join(dataset['tags'])}")
        
        # End the card
        st.markdown('</div>', unsafe_allow_html=True)

def dataset_grid(datasets: list, cols: int = 2) -> None:
    """
    Display a grid of dataset cards.
    
    Args:
        datasets: List of datasets.
        cols: Number of columns.
    """
    # Create columns
    columns = st.columns(cols)
    
    # Display datasets in columns
    for i, dataset in enumerate(datasets):
        with columns[i % cols]:
            dataset_card(dataset)
            
            # Add a button to view details
            if st.button(f"View Details", key=f"view_{i}"):
                st.session_state.selected_dataset = dataset
