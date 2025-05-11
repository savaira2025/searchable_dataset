"""
Download button component for the Streamlit application.
"""
import os
import streamlit as st
import time
from typing import Dict, Any, Optional, List, Tuple

from data_sources import get_connector
from utils.downloader import downloader, DownloadStatus
from utils.logger import setup_logger

# Set up logger
log = setup_logger("download_button")

def format_size(bytes: int) -> str:
    """
    Format size in bytes to human-readable format.
    
    Args:
        bytes: Size in bytes.
        
    Returns:
        str: Formatted size.
    """
    if bytes is None or bytes == 0:
        return "Unknown"
    
    # Convert to appropriate unit
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(bytes)
    unit_index = 0
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    return f"{size:.2f} {units[unit_index]}"

def format_time(seconds: float) -> str:
    """
    Format time in seconds to human-readable format.
    
    Args:
        seconds: Time in seconds.
        
    Returns:
        str: Formatted time.
    """
    if seconds is None or seconds < 0:
        return "Unknown"
    
    if seconds < 60:
        return f"{seconds:.0f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.0f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"

def download_button(dataset: Dict[str, Any], key: str = "download") -> None:
    """
    Display a download button for a dataset.
    
    Args:
        dataset: Dataset information.
        key: Unique key for the button.
    """
    # Initialize session state for downloads if not exists
    if "downloads" not in st.session_state:
        st.session_state.downloads = {}
    
    # Get dataset information
    dataset_id = dataset.get("id")
    dataset_name = dataset.get("name")
    source = dataset.get("source")
    
    if not dataset_id or not source:
        st.error("Dataset information is incomplete.")
        return
    
    # Check if already downloading
    download_id = None
    for d_id, d_info in st.session_state.downloads.items():
        if d_info.get("dataset_id") == dataset_id:
            download_id = d_id
            break
    
    # If not downloading, show download button
    if not download_id:
        if st.button(f"Download Dataset", key=f"{key}_{dataset_id}"):
            try:
                # Get the connector for this source
                connector = get_connector(source.lower())
                if not connector:
                    st.error(f"Connector not found for source: {source}")
                    return
                
                # Ensure Datasets directory exists
                os.makedirs("Datasets", exist_ok=True)
                
                # Start download
                download_id = connector.download_dataset(dataset_id)
                
                if download_id:
                    # Store download information
                    st.session_state.downloads[download_id] = {
                        "dataset_id": dataset_id,
                        "dataset_name": dataset_name,
                        "source": source,
                        "status": DownloadStatus.PENDING,
                        "progress": 0.0,
                    }
                    
                    # Force rerun to show progress
                    st.rerun()
                else:
                    st.error(f"Failed to start download for dataset: {dataset_name}")
            except Exception as e:
                st.error(f"Error starting download: {e}")
                log.error(f"Error starting download: {e}")
    else:
        # Show download progress
        download_progress(download_id)

def download_progress(download_id: str) -> None:
    """
    Display download progress.
    
    Args:
        download_id: Download task ID.
    """
    # Get download status
    status = downloader.get_download_status(download_id)
    
    if not status:
        # Download not found, remove from session state
        if download_id in st.session_state.downloads:
            del st.session_state.downloads[download_id]
        return
    
    # Update session state
    st.session_state.downloads[download_id] = status
    
    # Display progress
    dataset_name = status.get("dataset_name", "Unknown")
    progress = status.get("progress", 0.0)
    status_text = status.get("status", DownloadStatus.PENDING)
    
    # Create a container for the progress
    with st.container():
        # Display dataset name
        st.markdown(f"**{dataset_name}**")
        
        # Display progress bar
        progress_bar = st.progress(progress)
        
        # Display status and details
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"Status: **{status_text.capitalize()}**")
            
            if status_text == DownloadStatus.DOWNLOADING:
                # Show cancel button
                # Add timestamp to key to ensure uniqueness
                import time
                timestamp = int(time.time() * 1000)
                if st.button("Cancel", key=f"cancel_{download_id}_{timestamp}"):
                    downloader.cancel_download(download_id)
                    st.rerun()
            
            elif status_text == DownloadStatus.COMPLETED:
                # Show open button
                file_path = status.get("file_path")
                if file_path and os.path.exists(file_path):
                    st.markdown(f"Saved to: **{file_path}**")
                
                # Clear button
                import time
                timestamp = int(time.time() * 1000)
                if st.button("Clear", key=f"clear_{download_id}_{timestamp}"):
                    del st.session_state.downloads[download_id]
                    st.rerun()
            
            elif status_text == DownloadStatus.FAILED:
                # Show error
                error = status.get("error", "Unknown error")
                st.error(f"Error: {error}")
                
                # Retry button
                import time
                timestamp = int(time.time() * 1000)
                if st.button("Retry", key=f"retry_{download_id}_{timestamp}"):
                    # Get dataset information
                    dataset_id = status.get("dataset_id")
                    source = status.get("source")
                    
                    # Remove old download
                    del st.session_state.downloads[download_id]
                    
                    # Start new download
                    connector = get_connector(source.lower())
                    if connector:
                        new_download_id = connector.download_dataset(dataset_id)
                        if new_download_id:
                            st.session_state.downloads[new_download_id] = {
                                "dataset_id": dataset_id,
                                "dataset_name": status.get("dataset_name"),
                                "source": source,
                                "status": DownloadStatus.PENDING,
                                "progress": 0.0,
                            }
                    
                    st.rerun()
                
                # Clear button
                timestamp2 = int(time.time() * 1000) + 1
                if st.button("Clear", key=f"clear_error_{download_id}_{timestamp2}"):
                    del st.session_state.downloads[download_id]
                    st.rerun()
            
            elif status_text == DownloadStatus.CANCELLED:
                # Show cancelled message
                st.warning("Download cancelled")
                
                # Clear button
                import time
                timestamp = int(time.time() * 1000)
                if st.button("Clear", key=f"clear_cancelled_{download_id}_{timestamp}"):
                    del st.session_state.downloads[download_id]
                    st.rerun()
        
        with col2:
            if status_text == DownloadStatus.DOWNLOADING:
                # Show download details
                file_size = status.get("file_size", 0)
                downloaded_size = status.get("downloaded_size", 0)
                speed = status.get("speed", 0)
                eta = status.get("eta", 0)
                
                if file_size > 0:
                    st.markdown(f"Size: {format_size(downloaded_size)} / {format_size(file_size)}")
                else:
                    st.markdown(f"Downloaded: {format_size(downloaded_size)}")
                
                if speed > 0:
                    st.markdown(f"Speed: {format_size(speed)}/s")
                
                if eta > 0:
                    st.markdown(f"ETA: {format_time(eta)}")

def downloads_sidebar() -> None:
    """Display active downloads in the sidebar."""
    if "downloads" not in st.session_state:
        st.session_state.downloads = {}
    
    # Get all downloads
    downloads = st.session_state.downloads
    
    if not downloads:
        return
    
    # Display downloads section
    st.sidebar.markdown("---")
    st.sidebar.subheader("Downloads")
    
    # Update download statuses
    for download_id in list(downloads.keys()):
        status = downloader.get_download_status(download_id)
        if status:
            st.session_state.downloads[download_id] = status
        else:
            # Download not found, remove from session state
            del st.session_state.downloads[download_id]
    
    # Group downloads by status
    active_downloads = []
    completed_downloads = []
    failed_downloads = []
    
    for download_id, status in downloads.items():
        status_text = status.get("status", DownloadStatus.PENDING)
        
        if status_text in [DownloadStatus.PENDING, DownloadStatus.DOWNLOADING]:
            active_downloads.append((download_id, status))
        elif status_text == DownloadStatus.COMPLETED:
            completed_downloads.append((download_id, status))
        else:  # FAILED or CANCELLED
            failed_downloads.append((download_id, status))
    
    # Display active downloads
    if active_downloads:
        st.sidebar.markdown("**Active Downloads:**")
        for download_id, status in active_downloads:
            dataset_name = status.get("dataset_name", "Unknown")
            progress = status.get("progress", 0.0)
            
            # Display progress
            st.sidebar.markdown(f"{dataset_name}")
            st.sidebar.progress(progress)
    
    # Display completed downloads
    if completed_downloads:
        st.sidebar.markdown("**Completed Downloads:**")
        for download_id, status in completed_downloads:
            dataset_name = status.get("dataset_name", "Unknown")
            file_path = status.get("file_path", "")
            
            # Display completed download
            st.sidebar.markdown(f"✅ {dataset_name}")
    
    # Display failed downloads
    if failed_downloads:
        st.sidebar.markdown("**Failed Downloads:**")
        for download_id, status in failed_downloads:
            dataset_name = status.get("dataset_name", "Unknown")
            status_text = status.get("status", "").capitalize()
            
            # Display failed download
            st.sidebar.markdown(f"❌ {dataset_name} ({status_text})")
    
    # Clear all button
    import time
    timestamp = int(time.time() * 1000)
    if st.sidebar.button("Clear All Downloads", key=f"clear_all_{timestamp}"):
        st.session_state.downloads = {}
        st.rerun()
