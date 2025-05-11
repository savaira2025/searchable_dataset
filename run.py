#!/usr/bin/env python
"""
Script to run the SearchableDataset application.
"""
import os
import sys
import argparse
import subprocess
from typing import List, Optional

# Add the current directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

def run_streamlit(args: List[str]) -> None:
    """
    Run the Streamlit application.
    
    Args:
        args: Command-line arguments.
    """
    # Run the Streamlit application
    cmd = ["streamlit", "run", "app/main.py"] + args
    subprocess.run(cmd)

def run_tests(args: List[str]) -> None:
    """
    Run the tests.
    
    Args:
        args: Command-line arguments.
    """
    # Run the tests
    cmd = ["pytest"] + args
    subprocess.run(cmd)

def main() -> None:
    """Main function."""
    # Create argument parser
    parser = argparse.ArgumentParser(description="Run the SearchableDataset application.")
    
    # Add subparsers
    subparsers = parser.add_subparsers(dest="command", help="Command to run.")
    
    # Add run command
    run_parser = subparsers.add_parser("run", help="Run the Streamlit application.")
    run_parser.add_argument("--port", type=int, default=8501, help="Port to run the application on.")
    run_parser.add_argument("--host", type=str, default="localhost", help="Host to run the application on.")
    
    # Add test command
    test_parser = subparsers.add_parser("test", help="Run the tests.")
    test_parser.add_argument("--verbose", "-v", action="store_true", help="Run tests in verbose mode.")
    test_parser.add_argument("--coverage", "-c", action="store_true", help="Run tests with coverage.")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Run the appropriate command
    if args.command == "run":
        # Run the Streamlit application
        streamlit_args = []
        if args.port:
            streamlit_args.extend(["--server.port", str(args.port)])
        if args.host:
            streamlit_args.extend(["--server.address", args.host])
        
        run_streamlit(streamlit_args)
    elif args.command == "test":
        # Run the tests
        test_args = []
        if args.verbose:
            test_args.append("-v")
        if args.coverage:
            test_args.extend(["--cov=.", "--cov-report=term-missing"])
        
        run_tests(test_args)
    else:
        # Show help
        parser.print_help()

if __name__ == "__main__":
    main()
