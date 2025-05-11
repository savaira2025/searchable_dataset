#!/usr/bin/env python
"""
Script to clear the cache.
"""
import os
import sys

# Add the current directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from utils.cache import cache

def main():
    """Clear the cache."""
    print("Clearing cache...")
    cache.clear()
    print("Cache cleared.")

if __name__ == "__main__":
    main()
