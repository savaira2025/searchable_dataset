"""
Setup script for the SearchableDataset package.
"""
from setuptools import setup, find_packages

# Read requirements
with open("requirements.txt") as f:
    requirements = f.read().splitlines()

# Read README
with open("README.md") as f:
    long_description = f.read()

setup(
    name="searchable-dataset",
    version="0.1.0",
    description="A Python application that searches datasets from the internet using LLM as an agent with a Streamlit frontend.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/yourusername/SearchableDataset",
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "searchable-dataset=run:main",
        ],
    },
)
