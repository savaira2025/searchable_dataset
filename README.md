# SearchableDataset

A Python application that searches datasets from the internet using LLM as an agent with a Streamlit frontend.

## Features

- Search for datasets across multiple sources (Kaggle, Google Dataset Search, Hugging Face, etc.)
- LLM-powered search agent to understand natural language queries
- Interactive visualization of dataset information
- Easy-to-use Streamlit interface

## Project Structure

```
SearchableDataset/
├── app/                      # Streamlit application
│   ├── main.py               # Main Streamlit application
│   ├── pages/                # Additional Streamlit pages
│   └── components/           # Reusable UI components
├── agents/                   # LLM agent implementation
│   ├── llm_agent.py          # LLM agent implementation
│   ├── prompts.py            # Prompt templates
│   └── processors.py         # Response processors
├── data_sources/             # Dataset source connectors
│   ├── base.py               # Base connector class
│   ├── kaggle.py             # Kaggle connector
│   ├── google_dataset.py     # Google Dataset Search connector
│   └── huggingface.py        # Hugging Face Datasets connector
├── utils/                    # Utility functions
│   ├── cache.py              # Caching utilities
│   ├── logger.py             # Logging configuration
│   └── config.py             # Configuration management
├── tests/                    # Test files
├── requirements.txt          # Project dependencies
├── README.md                 # Project documentation
└── .env                      # Environment variables (gitignored)
```

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/SearchableDataset.git
   cd SearchableDataset
   ```

2. Create a virtual environment and activate it:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   - Copy `.env.example` to `.env`
   - Fill in your API keys and other configuration

## Usage

1. Start the Streamlit app:
   ```
   streamlit run app/main.py
   ```

2. Open your browser and navigate to `http://localhost:8501`

## Development

### Adding a New Dataset Source

1. Create a new connector in the `data_sources` directory
2. Implement the required methods from the base connector class
3. Register the connector in the data sources registry

### Testing

Run tests with pytest:
```
pytest
```

## License

MIT

## Contributors

- Your Name <your.email@example.com>
