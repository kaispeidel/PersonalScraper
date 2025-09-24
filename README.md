# Reddit Scraper Pipeline Documentation

## Overview

This document provides comprehensive documentation for the Reddit Scraper Pipeline, a modular system designed to collect, store, clean, and prepare data from specific subreddits for analysis or machine learning tasks.

## Features

- **Robust API Integration**: Uses PRAW (Python Reddit API Wrapper) to interact with Reddit's official API
- **Modular Architecture**: Separate modules for scraping, storage, cleaning, and error handling
- **Multiple Storage Options**: Support for SQLite, JSON, and CSV backends
- **Advanced Text Processing**: Comprehensive text cleaning and preprocessing capabilities
- **Flexible Filtering**: Filter by date, score, or custom criteria
- **Error Handling**: Robust error handling with retry mechanisms and detailed logging
- **Analysis Ready**: Data prepared for further analysis or ML preprocessing

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd reddit-scraper
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Set up Reddit API credentials:
   - Create a Reddit developer application at https://www.reddit.com/prefs/apps
   - Copy `.env.example` to `.env`
   - Fill in your Reddit API credentials in the `.env` file

## Project Structure

```
reddit_scraper/
├── src/
│   ├── scraper.py         # Reddit API interaction
│   ├── storage.py         # Data storage backends
│   ├── preprocessing.py   # Data cleaning and preprocessing
│   ├── error_handling.py  # Logging and error handling
├── data/                  # Data storage directory
├── logs/                  # Log files
├── tests/                 # Test scripts
├── main.py                # Main pipeline script
├── test_sample.py         # Sample test script
├── requirements.txt       # Dependencies
├── .env.example           # Example environment file
└── architecture.md        # Architecture documentation
```

## Usage

### Basic Usage

Run the main pipeline script with default settings:

```bash
python main.py
```

This will scrape 10 hot posts from r/MachineLearning and their comments, store them in a SQLite database, and apply basic cleaning.

### Command Line Options

The pipeline supports various command line options:

```bash
python main.py --help
```

Key options include:

- `--subreddit`: Target subreddit (default: MachineLearning)
- `--limit`: Maximum number of posts to scrape (default: 10)
- `--sort`: Post sorting method (hot, new, top, rising, controversial)
- `--time`: Time filter for posts (hour, day, week, month, year, all)
- `--storage`: Storage backend (sqlite, json, csv)
- `--data-dir`: Directory for data storage
- `--min-score`: Minimum score for posts and comments
- `--clean-text`: Enable text cleaning
- `--log-level`: Logging level (debug, info, warning, error, critical)

### Examples

Scrape 20 top posts from r/Python from the past month:

```bash
python main.py --subreddit Python --limit 20 --sort top --time month
```

Scrape posts with text cleaning and minimum score:

```bash
python main.py --clean-text --min-score 10
```

Use JSON storage instead of SQLite:

```bash
python main.py --storage json --data-dir data/json_data
```

## Module Documentation

### Scraper Module

The scraper module (`src/scraper.py`) handles interaction with Reddit's API using PRAW. It provides functions to:

- Authenticate with Reddit API
- Retrieve posts with pagination
- Retrieve comments with threading
- Handle rate limits
- Search posts
- Get subreddit information

Example usage:

```python
from src.scraper import RedditScraper

# Initialize scraper
scraper = RedditScraper(subreddit_name='MachineLearning')

# Get posts and comments
data = scraper.get_posts_with_comments(limit=10, sort_by='hot')
posts = data['posts']
comments = data['comments']
```

### Storage Module

The storage module (`src/storage.py`) provides multiple backends for storing scraped data:

- SQLite: Relational database storage
- JSON: File-based JSON storage
- CSV: File-based CSV storage

Example usage:

```python
from src.storage import StorageFactory

# Create storage backend
storage = StorageFactory.create_storage('sqlite', db_path='data/reddit.db')

# Store data
storage.save_posts(posts)
storage.save_comments(comments)

# Retrieve data
stored_posts = storage.get_posts()
post_comments = storage.get_comments({'post_id': 'abc123'})
```

### Preprocessing Module

The preprocessing module (`src/preprocessing.py`) handles data cleaning and preparation:

- Text normalization (URLs, special characters, stopwords)
- Duplicate removal
- Filtering by date, score, or custom criteria
- Feature extraction
- Preparation for ML tasks

Example usage:

```python
from src.preprocessing import DataCleaner, TextPreprocessor

# Initialize preprocessor
text_preprocessor = TextPreprocessor(remove_urls=True, remove_stopwords=True)
cleaner = DataCleaner(text_preprocessor=text_preprocessor)

# Clean data
cleaned_posts = cleaner.clean_posts(posts, min_score=5, clean_title=True)
cleaned_comments = cleaner.clean_comments(comments, min_score=2)
```

### Error Handling Module

The error handling module (`src/error_handling.py`) provides utilities for logging and error management:

- Centralized logging configuration
- Retry decorator for transient failures
- Safe execution wrappers
- Structured error handling

Example usage:

```python
from src.error_handling import setup_logging, retry, ErrorHandler

# Set up logging
logger = setup_logging(console_level='info')

# Use retry decorator
@retry(max_attempts=3, exceptions=(ConnectionError,))
def fetch_data():
    # ...

# Use error handler
error_handler = ErrorHandler(logger)
result = error_handler.with_error_handling(
    function, arg1, arg2, context={'operation': 'data_fetch'}
)
```

## Extending the Pipeline

### Adding New Storage Backends

To add a new storage backend:

1. Create a new class that inherits from `StorageBackend` in `src/storage.py`
2. Implement the required methods: `save_posts`, `save_comments`, `get_posts`, `get_comments`
3. Add the new backend to the `StorageFactory.create_storage` method

### Customizing Text Preprocessing

To customize text preprocessing:

1. Create a new instance of `TextPreprocessor` with your desired options
2. Pass it to the `DataCleaner` constructor
3. Use the cleaner's methods with your specific parameters

### Adding New Analysis Features

To add new analysis capabilities:

1. Extend the `DataPreprocessor` class in `src/preprocessing.py`
2. Add new methods for your specific analysis needs
3. Use the preprocessor to transform the data for your analysis

## Troubleshooting

### API Rate Limiting

If you encounter rate limiting issues:

- Increase the `rate_limit_delay` parameter when initializing the `RedditScraper`
- Reduce the number of posts requested
- Use authenticated access with your Reddit account

### Authentication Errors

If you see authentication errors:

- Verify your Reddit API credentials in the `.env` file
- Ensure your Reddit API application is correctly set up
- Check that your user agent string follows Reddit's guidelines

### Storage Errors

If you encounter storage issues:

- Check file permissions for the data directory
- Verify database connection parameters
- Ensure you have sufficient disk space

## Future Enhancements

Potential enhancements for future versions:

- Support for additional Reddit data (user profiles, awards, etc.)
- Integration with more data sources (Pushshift API, etc.)
- Advanced NLP features (sentiment analysis, topic modeling)
- Real-time monitoring and alerting
- Web interface for configuration and visualization
- Distributed processing for large-scale scraping
