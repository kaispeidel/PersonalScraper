"""
Example Usage Script

This script demonstrates how to use the Reddit scraper pipeline for common tasks.
"""

import os
import sys
from datetime import datetime, timedelta

# Add the src directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.scraper import RedditScraper
from src.storage import StorageFactory
from src.preprocessing import DataCleaner, TextPreprocessor, DataPreprocessor
from src.error_handling import setup_logging

# Set up logging
logger = setup_logging(console_level='info')
logger.info("Starting Reddit scraper examples")

# Example 1: Basic scraping
def example_basic_scraping():
    """Basic scraping example."""
    logger.info("Example 1: Basic scraping")
    
    # Initialize scraper
    scraper = RedditScraper(subreddit_name='MachineLearning')
    
    # Get 5 hot posts
    posts = list(scraper.get_posts(limit=5, sort_by='hot'))
    
    # Print post titles
    for i, post in enumerate(posts, 1):
        print(f"{i}. {post['title']} (Score: {post['score']})")
    
    # Get comments for the first post
    if posts:
        comments = list(scraper.get_comments(post_id=posts[0]['id'], limit=5))
        print(f"\nComments for post: {posts[0]['title']}")
        
        for i, comment in enumerate(comments, 1):
            print(f"  {i}. {comment['author']}: {comment['body'][:100]}...")

# Example 2: Storing data
def example_storing_data():
    """Data storage example."""
    logger.info("Example 2: Storing data")
    
    # Initialize scraper
    scraper = RedditScraper(subreddit_name='MachineLearning')
    
    # Get posts and comments
    result = scraper.get_posts_with_comments(limit=3)
    posts = result['posts']
    comments = result['comments']
    
    print(f"Scraped {len(posts)} posts and {len(comments)} comments")
    
    # Store in SQLite
    sqlite_storage = StorageFactory.create_storage('sqlite', db_path='data/example.db')
    sqlite_storage.save_posts(posts)
    sqlite_storage.save_comments(comments)
    print("Data stored in SQLite database")
    
    # Store in JSON
    json_storage = StorageFactory.create_storage('json', data_dir='data/json_example')
    json_storage.save_posts(posts)
    json_storage.save_comments(comments)
    print("Data stored in JSON files")
    
    # Retrieve data
    stored_posts = sqlite_storage.get_posts()
    print(f"Retrieved {len(stored_posts)} posts from SQLite")
    
    # Close connections
    sqlite_storage.close()

# Example 3: Data cleaning and preprocessing
def example_data_cleaning():
    """Data cleaning example."""
    logger.info("Example 3: Data cleaning and preprocessing")
    
    # Initialize scraper
    scraper = RedditScraper(subreddit_name='MachineLearning')
    
    # Get posts and comments
    result = scraper.get_posts_with_comments(limit=3)
    posts = result['posts']
    comments = result['comments']
    
    # Initialize text preprocessor
    text_preprocessor = TextPreprocessor(
        remove_urls=True,
        remove_special_chars=True,
        remove_stopwords=True,
        lowercase=True
    )
    
    # Initialize data cleaner
    cleaner = DataCleaner(text_preprocessor=text_preprocessor)
    
    # Clean posts
    cleaned_posts = cleaner.clean_posts(
        posts,
        min_score=1,
        clean_title=True,
        clean_selftext=True
    )
    
    # Clean comments
    cleaned_comments = cleaner.clean_comments(
        comments,
        min_score=1,
        clean_body=True
    )
    
    print(f"Original posts: {len(posts)}, Cleaned posts: {len(cleaned_posts)}")
    print(f"Original comments: {len(comments)}, Cleaned comments: {len(cleaned_comments)}")
    
    # Show example of cleaned text
    if posts and 'selftext' in posts[0] and posts[0]['selftext']:
        original_text = posts[0]['selftext'][:200]
        cleaned_text = cleaned_posts[0]['selftext'][:200]
        
        print("\nOriginal text:")
        print(original_text)
        print("\nCleaned text:")
        print(cleaned_text)

# Example 4: Preparing for analysis
def example_data_analysis():
    """Data analysis preparation example."""
    logger.info("Example 4: Preparing for analysis")
    
    # Initialize scraper
    scraper = RedditScraper(subreddit_name='MachineLearning')
    
    # Get posts and comments
    result = scraper.get_posts_with_comments(limit=10)
    posts = result['posts']
    comments = result['comments']
    
    # Initialize preprocessor
    preprocessor = DataPreprocessor()
    
    # Prepare for analysis
    analysis_data = preprocessor.prepare_for_analysis(posts, comments)
    posts_df = analysis_data['posts']
    comments_df = analysis_data['comments']
    
    # Extract features
    features = preprocessor.extract_features(posts, comments)
    
    print("Post features:")
    for key, value in features['post_features'].items():
        print(f"  {key}: {value}")
    
    print("\nComment features:")
    for key, value in features['comment_features'].items():
        print(f"  {key}: {value}")
    
    # Show dataframe info
    print("\nPosts DataFrame info:")
    print(f"  Shape: {posts_df.shape}")
    print(f"  Columns: {', '.join(posts_df.columns)}")
    
    print("\nComments DataFrame info:")
    print(f"  Shape: {comments_df.shape}")
    print(f"  Columns: {', '.join(comments_df.columns)}")

# Example 5: Custom filtering
def example_custom_filtering():
    """Custom filtering example."""
    logger.info("Example 5: Custom filtering")
    
    # Initialize scraper
    scraper = RedditScraper(subreddit_name='MachineLearning')
    
    # Get posts
    posts = list(scraper.get_posts(limit=20, sort_by='hot'))
    
    # Initialize data cleaner
    cleaner = DataCleaner()
    
    # Filter posts by custom criteria
    def has_ml_keywords(post):
        """Check if post contains ML keywords."""
        keywords = ['neural', 'deep learning', 'tensorflow', 'pytorch', 'model']
        title = post['title'].lower()
        return any(keyword in title for keyword in keywords)
    
    filtered_posts = cleaner.filter_by_custom(posts, has_ml_keywords)
    
    print(f"Original posts: {len(posts)}")
    print(f"Posts with ML keywords: {len(filtered_posts)}")
    
    # Print filtered post titles
    for i, post in enumerate(filtered_posts, 1):
        print(f"{i}. {post['title']}")

# Run examples
if __name__ == "__main__":
    # Uncomment examples to run them
    # example_basic_scraping()
    # example_storing_data()
    # example_data_cleaning()
    # example_data_analysis()
    # example_custom_filtering()
    
    print("To run examples, uncomment the example functions in this script.")
    print("Note: You need to set up your Reddit API credentials in .env file first.")
