"""
Sample Test Script

This script demonstrates how to use the Reddit scraper pipeline with a sample run.
It validates the integration of all modules with a test scrape of r/MachineLearning.
"""

import os
import sys
import logging
from datetime import datetime, timedelta

# Add the src directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.scraper import RedditScraper
from src.storage import StorageFactory
from src.preprocessing import DataCleaner, TextPreprocessor, DataPreprocessor
from src.error_handling import setup_logging, ErrorHandler

def run_sample_test():
    """Run a sample test of the Reddit scraper pipeline."""
    # Set up logging
    logger = setup_logging(console_level='info')
    logger.info("Starting sample test of Reddit scraper pipeline")
    
    # Initialize error handler
    error_handler = ErrorHandler(logger)
    
    try:
        # Test parameters
        subreddit = 'MachineLearning'
        post_limit = 5
        storage_type = 'sqlite'
        data_dir = 'data/test'
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        # Initialize Reddit scraper
        logger.info(f"Initializing Reddit scraper for r/{subreddit}")
        scraper = RedditScraper(
            subreddit_name=subreddit,
            rate_limit_delay=1.0
        )
        
        # Get subreddit info
        logger.info("Retrieving subreddit information")
        subreddit_info = scraper.get_subreddit_info()
        logger.info(f"Subreddit: r/{subreddit_info['name']} - {subreddit_info['title']}")
        logger.info(f"Subscribers: {subreddit_info['subscribers']}")
        
        # Initialize storage backends (testing multiple backends)
        logger.info("Initializing storage backends")
        sqlite_storage = StorageFactory.create_storage(
            'sqlite',
            db_path=os.path.join(data_dir, f'{subreddit.lower()}.db')
        )
        
        json_storage = StorageFactory.create_storage(
            'json',
            data_dir=os.path.join(data_dir, 'json')
        )
        
        csv_storage = StorageFactory.create_storage(
            'csv',
            data_dir=os.path.join(data_dir, 'csv')
        )
        
        # Initialize data cleaner and preprocessor
        logger.info("Initializing data cleaner and preprocessor")
        text_preprocessor = TextPreprocessor(
            remove_urls=True,
            remove_special_chars=True,
            remove_stopwords=True,
            lowercase=True
        )
        cleaner = DataCleaner(text_preprocessor=text_preprocessor)
        preprocessor = DataPreprocessor(cleaner=cleaner)
        
        # Scrape posts and comments
        logger.info(f"Scraping {post_limit} hot posts from r/{subreddit}")
        result = scraper.get_posts_with_comments(
            limit=post_limit,
            time_filter='week',
            sort_by='hot',
            comment_limit=10
        )
        
        posts = result['posts']
        comments = result['comments']
        
        logger.info(f"Scraped {len(posts)} posts and {len(comments)} comments")
        
        # Display sample post data
        if posts:
            sample_post = posts[0]
            logger.info(f"Sample post: {sample_post['title']}")
            logger.info(f"  Author: {sample_post['author']}")
            logger.info(f"  Score: {sample_post['score']}")
            logger.info(f"  Comments: {sample_post['num_comments']}")
            logger.info(f"  Created: {sample_post['created_utc']}")
        
        # Display sample comment data
        if comments:
            sample_comment = comments[0]
            logger.info(f"Sample comment by {sample_comment['author']}")
            logger.info(f"  Score: {sample_comment['score']}")
            logger.info(f"  Body: {sample_comment['body'][:100]}...")
        
        # Test data cleaning
        logger.info("Testing data cleaning")
        cleaned_posts = cleaner.clean_posts(
            posts,
            min_score=1,
            clean_title=True,
            clean_selftext=True
        )
        
        cleaned_comments = cleaner.clean_comments(
            comments,
            min_score=1,
            clean_body=True
        )
        
        logger.info(f"After cleaning: {len(cleaned_posts)} posts and {len(cleaned_comments)} comments")
        
        # Test data preprocessing for ML
        logger.info("Testing data preprocessing for ML")
        ml_data = preprocessor.prepare_for_ml(cleaned_posts, cleaned_comments)
        logger.info(f"Prepared data for ML with {len(ml_data['post_texts'])} post texts and {len(ml_data['comment_texts'])} comment texts")
        
        # Extract features
        logger.info("Extracting features")
        features = preprocessor.extract_features(cleaned_posts, cleaned_comments)
        logger.info(f"Post features: {features['post_features']}")
        logger.info(f"Comment features: {features['comment_features']}")
        
        # Store data in all backends
        logger.info("Storing data in SQLite")
        sqlite_storage.save_posts(posts)
        sqlite_storage.save_comments(comments)
        
        logger.info("Storing data in JSON")
        json_storage.save_posts(posts)
        json_storage.save_comments(comments)
        
        logger.info("Storing data in CSV")
        csv_storage.save_posts(posts)
        csv_storage.save_comments(comments)
        
        # Retrieve and verify stored data
        logger.info("Verifying data storage")
        sqlite_posts = sqlite_storage.get_posts()
        sqlite_comments = sqlite_storage.get_comments()
        logger.info(f"SQLite: Retrieved {len(sqlite_posts)} posts and {len(sqlite_comments)} comments")
        
        json_posts = json_storage.get_posts()
        json_comments = json_storage.get_comments()
        logger.info(f"JSON: Retrieved {len(json_posts)} posts and {len(json_comments)} comments")
        
        csv_posts = csv_storage.get_posts()
        csv_comments = csv_storage.get_comments()
        logger.info(f"CSV: Retrieved {len(csv_posts)} posts and {len(csv_comments)} comments")
        
        # Test filtering
        logger.info("Testing data filtering")
        high_score_posts = sqlite_storage.get_posts({'score': 10})
        logger.info(f"Posts with score >= 10: {len(high_score_posts)}")
        
        # Close storage connections
        sqlite_storage.close()
        
        logger.info("Sample test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Sample test failed: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    success = run_sample_test()
    sys.exit(0 if success else 1)
