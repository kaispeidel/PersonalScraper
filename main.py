"""
Main Pipeline Script

This script integrates all modules to create a complete Reddit scraper pipeline.
It demonstrates how to use the scraper, storage, and preprocessing modules together.
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta

# Add the src directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.scraper import RedditScraper
from src.storage import StorageFactory
from src.preprocessing import DataCleaner, TextPreprocessor
from src.error_handling import setup_logging, retry, ErrorHandler

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Reddit Scraper Pipeline')
    
    # Scraper arguments
    parser.add_argument('--subreddit', type=str, default='MachineLearning',
                        help='Subreddit to scrape (default: MachineLearning)')
    parser.add_argument('--limit', type=int, default=10,
                        help='Maximum number of posts to scrape (default: 10)')
    parser.add_argument('--sort', type=str, default='hot',
                        choices=['hot', 'new', 'top', 'rising', 'controversial'],
                        help='Post sorting method (default: hot)')
    parser.add_argument('--time', type=str, default='week',
                        choices=['hour', 'day', 'week', 'month', 'year', 'all'],
                        help='Time filter for posts (default: week)')
    
    # Storage arguments
    parser.add_argument('--storage', type=str, default='sqlite',
                        choices=['sqlite', 'json', 'csv'],
                        help='Storage backend to use (default: sqlite)')
    parser.add_argument('--data-dir', type=str, default='data',
                        help='Directory for data storage (default: data)')
    
    # Preprocessing arguments
    parser.add_argument('--min-score', type=int, default=None,
                        help='Minimum score for posts and comments (default: None)')
    parser.add_argument('--clean-text', action='store_true',
                        help='Enable text cleaning (default: False)')
    
    # Logging arguments
    parser.add_argument('--log-level', type=str, default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='Logging level (default: info)')
    
    return parser.parse_args()

def main():
    """Main function to run the Reddit scraper pipeline."""
    # Parse command line arguments
    args = parse_args()
    
    # Set up logging
    logger = setup_logging(console_level=args.log_level)
    logger.info(f"Starting Reddit scraper pipeline for r/{args.subreddit}")
    
    # Initialize error handler
    error_handler = ErrorHandler(logger)
    
    try:
        # Initialize Reddit scraper
        logger.info("Initializing Reddit scraper")
        scraper = RedditScraper(
            subreddit_name=args.subreddit,
            rate_limit_delay=1.0
        )
        
        # Initialize storage backend
        logger.info(f"Initializing {args.storage} storage backend")
        if args.storage == 'sqlite':
            storage = StorageFactory.create_storage(
                'sqlite',
                db_path=os.path.join(args.data_dir, f'{args.subreddit.lower()}.db')
            )
        else:
            storage = StorageFactory.create_storage(
                args.storage,
                data_dir=os.path.join(args.data_dir, args.subreddit.lower())
            )
        
        # Initialize data cleaner
        logger.info("Initializing data cleaner")
        text_preprocessor = TextPreprocessor(
            remove_urls=args.clean_text,
            remove_special_chars=args.clean_text,
            remove_stopwords=args.clean_text,
            lowercase=args.clean_text
        )
        cleaner = DataCleaner(text_preprocessor=text_preprocessor)
        
        # Scrape posts and comments
        logger.info(f"Scraping {args.limit} {args.sort} posts from r/{args.subreddit}")
        result = error_handler.with_error_handling(
            scraper.get_posts_with_comments,
            limit=args.limit,
            time_filter=args.time,
            sort_by=args.sort,
            comment_limit=None,
            context={'subreddit': args.subreddit, 'limit': args.limit}
        )
        
        if not result:
            logger.error("Failed to scrape data")
            return 1
        
        posts = result['posts']
        comments = result['comments']
        
        logger.info(f"Scraped {len(posts)} posts and {len(comments)} comments")
        
        # Clean data if requested
        if args.min_score is not None or args.clean_text:
            logger.info("Cleaning data")
            posts = cleaner.clean_posts(
                posts,
                min_score=args.min_score,
                clean_title=args.clean_text,
                clean_selftext=args.clean_text
            )
            
            comments = cleaner.clean_comments(
                comments,
                min_score=args.min_score,
                clean_body=args.clean_text
            )
            
            logger.info(f"After cleaning: {len(posts)} posts and {len(comments)} comments")
        
        # Store data
        logger.info(f"Storing data in {args.storage} backend")
        storage.save_posts(posts)
        storage.save_comments(comments)
        
        # Retrieve and verify stored data
        stored_posts = storage.get_posts()
        stored_comments = storage.get_comments()
        
        logger.info(f"Verified storage: {len(stored_posts)} posts and {len(stored_comments)} comments retrieved")
        
        # Close storage connection
        storage.close()
        
        logger.info("Reddit scraper pipeline completed successfully")
        return 0
        
    except Exception as e:
        logger.critical(f"Pipeline failed: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
