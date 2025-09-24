"""
Reddit Scraper Module

This module provides functionality to scrape posts and comments from Reddit
using the PRAW library. It handles authentication, pagination, and rate limiting.
"""

import os
import time
import logging
from typing import Dict, List, Optional, Generator, Union, Any
from datetime import datetime, timedelta

import praw
from praw.models import Submission, Comment, Subreddit
from praw.exceptions import PRAWException, APIException
from prawcore.exceptions import ResponseException, RequestException
from dotenv import load_dotenv
from tqdm import tqdm

# Configure logger
logger = logging.getLogger(__name__)

class RedditScraper:
    """
    A class to scrape Reddit posts and comments using PRAW.
    
    Attributes:
        reddit: Authenticated PRAW Reddit instance
        subreddit: Target subreddit to scrape
        rate_limit_delay: Delay between API requests in seconds
    """
    
    def __init__(self, 
                 subreddit_name: str,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 user_agent: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 rate_limit_delay: float = 1.0,
                 env_file: str = '.env'):
        """
        Initialize the Reddit scraper with authentication credentials.
        
        Args:
            subreddit_name: Name of the subreddit to scrape
            client_id: Reddit API client ID
            client_secret: Reddit API client secret
            user_agent: User agent string for Reddit API
            username: Reddit username
            password: Reddit password
            rate_limit_delay: Delay between API requests in seconds
            env_file: Path to .env file containing credentials
        """
        # Load credentials from environment variables if not provided
        if None in (client_id, client_secret, user_agent):
            load_dotenv(env_file)
            client_id = client_id or os.getenv('REDDIT_CLIENT_ID')
            client_secret = client_secret or os.getenv('REDDIT_CLIENT_SECRET')
            user_agent = user_agent or os.getenv('REDDIT_USER_AGENT')
            username = username or os.getenv('REDDIT_USERNAME')
            password = password or os.getenv('REDDIT_PASSWORD')
        
        # Validate required credentials
        if not all([client_id, client_secret, user_agent]):
            raise ValueError("Missing Reddit API credentials. Provide them as parameters or in .env file.")
        
        # Initialize PRAW Reddit instance
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            username=username,
            password=password
        )
        
        # Set subreddit and rate limit delay
        self.subreddit = self.reddit.subreddit(subreddit_name)
        self.rate_limit_delay = rate_limit_delay
        
        logger.info(f"Initialized RedditScraper for r/{subreddit_name}")
    
    def _handle_rate_limit(self) -> None:
        """Apply rate limiting delay between API requests."""
        time.sleep(self.rate_limit_delay)
    
    def get_posts(self, 
                  limit: int = 100, 
                  time_filter: str = 'week',
                  sort_by: str = 'hot',
                  after: Optional[str] = None,
                  before: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """
        Retrieve posts from the subreddit with pagination support.
        
        Args:
            limit: Maximum number of posts to retrieve
            time_filter: Time filter for posts ('hour', 'day', 'week', 'month', 'year', 'all')
            sort_by: Sorting method ('hot', 'new', 'top', 'rising', 'controversial')
            after: Fullname of a post to retrieve posts after
            before: Fullname of a post to retrieve posts before
            
        Yields:
            Dictionary containing post data
        """
        logger.info(f"Retrieving {limit} {sort_by} posts from r/{self.subreddit.display_name} for time filter: {time_filter}")
        
        # Select the appropriate listing based on sort_by
        if sort_by == 'hot':
            posts = self.subreddit.hot(limit=limit)
        elif sort_by == 'new':
            posts = self.subreddit.new(limit=limit)
        elif sort_by == 'top':
            posts = self.subreddit.top(time_filter=time_filter, limit=limit)
        elif sort_by == 'rising':
            posts = self.subreddit.rising(limit=limit)
        elif sort_by == 'controversial':
            posts = self.subreddit.controversial(time_filter=time_filter, limit=limit)
        else:
            raise ValueError(f"Invalid sort_by value: {sort_by}")
        
        # Process each post
        try:
            for post in tqdm(posts, total=limit, desc="Scraping posts"):
                self._handle_rate_limit()
                
                # Convert post to dictionary
                post_data = {
                    'id': post.id,
                    'title': post.title,
                    'author': str(post.author) if post.author else '[deleted]',
                    'created_utc': datetime.fromtimestamp(post.created_utc),
                    'score': post.score,
                    'upvote_ratio': post.upvote_ratio,
                    'num_comments': post.num_comments,
                    'url': post.url,
                    'selftext': post.selftext,
                    'is_self': post.is_self,
                    'permalink': post.permalink,
                    'flair': post.link_flair_text,
                    'domain': post.domain,
                    'is_video': post.is_video,
                    'is_original_content': post.is_original_content,
                    'subreddit': post.subreddit.display_name
                }
                
                yield post_data
                
        except (PRAWException, APIException, ResponseException, RequestException) as e:
            logger.error(f"Error retrieving posts: {str(e)}")
            raise
    
    def get_comments(self, 
                     post_id: str, 
                     limit: Optional[int] = None,
                     sort_by: str = 'best') -> Generator[Dict[str, Any], None, None]:
        """
        Retrieve comments for a specific post.
        
        Args:
            post_id: ID of the post to retrieve comments for
            limit: Maximum number of comments to retrieve (None for all)
            sort_by: Comment sorting method ('best', 'top', 'new', 'controversial', 'old', 'qa')
            
        Yields:
            Dictionary containing comment data
        """
        logger.info(f"Retrieving comments for post {post_id} sorted by {sort_by}")
        
        try:
            # Get the submission
            submission = self.reddit.submission(id=post_id)
            
            # Replace MoreComments objects with actual comments
            submission.comments.replace_more(limit=limit)
            
            # Sort comments if specified
            if sort_by == 'best':
                comments = submission.comments
            elif sort_by == 'top':
                comments = submission.comments.top()
            elif sort_by == 'new':
                comments = submission.comments.new()
            elif sort_by == 'controversial':
                comments = submission.comments.controversial()
            elif sort_by == 'old':
                comments = submission.comments.old()
            elif sort_by == 'qa':
                comments = submission.comments.qa()
            else:
                raise ValueError(f"Invalid sort_by value: {sort_by}")
            
            # Process all comments in the comment forest
            comment_queue = list(comments)
            while comment_queue:
                comment = comment_queue.pop(0)
                self._handle_rate_limit()
                
                # Convert comment to dictionary
                comment_data = {
                    'id': comment.id,
                    'post_id': post_id,
                    'parent_id': comment.parent_id.split('_')[1],
                    'author': str(comment.author) if comment.author else '[deleted]',
                    'created_utc': datetime.fromtimestamp(comment.created_utc),
                    'score': comment.score,
                    'body': comment.body,
                    'permalink': comment.permalink,
                    'depth': comment.depth,
                    'is_submitter': comment.is_submitter,
                    'subreddit': comment.subreddit.display_name
                }
                
                yield comment_data
                
                # Add replies to the queue
                comment_queue.extend(comment.replies)
                
        except (PRAWException, APIException, ResponseException, RequestException) as e:
            logger.error(f"Error retrieving comments for post {post_id}: {str(e)}")
            raise
    
    def get_posts_with_comments(self, 
                               limit: int = 10,
                               time_filter: str = 'week',
                               sort_by: str = 'hot',
                               comment_limit: Optional[int] = None,
                               comment_sort: str = 'best') -> Dict[str, List[Dict[str, Any]]]:
        """
        Retrieve posts and their comments in a single operation.
        
        Args:
            limit: Maximum number of posts to retrieve
            time_filter: Time filter for posts
            sort_by: Post sorting method
            comment_limit: Maximum number of comments to retrieve per post
            comment_sort: Comment sorting method
            
        Returns:
            Dictionary with 'posts' and 'comments' lists
        """
        logger.info(f"Retrieving {limit} posts with comments from r/{self.subreddit.display_name}")
        
        result = {
            'posts': [],
            'comments': []
        }
        
        # Get posts
        for post_data in self.get_posts(limit=limit, time_filter=time_filter, sort_by=sort_by):
            result['posts'].append(post_data)
            
            # Get comments for each post
            post_comments = list(self.get_comments(
                post_id=post_data['id'],
                limit=comment_limit,
                sort_by=comment_sort
            ))
            
            result['comments'].extend(post_comments)
            logger.info(f"Retrieved {len(post_comments)} comments for post {post_data['id']}")
        
        return result
    
    def search_posts(self,
                    query: str,
                    limit: int = 100,
                    sort_by: str = 'relevance',
                    time_filter: str = 'all') -> Generator[Dict[str, Any], None, None]:
        """
        Search for posts in the subreddit.
        
        Args:
            query: Search query string
            limit: Maximum number of posts to retrieve
            sort_by: Sorting method ('relevance', 'hot', 'new', 'top', 'comments')
            time_filter: Time filter for search results
            
        Yields:
            Dictionary containing post data
        """
        logger.info(f"Searching for '{query}' in r/{self.subreddit.display_name}")
        
        try:
            # Perform search
            search_results = self.subreddit.search(
                query=query,
                sort=sort_by,
                time_filter=time_filter,
                limit=limit
            )
            
            # Process each result
            for post in tqdm(search_results, total=limit, desc="Searching posts"):
                self._handle_rate_limit()
                
                # Convert post to dictionary (same as in get_posts)
                post_data = {
                    'id': post.id,
                    'title': post.title,
                    'author': str(post.author) if post.author else '[deleted]',
                    'created_utc': datetime.fromtimestamp(post.created_utc),
                    'score': post.score,
                    'upvote_ratio': post.upvote_ratio,
                    'num_comments': post.num_comments,
                    'url': post.url,
                    'selftext': post.selftext,
                    'is_self': post.is_self,
                    'permalink': post.permalink,
                    'flair': post.link_flair_text,
                    'domain': post.domain,
                    'is_video': post.is_video,
                    'is_original_content': post.is_original_content,
                    'subreddit': post.subreddit.display_name
                }
                
                yield post_data
                
        except (PRAWException, APIException, ResponseException, RequestException) as e:
            logger.error(f"Error searching posts: {str(e)}")
            raise
    
    def get_subreddit_info(self) -> Dict[str, Any]:
        """
        Retrieve information about the subreddit.
        
        Returns:
            Dictionary containing subreddit information
        """
        logger.info(f"Retrieving information for r/{self.subreddit.display_name}")
        
        try:
            self._handle_rate_limit()
            
            # Convert subreddit to dictionary
            subreddit_data = {
                'id': self.subreddit.id,
                'name': self.subreddit.display_name,
                'title': self.subreddit.title,
                'description': self.subreddit.description,
                'public_description': self.subreddit.public_description,
                'subscribers': self.subreddit.subscribers,
                'created_utc': datetime.fromtimestamp(self.subreddit.created_utc),
                'over18': self.subreddit.over18,
                'url': self.subreddit.url
            }
            
            return subreddit_data
            
        except (PRAWException, APIException, ResponseException, RequestException) as e:
            logger.error(f"Error retrieving subreddit info: {str(e)}")
            raise
