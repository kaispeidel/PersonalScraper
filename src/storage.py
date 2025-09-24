"""
Storage Module

This module provides functionality to store scraped Reddit data in various formats
including SQLite, JSON, and CSV. It defines schemas and interfaces for data persistence.
"""

import os
import json
import csv
import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Configure logger
logger = logging.getLogger(__name__)

# Create SQLAlchemy base
Base = declarative_base()

# Define SQLAlchemy models
class Post(Base):
    """SQLAlchemy model for Reddit posts."""
    __tablename__ = 'posts'
    
    id = Column(String(10), primary_key=True)
    title = Column(String(300), nullable=False)
    author = Column(String(100), nullable=False)
    created_utc = Column(DateTime, nullable=False)
    score = Column(Integer, nullable=False)
    upvote_ratio = Column(Float, nullable=True)
    num_comments = Column(Integer, nullable=False)
    url = Column(String(500), nullable=False)
    selftext = Column(Text, nullable=True)
    is_self = Column(Boolean, nullable=False)
    permalink = Column(String(500), nullable=False)
    flair = Column(String(100), nullable=True)
    domain = Column(String(100), nullable=True)
    is_video = Column(Boolean, nullable=True)
    is_original_content = Column(Boolean, nullable=True)
    subreddit = Column(String(100), nullable=False)
    
    # Relationship with comments
    comments = relationship("Comment", back_populates="post")
    
    def __repr__(self):
        return f"<Post(id='{self.id}', title='{self.title[:20]}...', author='{self.author}')>"


class Comment(Base):
    """SQLAlchemy model for Reddit comments."""
    __tablename__ = 'comments'
    
    id = Column(String(10), primary_key=True)
    post_id = Column(String(10), ForeignKey('posts.id'), nullable=False)
    parent_id = Column(String(10), nullable=False)
    author = Column(String(100), nullable=False)
    created_utc = Column(DateTime, nullable=False)
    score = Column(Integer, nullable=False)
    body = Column(Text, nullable=False)
    permalink = Column(String(500), nullable=True)
    depth = Column(Integer, nullable=True)
    is_submitter = Column(Boolean, nullable=True)
    subreddit = Column(String(100), nullable=False)
    
    # Relationship with post
    post = relationship("Post", back_populates="comments")
    
    def __repr__(self):
        return f"<Comment(id='{self.id}', author='{self.author}', post_id='{self.post_id}')>"


class StorageBackend:
    """Base class for storage backends."""
    
    def save_posts(self, posts: List[Dict[str, Any]]) -> None:
        """
        Save posts to storage.
        
        Args:
            posts: List of post dictionaries
        """
        raise NotImplementedError("Subclasses must implement save_posts")
    
    def save_comments(self, comments: List[Dict[str, Any]]) -> None:
        """
        Save comments to storage.
        
        Args:
            comments: List of comment dictionaries
        """
        raise NotImplementedError("Subclasses must implement save_comments")
    
    def get_posts(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Retrieve posts from storage with optional filtering.
        
        Args:
            filters: Dictionary of filter criteria
            
        Returns:
            List of post dictionaries
        """
        raise NotImplementedError("Subclasses must implement get_posts")
    
    def get_comments(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Retrieve comments from storage with optional filtering.
        
        Args:
            filters: Dictionary of filter criteria
            
        Returns:
            List of comment dictionaries
        """
        raise NotImplementedError("Subclasses must implement get_comments")
    
    def close(self) -> None:
        """Close any open connections or resources."""
        pass


class SQLiteStorage(StorageBackend):
    """Storage backend using SQLite database."""
    
    def __init__(self, db_path: str, echo: bool = False):
        """
        Initialize SQLite storage.
        
        Args:
            db_path: Path to SQLite database file
            echo: Whether to echo SQL statements
        """
        self.db_path = db_path
        self.engine = create_engine(f'sqlite:///{db_path}', echo=echo)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        logger.info(f"Initialized SQLite storage at {db_path}")
    
    def _convert_datetime(self, obj: Dict[str, Any]) -> Dict[str, Any]:
        """Convert datetime objects to strings for JSON serialization."""
        result = obj.copy()
        for key, value in result.items():
            if isinstance(value, datetime):
                result[key] = value
        return result
    
    def save_posts(self, posts: List[Dict[str, Any]]) -> None:
        """
        Save posts to SQLite database.
        
        Args:
            posts: List of post dictionaries
        """
        try:
            for post_data in posts:
                # Check if post already exists
                existing_post = self.session.query(Post).filter_by(id=post_data['id']).first()
                if existing_post:
                    # Update existing post
                    for key, value in post_data.items():
                        setattr(existing_post, key, value)
                else:
                    # Create new post
                    post = Post(**self._convert_datetime(post_data))
                    self.session.add(post)
            
            self.session.commit()
            logger.info(f"Saved {len(posts)} posts to SQLite database")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error saving posts to SQLite: {str(e)}")
            raise
    
    def save_comments(self, comments: List[Dict[str, Any]]) -> None:
        """
        Save comments to SQLite database.
        
        Args:
            comments: List of comment dictionaries
        """
        try:
            for comment_data in comments:
                # Check if comment already exists
                existing_comment = self.session.query(Comment).filter_by(id=comment_data['id']).first()
                if existing_comment:
                    # Update existing comment
                    for key, value in comment_data.items():
                        setattr(existing_comment, key, value)
                else:
                    # Create new comment
                    comment = Comment(**self._convert_datetime(comment_data))
                    self.session.add(comment)
            
            self.session.commit()
            logger.info(f"Saved {len(comments)} comments to SQLite database")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error saving comments to SQLite: {str(e)}")
            raise
    
    def get_posts(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Retrieve posts from SQLite database with optional filtering.
        
        Args:
            filters: Dictionary of filter criteria
            
        Returns:
            List of post dictionaries
        """
        try:
            query = self.session.query(Post)
            
            # Apply filters if provided
            if filters:
                for key, value in filters.items():
                    if hasattr(Post, key):
                        query = query.filter(getattr(Post, key) == value)
            
            # Convert SQLAlchemy objects to dictionaries
            posts = []
            for post in query.all():
                post_dict = {column.name: getattr(post, column.name) for column in post.__table__.columns}
                posts.append(post_dict)
            
            return posts
        except Exception as e:
            logger.error(f"Error retrieving posts from SQLite: {str(e)}")
            raise
    
    def get_comments(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Retrieve comments from SQLite database with optional filtering.
        
        Args:
            filters: Dictionary of filter criteria
            
        Returns:
            List of comment dictionaries
        """
        try:
            query = self.session.query(Comment)
            
            # Apply filters if provided
            if filters:
                for key, value in filters.items():
                    if hasattr(Comment, key):
                        query = query.filter(getattr(Comment, key) == value)
            
            # Convert SQLAlchemy objects to dictionaries
            comments = []
            for comment in query.all():
                comment_dict = {column.name: getattr(comment, column.name) for column in comment.__table__.columns}
                comments.append(comment_dict)
            
            return comments
        except Exception as e:
            logger.error(f"Error retrieving comments from SQLite: {str(e)}")
            raise
    
    def close(self) -> None:
        """Close the database session."""
        self.session.close()
        logger.info("Closed SQLite database session")


class JSONStorage(StorageBackend):
    """Storage backend using JSON files."""
    
    def __init__(self, data_dir: str):
        """
        Initialize JSON storage.
        
        Args:
            data_dir: Directory to store JSON files
        """
        self.data_dir = data_dir
        self.posts_file = os.path.join(data_dir, 'posts.json')
        self.comments_file = os.path.join(data_dir, 'comments.json')
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        # Initialize empty files if they don't exist
        if not os.path.exists(self.posts_file):
            with open(self.posts_file, 'w') as f:
                json.dump([], f)
        
        if not os.path.exists(self.comments_file):
            with open(self.comments_file, 'w') as f:
                json.dump([], f)
        
        logger.info(f"Initialized JSON storage in {data_dir}")
    
    def _datetime_serializer(self, obj: Any) -> str:
        """JSON serializer for datetime objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
    
    def save_posts(self, posts: List[Dict[str, Any]]) -> None:
        """
        Save posts to JSON file.
        
        Args:
            posts: List of post dictionaries
        """
        try:
            # Read existing posts
            with open(self.posts_file, 'r') as f:
                existing_posts = json.load(f)
            
            # Create a dictionary of existing posts for quick lookup
            existing_posts_dict = {post['id']: post for post in existing_posts}
            
            # Update existing posts or add new ones
            for post in posts:
                existing_posts_dict[post['id']] = post
            
            # Write back to file
            with open(self.posts_file, 'w') as f:
                json.dump(list(existing_posts_dict.values()), f, default=self._datetime_serializer, indent=2)
            
            logger.info(f"Saved {len(posts)} posts to JSON file")
        except Exception as e:
            logger.error(f"Error saving posts to JSON: {str(e)}")
            raise
    
    def save_comments(self, comments: List[Dict[str, Any]]) -> None:
        """
        Save comments to JSON file.
        
        Args:
            comments: List of comment dictionaries
        """
        try:
            # Read existing comments
            with open(self.comments_file, 'r') as f:
                existing_comments = json.load(f)
            
            # Create a dictionary of existing comments for quick lookup
            existing_comments_dict = {comment['id']: comment for comment in existing_comments}
            
            # Update existing comments or add new ones
            for comment in comments:
                existing_comments_dict[comment['id']] = comment
            
            # Write back to file
            with open(self.comments_file, 'w') as f:
                json.dump(list(existing_comments_dict.values()), f, default=self._datetime_serializer, indent=2)
            
            logger.info(f"Saved {len(comments)} comments to JSON file")
        except Exception as e:
            logger.error(f"Error saving comments to JSON: {str(e)}")
            raise
    
    def get_posts(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Retrieve posts from JSON file with optional filtering.
        
        Args:
            filters: Dictionary of filter criteria
            
        Returns:
            List of post dictionaries
        """
        try:
            # Read posts from file
            with open(self.posts_file, 'r') as f:
                posts = json.load(f)
            
            # Apply filters if provided
            if filters:
                filtered_posts = []
                for post in posts:
                    if all(post.get(key) == value for key, value in filters.items()):
                        filtered_posts.append(post)
                return filtered_posts
            
            return posts
        except Exception as e:
            logger.error(f"Error retrieving posts from JSON: {str(e)}")
            raise
    
    def get_comments(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Retrieve comments from JSON file with optional filtering.
        
        Args:
            filters: Dictionary of filter criteria
            
        Returns:
            List of comment dictionaries
        """
        try:
            # Read comments from file
            with open(self.comments_file, 'r') as f:
                comments = json.load(f)
            
            # Apply filters if provided
            if filters:
                filtered_comments = []
                for comment in comments:
                    if all(comment.get(key) == value for key, value in filters.items()):
                        filtered_comments.append(comment)
                return filtered_comments
            
            return comments
        except Exception as e:
            logger.error(f"Error retrieving comments from JSON: {str(e)}")
            raise


class CSVStorage(StorageBackend):
    """Storage backend using CSV files."""
    
    def __init__(self, data_dir: str):
        """
        Initialize CSV storage.
        
        Args:
            data_dir: Directory to store CSV files
        """
        self.data_dir = data_dir
        self.posts_file = os.path.join(data_dir, 'posts.csv')
        self.comments_file = os.path.join(data_dir, 'comments.csv')
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        logger.info(f"Initialized CSV storage in {data_dir}")
    
    def save_posts(self, posts: List[Dict[str, Any]]) -> None:
        """
        Save posts to CSV file.
        
        Args:
            posts: List of post dictionaries
        """
        try:
            # Convert to DataFrame
            df = pd.DataFrame(posts)
            
            # Check if file exists
            if os.path.exists(self.posts_file):
                # Read existing posts
                existing_df = pd.read_csv(self.posts_file)
                
                # Combine and remove duplicates
                combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=['id'], keep='last')
                combined_df.to_csv(self.posts_file, index=False)
            else:
                # Create new file
                df.to_csv(self.posts_file, index=False)
            
            logger.info(f"Saved {len(posts)} posts to CSV file")
        except Exception as e:
            logger.error(f"Error saving posts to CSV: {str(e)}")
            raise
    
    def save_comments(self, comments: List[Dict[str, Any]]) -> None:
        """
        Save comments to CSV file.
        
        Args:
            comments: List of comment dictionaries
        """
        try:
            # Convert to DataFrame
            df = pd.DataFrame(comments)
            
            # Check if file exists
            if os.path.exists(self.comments_file):
                # Read existing comments
                existing_df = pd.read_csv(self.comments_file)
                
                # Combine and remove duplicates
                combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=['id'], keep='last')
                combined_df.to_csv(self.comments_file, index=False)
            else:
                # Create new file
                df.to_csv(self.comments_file, index=False)
            
            logger.info(f"Saved {len(comments)} comments to CSV file")
        except Exception as e:
            logger.error(f"Error saving comments to CSV: {str(e)}")
            raise
    
    def get_posts(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Retrieve posts from CSV file with optional filtering.
        
        Args:
            filters: Dictionary of filter criteria
            
        Returns:
            List of post dictionaries
        """
        try:
            # Check if file exists
            if not os.path.exists(self.posts_file):
                return []
            
            # Read posts from file
            df = pd.read_csv(self.posts_file)
            
            # Apply filters if provided
            if filters:
                for key, value in filters.items():
                    if key in df.columns:
                        df = df[df[key] == value]
            
            # Convert to list of dictionaries
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error retrieving posts from CSV: {str(e)}")
            raise
    
    def get_comments(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Retrieve comments from CSV file with optional filtering.
        
        Args:
            filters: Dictionary of filter criteria
            
        Returns:
            List of comment dictionaries
        """
        try:
            # Check if file exists
            if not os.path.exists(self.comments_file):
                return []
            
            # Read comments from file
            df = pd.read_csv(self.comments_file)
            
            # Apply filters if provided
            if filters:
                for key, value in filters.items():
                    if key in df.columns:
                        df = df[df[key] == value]
            
            # Convert to list of dictionaries
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error retrieving comments from CSV: {str(e)}")
            raise


class StorageFactory:
    """Factory class for creating storage backends."""
    
    @staticmethod
    def create_storage(storage_type: str, **kwargs) -> StorageBackend:
        """
        Create a storage backend of the specified type.
        
        Args:
            storage_type: Type of storage backend ('sqlite', 'json', 'csv')
            **kwargs: Additional arguments for the storage backend
            
        Returns:
            StorageBackend instance
        """
        if storage_type.lower() == 'sqlite':
            db_path = kwargs.get('db_path', 'reddit_data.db')
            echo = kwargs.get('echo', False)
            return SQLiteStorage(db_path=db_path, echo=echo)
        elif storage_type.lower() == 'json':
            data_dir = kwargs.get('data_dir', 'data')
            return JSONStorage(data_dir=data_dir)
        elif storage_type.lower() == 'csv':
            data_dir = kwargs.get('data_dir', 'data')
            return CSVStorage(data_dir=data_dir)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")
