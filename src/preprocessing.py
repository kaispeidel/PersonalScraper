"""
Data Cleaning and Preprocessing Module

This module provides functionality to clean and preprocess Reddit data,
including text normalization, duplicate removal, and filtering by metadata.
"""

import re
import logging
from typing import Dict, List, Optional, Any, Union, Callable
from datetime import datetime
import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer, WordNetLemmatizer

# Configure logger
logger = logging.getLogger(__name__)

# Download NLTK resources if not already downloaded
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/stopwords')
    nltk.data.find('corpora/wordnet')
except LookupError:
    nltk.download('punkt')
    nltk.download('stopwords')
    nltk.download('wordnet')


class TextPreprocessor:
    """Class for text preprocessing operations."""
    
    def __init__(self, 
                 remove_urls: bool = True,
                 remove_special_chars: bool = True,
                 remove_numbers: bool = False,
                 remove_stopwords: bool = True,
                 lowercase: bool = True,
                 stemming: bool = False,
                 lemmatization: bool = False,
                 language: str = 'english'):
        """
        Initialize text preprocessor with specified options.
        
        Args:
            remove_urls: Whether to remove URLs
            remove_special_chars: Whether to remove special characters
            remove_numbers: Whether to remove numbers
            remove_stopwords: Whether to remove stopwords
            lowercase: Whether to convert text to lowercase
            stemming: Whether to apply stemming
            lemmatization: Whether to apply lemmatization
            language: Language for stopwords
        """
        self.remove_urls = remove_urls
        self.remove_special_chars = remove_special_chars
        self.remove_numbers = remove_numbers
        self.remove_stopwords = remove_stopwords
        self.lowercase = lowercase
        self.stemming = stemming
        self.lemmatization = lemmatization
        self.language = language
        
        # Initialize NLTK components
        if remove_stopwords:
            self.stop_words = set(stopwords.words(language))
        if stemming:
            self.stemmer = PorterStemmer()
        if lemmatization:
            self.lemmatizer = WordNetLemmatizer()
        
        logger.info("Initialized TextPreprocessor")
    
    def preprocess(self, text: str) -> str:
        """
        Apply preprocessing steps to text.
        
        Args:
            text: Input text to preprocess
            
        Returns:
            Preprocessed text
        """
        if not text or not isinstance(text, str):
            return ""
        
        # Remove URLs
        if self.remove_urls:
            text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        
        # Convert to lowercase
        if self.lowercase:
            text = text.lower()
        
        # Remove special characters
        if self.remove_special_chars:
            text = re.sub(r'[^\w\s]', '', text)
        
        # Remove numbers
        if self.remove_numbers:
            text = re.sub(r'\d+', '', text)
        
        # Tokenize
        tokens = word_tokenize(text)
        
        # Remove stopwords
        if self.remove_stopwords:
            tokens = [word for word in tokens if word not in self.stop_words]
        
        # Apply stemming
        if self.stemming:
            tokens = [self.stemmer.stem(word) for word in tokens]
        
        # Apply lemmatization
        if self.lemmatization:
            tokens = [self.lemmatizer.lemmatize(word) for word in tokens]
        
        # Join tokens back into text
        processed_text = ' '.join(tokens)
        
        return processed_text
    
    def preprocess_batch(self, texts: List[str]) -> List[str]:
        """
        Apply preprocessing to a batch of texts.
        
        Args:
            texts: List of input texts
            
        Returns:
            List of preprocessed texts
        """
        return [self.preprocess(text) for text in texts]


class DataCleaner:
    """Class for cleaning and preprocessing Reddit data."""
    
    def __init__(self, text_preprocessor: Optional[TextPreprocessor] = None):
        """
        Initialize data cleaner.
        
        Args:
            text_preprocessor: TextPreprocessor instance for text cleaning
        """
        self.text_preprocessor = text_preprocessor or TextPreprocessor()
        logger.info("Initialized DataCleaner")
    
    def remove_duplicates(self, data: List[Dict[str, Any]], key_field: str = 'id') -> List[Dict[str, Any]]:
        """
        Remove duplicate entries from data.
        
        Args:
            data: List of data dictionaries
            key_field: Field to use for identifying duplicates
            
        Returns:
            Deduplicated data
        """
        unique_data = {}
        for item in data:
            if key_field in item:
                unique_data[item[key_field]] = item
        
        result = list(unique_data.values())
        logger.info(f"Removed {len(data) - len(result)} duplicates from {len(data)} items")
        return result
    
    def filter_by_date(self, 
                      data: List[Dict[str, Any]], 
                      date_field: str = 'created_utc',
                      start_date: Optional[datetime] = None,
                      end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Filter data by date range.
        
        Args:
            data: List of data dictionaries
            date_field: Field containing date information
            start_date: Start date for filtering (inclusive)
            end_date: End date for filtering (inclusive)
            
        Returns:
            Filtered data
        """
        if not start_date and not end_date:
            return data
        
        filtered_data = []
        for item in data:
            if date_field in item:
                date_value = item[date_field]
                
                # Convert string to datetime if needed
                if isinstance(date_value, str):
                    try:
                        date_value = datetime.fromisoformat(date_value)
                    except ValueError:
                        continue
                
                # Apply date filters
                if start_date and date_value < start_date:
                    continue
                if end_date and date_value > end_date:
                    continue
                
                filtered_data.append(item)
        
        logger.info(f"Filtered {len(data) - len(filtered_data)} items by date from {len(data)} items")
        return filtered_data
    
    def filter_by_score(self,
                       data: List[Dict[str, Any]],
                       score_field: str = 'score',
                       min_score: Optional[int] = None,
                       max_score: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Filter data by score range.
        
        Args:
            data: List of data dictionaries
            score_field: Field containing score information
            min_score: Minimum score (inclusive)
            max_score: Maximum score (inclusive)
            
        Returns:
            Filtered data
        """
        if min_score is None and max_score is None:
            return data
        
        filtered_data = []
        for item in data:
            if score_field in item:
                score = item[score_field]
                
                # Apply score filters
                if min_score is not None and score < min_score:
                    continue
                if max_score is not None and score > max_score:
                    continue
                
                filtered_data.append(item)
        
        logger.info(f"Filtered {len(data) - len(filtered_data)} items by score from {len(data)} items")
        return filtered_data
    
    def filter_by_custom(self,
                        data: List[Dict[str, Any]],
                        filter_func: Callable[[Dict[str, Any]], bool]) -> List[Dict[str, Any]]:
        """
        Filter data using a custom filter function.
        
        Args:
            data: List of data dictionaries
            filter_func: Function that takes an item and returns True to keep it
            
        Returns:
            Filtered data
        """
        filtered_data = [item for item in data if filter_func(item)]
        logger.info(f"Filtered {len(data) - len(filtered_data)} items by custom filter from {len(data)} items")
        return filtered_data
    
    def clean_text_fields(self,
                         data: List[Dict[str, Any]],
                         text_fields: List[str]) -> List[Dict[str, Any]]:
        """
        Clean specified text fields in data.
        
        Args:
            data: List of data dictionaries
            text_fields: List of field names containing text to clean
            
        Returns:
            Data with cleaned text fields
        """
        cleaned_data = []
        for item in data:
            cleaned_item = item.copy()
            for field in text_fields:
                if field in item and item[field]:
                    cleaned_item[field] = self.text_preprocessor.preprocess(item[field])
            cleaned_data.append(cleaned_item)
        
        logger.info(f"Cleaned text fields {text_fields} in {len(data)} items")
        return cleaned_data
    
    def add_derived_fields(self,
                          data: List[Dict[str, Any]],
                          derivation_funcs: Dict[str, Callable[[Dict[str, Any]], Any]]) -> List[Dict[str, Any]]:
        """
        Add derived fields to data.
        
        Args:
            data: List of data dictionaries
            derivation_funcs: Dictionary mapping new field names to functions that derive their values
            
        Returns:
            Data with added derived fields
        """
        enhanced_data = []
        for item in data:
            enhanced_item = item.copy()
            for field_name, func in derivation_funcs.items():
                enhanced_item[field_name] = func(item)
            enhanced_data.append(enhanced_item)
        
        logger.info(f"Added derived fields {list(derivation_funcs.keys())} to {len(data)} items")
        return enhanced_data
    
    def clean_posts(self,
                   posts: List[Dict[str, Any]],
                   remove_duplicates: bool = True,
                   clean_title: bool = True,
                   clean_selftext: bool = True,
                   min_score: Optional[int] = None,
                   start_date: Optional[datetime] = None,
                   end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Clean and preprocess post data.
        
        Args:
            posts: List of post dictionaries
            remove_duplicates: Whether to remove duplicate posts
            clean_title: Whether to clean post titles
            clean_selftext: Whether to clean post selftext
            min_score: Minimum score for filtering
            start_date: Start date for filtering
            end_date: End date for filtering
            
        Returns:
            Cleaned post data
        """
        logger.info(f"Cleaning {len(posts)} posts")
        
        # Remove duplicates
        if remove_duplicates:
            posts = self.remove_duplicates(posts)
        
        # Filter by date
        if start_date or end_date:
            posts = self.filter_by_date(posts, start_date=start_date, end_date=end_date)
        
        # Filter by score
        if min_score is not None:
            posts = self.filter_by_score(posts, min_score=min_score)
        
        # Clean text fields
        text_fields = []
        if clean_title:
            text_fields.append('title')
        if clean_selftext:
            text_fields.append('selftext')
        
        if text_fields:
            posts = self.clean_text_fields(posts, text_fields)
        
        logger.info(f"Finished cleaning posts, {len(posts)} posts remaining")
        return posts
    
    def clean_comments(self,
                      comments: List[Dict[str, Any]],
                      remove_duplicates: bool = True,
                      clean_body: bool = True,
                      min_score: Optional[int] = None,
                      start_date: Optional[datetime] = None,
                      end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Clean and preprocess comment data.
        
        Args:
            comments: List of comment dictionaries
            remove_duplicates: Whether to remove duplicate comments
            clean_body: Whether to clean comment body text
            min_score: Minimum score for filtering
            start_date: Start date for filtering
            end_date: End date for filtering
            
        Returns:
            Cleaned comment data
        """
        logger.info(f"Cleaning {len(comments)} comments")
        
        # Remove duplicates
        if remove_duplicates:
            comments = self.remove_duplicates(comments)
        
        # Filter by date
        if start_date or end_date:
            comments = self.filter_by_date(comments, start_date=start_date, end_date=end_date)
        
        # Filter by score
        if min_score is not None:
            comments = self.filter_by_score(comments, min_score=min_score)
        
        # Clean text fields
        if clean_body:
            comments = self.clean_text_fields(comments, ['body'])
        
        logger.info(f"Finished cleaning comments, {len(comments)} comments remaining")
        return comments


class DataPreprocessor:
    """Class for preparing data for analysis or ML tasks."""
    
    def __init__(self, cleaner: Optional[DataCleaner] = None):
        """
        Initialize data preprocessor.
        
        Args:
            cleaner: DataCleaner instance for data cleaning
        """
        self.cleaner = cleaner or DataCleaner()
        logger.info("Initialized DataPreprocessor")
    
    def prepare_for_analysis(self,
                            posts: List[Dict[str, Any]],
                            comments: List[Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
        """
        Prepare data for analysis by converting to pandas DataFrames.
        
        Args:
            posts: List of post dictionaries
            comments: List of comment dictionaries
            
        Returns:
            Dictionary with 'posts' and 'comments' DataFrames
        """
        posts_df = pd.DataFrame(posts)
        comments_df = pd.DataFrame(comments)
        
        logger.info(f"Prepared {len(posts)} posts and {len(comments)} comments for analysis")
        return {
            'posts': posts_df,
            'comments': comments_df
        }
    
    def prepare_for_ml(self,
                      posts: List[Dict[str, Any]],
                      comments: List[Dict[str, Any]],
                      post_text_field: str = 'selftext',
                      comment_text_field: str = 'body') -> Dict[str, Any]:
        """
        Prepare data for machine learning tasks.
        
        Args:
            posts: List of post dictionaries
            comments: List of comment dictionaries
            post_text_field: Field containing post text
            comment_text_field: Field containing comment text
            
        Returns:
            Dictionary with prepared data for ML
        """
        # Convert to DataFrames
        posts_df = pd.DataFrame(posts)
        comments_df = pd.DataFrame(comments)
        
        # Extract text data for ML
        post_texts = posts_df[post_text_field].fillna('').tolist()
        comment_texts = comments_df[comment_text_field].fillna('').tolist()
        
        # Combine all texts for corpus analysis
        all_texts = post_texts + comment_texts
        
        logger.info(f"Prepared {len(posts)} posts and {len(comments)} comments for ML")
        return {
            'posts_df': posts_df,
            'comments_df': comments_df,
            'post_texts': post_texts,
            'comment_texts': comment_texts,
            'all_texts': all_texts
        }
    
    def extract_features(self,
                        posts: List[Dict[str, Any]],
                        comments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract features from posts and comments for analysis.
        
        Args:
            posts: List of post dictionaries
            comments: List of comment dictionaries
            
        Returns:
            Dictionary with extracted features
        """
        # Convert to DataFrames
        posts_df = pd.DataFrame(posts)
        comments_df = pd.DataFrame(comments)
        
        # Extract post features
        post_features = {}
        if not posts_df.empty:
            post_features = {
                'avg_score': posts_df['score'].mean(),
                'max_score': posts_df['score'].max(),
                'avg_comments': posts_df['num_comments'].mean(),
                'total_posts': len(posts_df),
                'self_post_ratio': posts_df['is_self'].mean() if 'is_self' in posts_df else None,
                'top_domains': posts_df['domain'].value_counts().head(10).to_dict() if 'domain' in posts_df else None
            }
        
        # Extract comment features
        comment_features = {}
        if not comments_df.empty:
            comment_features = {
                'avg_score': comments_df['score'].mean(),
                'max_score': comments_df['score'].max(),
                'total_comments': len(comments_df),
                'avg_depth': comments_df['depth'].mean() if 'depth' in comments_df else None
            }
        
        logger.info(f"Extracted features from {len(posts)} posts and {len(comments)} comments")
        return {
            'post_features': post_features,
            'comment_features': comment_features
        }
