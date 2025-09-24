"""
Logging and Error Handling Module

This module provides centralized logging configuration and error handling utilities
for the Reddit scraper pipeline.
"""

import os
import sys
import logging
import traceback
import time
from typing import Callable, Any, Optional, Dict, List, Union
from functools import wraps
import colorlog

# Define log levels
LOG_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}

class RetryException(Exception):
    """Exception raised when retry attempts are exhausted."""
    pass

def setup_logging(log_dir: str = 'logs', 
                 log_file: str = 'reddit_scraper.log',
                 console_level: str = 'info',
                 file_level: str = 'debug',
                 log_format: Optional[str] = None) -> logging.Logger:
    """
    Set up logging configuration for the application.
    
    Args:
        log_dir: Directory to store log files
        log_file: Name of the log file
        console_level: Logging level for console output
        file_level: Logging level for file output
        log_format: Custom log format string
        
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Capture all levels
    
    # Clear existing handlers
    logger.handlers = []
    
    # Define log format
    if log_format is None:
        log_format = '%(log_color)s%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    
    # Create formatter
    formatter = colorlog.ColoredFormatter(
        log_format,
        datefmt='%Y-%m-%d %H:%M:%S',
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    )
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(LOG_LEVELS.get(console_level.lower(), logging.INFO))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create file handler
    file_handler = logging.FileHandler(os.path.join(log_dir, log_file))
    file_handler.setLevel(LOG_LEVELS.get(file_level.lower(), logging.DEBUG))
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    logger.addHandler(file_handler)
    
    # Log the setup
    logger.info(f"Logging configured: console={console_level}, file={file_level} ({os.path.join(log_dir, log_file)})")
    
    return logger

def retry(max_attempts: int = 3, 
          delay: float = 1.0, 
          backoff: float = 2.0,
          exceptions: tuple = (Exception,),
          logger: Optional[logging.Logger] = None):
    """
    Retry decorator with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier (how much to increase delay each retry)
        exceptions: Tuple of exceptions to catch and retry
        logger: Logger instance for logging retries
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            local_logger = logger or logging.getLogger(func.__module__)
            attempt = 1
            current_delay = delay
            
            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        local_logger.error(f"All {max_attempts} retry attempts failed for {func.__name__}: {str(e)}")
                        raise RetryException(f"Function {func.__name__} failed after {max_attempts} attempts: {str(e)}")
                    
                    local_logger.warning(f"Retry {attempt}/{max_attempts} for {func.__name__} after error: {str(e)}")
                    local_logger.warning(f"Waiting {current_delay:.2f}s before next attempt")
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
                    attempt += 1
        
        return wrapper
    return decorator

def safe_execute(func: Callable, 
                *args, 
                default_return: Any = None, 
                log_exception: bool = True,
                logger: Optional[logging.Logger] = None,
                **kwargs) -> Any:
    """
    Safely execute a function, catching and logging any exceptions.
    
    Args:
        func: Function to execute
        *args: Arguments to pass to the function
        default_return: Value to return if an exception occurs
        log_exception: Whether to log the exception
        logger: Logger instance for logging exceptions
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        Function result or default_return if an exception occurs
    """
    local_logger = logger or logging.getLogger()
    
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if log_exception:
            local_logger.error(f"Error executing {func.__name__}: {str(e)}")
            local_logger.debug(f"Exception traceback: {traceback.format_exc()}")
        return default_return

class ErrorHandler:
    """Class for centralized error handling."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize error handler.
        
        Args:
            logger: Logger instance for logging errors
        """
        self.logger = logger or logging.getLogger(__name__)
    
    def handle_error(self, 
                    error: Exception, 
                    context: Optional[Dict[str, Any]] = None,
                    critical: bool = False,
                    notify: bool = False) -> None:
        """
        Handle an error with appropriate logging and notification.
        
        Args:
            error: Exception to handle
            context: Additional context information
            critical: Whether this is a critical error
            notify: Whether to send notifications
        """
        # Format error message with context
        context_str = f" (Context: {context})" if context else ""
        error_message = f"{type(error).__name__}: {str(error)}{context_str}"
        
        # Log the error
        if critical:
            self.logger.critical(error_message)
            self.logger.debug(f"Critical error traceback: {traceback.format_exc()}")
        else:
            self.logger.error(error_message)
            self.logger.debug(f"Error traceback: {traceback.format_exc()}")
        
        # Add notification logic here if needed
        if notify:
            # This could be extended to send emails, Slack messages, etc.
            self.logger.info(f"Notification would be sent for: {error_message}")
    
    def with_error_handling(self, 
                           func: Callable, 
                           *args, 
                           context: Optional[Dict[str, Any]] = None,
                           critical: bool = False,
                           notify: bool = False,
                           default_return: Any = None,
                           **kwargs) -> Any:
        """
        Execute a function with error handling.
        
        Args:
            func: Function to execute
            *args: Arguments to pass to the function
            context: Additional context information
            critical: Whether errors are critical
            notify: Whether to send notifications for errors
            default_return: Value to return if an exception occurs
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            Function result or default_return if an exception occurs
        """
        try:
            return func(*args, **kwargs)
        except Exception as e:
            self.handle_error(e, context=context, critical=critical, notify=notify)
            return default_return
