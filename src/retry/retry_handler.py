#!/usr/bin/env python3
"""
Retry Handler
Implements exponential backoff retry logic with error classification
"""

import time
import random
import logging
from typing import Callable, Any, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RetryableError(Exception):
    """Exception for temporary failures that should be retried"""
    pass


class PermanentError(Exception):
    """Exception for permanent failures that should not be retried"""
    pass


class RetryHandler:
    """
    Handles retry logic with exponential backoff
    
    Features:
    - Configurable max retries
    - Exponential backoff with jitter
    - Error classification (retryable vs permanent)
    """
    
    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        backoff_multiplier: float = 2.0,
        max_delay: float = 30.0,
        jitter: bool = True
    ):
        """
        Initialize retry handler
        
        Args:
            max_retries: Maximum number of retry attempts
            initial_delay: Initial delay in seconds
            backoff_multiplier: Multiplier for exponential backoff
            max_delay: Maximum delay in seconds
            jitter: Whether to add random jitter to delays
        """
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.backoff_multiplier = backoff_multiplier
        self.max_delay = max_delay
        self.jitter = jitter
    
    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for a given attempt with exponential backoff
        
        Args:
            attempt: Retry attempt number (0-indexed)
            
        Returns:
            Delay in seconds
        """
        # Calculate exponential delay
        delay = min(
            self.initial_delay * (self.backoff_multiplier ** attempt),
            self.max_delay
        )
        
        # Add jitter (±20% randomization)
        if self.jitter:
            jitter_range = delay * 0.2
            delay = delay + random.uniform(-jitter_range, jitter_range)
            delay = max(0.1, delay)  # Ensure positive delay
        
        return delay
    
    def execute_with_retry(
        self,
        func: Callable,
        *args,
        error_context: Optional[str] = None,
        **kwargs
    ) -> Any:
        """
        Execute a function with retry logic
        
        Args:
            func: Function to execute
            *args: Positional arguments for the function
            error_context: Context string for error messages
            **kwargs: Keyword arguments for the function
            
        Returns:
            Result of the function execution
            
        Raises:
            PermanentError: If a permanent error occurs or max retries exceeded
        """
        last_exception = None
        context = error_context or func.__name__
        
        for attempt in range(self.max_retries + 1):
            try:
                # Attempt to execute the function
                result = func(*args, **kwargs)
                
                # Log success if this was a retry
                if attempt > 0:
                    logger.info(f"✓ Retry succeeded for {context} on attempt {attempt + 1}")
                
                return result
                
            except PermanentError as e:
                # Don't retry permanent errors
                logger.error(f"❌ Permanent error in {context}: {e}")
                raise
                
            except RetryableError as e:
                last_exception = e
                
                if attempt < self.max_retries:
                    # Calculate delay and retry
                    delay = self.calculate_delay(attempt)
                    logger.warning(
                        f"⚠️  Retryable error in {context} (attempt {attempt + 1}/{self.max_retries}): {e}"
                    )
                    logger.info(f"   Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)
                else:
                    # Max retries exceeded
                    logger.error(
                        f"❌ Max retries ({self.max_retries}) exceeded for {context}: {e}"
                    )
                    raise PermanentError(f"Max retries exceeded for {context}") from e
                    
            except Exception as e:
                # Unexpected error - treat as permanent
                logger.error(f"❌ Unexpected error in {context}: {e}")
                raise PermanentError(f"Unexpected error in {context}: {e}") from e
        
        # Should never reach here, but just in case
        if last_exception:
            raise PermanentError(f"Failed after {self.max_retries} retries") from last_exception
    
    def classify_error(self, error: Exception) -> bool:
        """
        Classify whether an error is retryable
        
        Args:
            error: Exception to classify
            
        Returns:
            True if retryable, False if permanent
        """
        # Already classified errors
        if isinstance(error, RetryableError):
            return True
        if isinstance(error, PermanentError):
            return False
        
        # Common retryable error patterns
        retryable_patterns = [
            'timeout',
            'connection',
            'unavailable',
            'temporary',
            'network',
            'socket',
            '503',
            '504',
            '429',  # Rate limit
        ]
        
        error_str = str(error).lower()
        
        for pattern in retryable_patterns:
            if pattern in error_str:
                return True
        
        # Default to permanent for safety
        return False
    
    def wrap_error(self, error: Exception) -> Exception:
        """
        Wrap an exception as either RetryableError or PermanentError
        
        Args:
            error: Original exception
            
        Returns:
            Wrapped exception
        """
        if isinstance(error, (RetryableError, PermanentError)):
            return error
        
        if self.classify_error(error):
            wrapped_error = RetryableError(str(error))
            wrapped_error.__cause__ = error
            return wrapped_error
        else:
            wrapped_error = PermanentError(str(error))
            wrapped_error.__cause__ = error
            return wrapped_error


def simulate_temporary_failure(attempt_count: list):
    """Simulate a function that fails temporarily then succeeds"""
    attempt_count[0] += 1
    
    if attempt_count[0] < 3:
        raise RetryableError(f"Temporary failure (attempt {attempt_count[0]})")
    
    return f"Success on attempt {attempt_count[0]}"


def simulate_permanent_failure():
    """Simulate a function that always fails"""
    raise PermanentError("Invalid data format")


# Example usage
if __name__ == "__main__":
    retry_handler = RetryHandler(
        max_retries=3,
        initial_delay=1.0,
        backoff_multiplier=2.0,
        jitter=True
    )
    
    # Test 1: Temporary failure that succeeds
    print("\n=== Test 1: Temporary Failure (should succeed) ===")
    attempt_count = [0]
    try:
        result = retry_handler.execute_with_retry(
            simulate_temporary_failure,
            attempt_count,
            error_context="Test 1"
        )
        print(f"Result: {result}")
    except Exception as e:
        print(f"Failed: {e}")
    
    # Test 2: Permanent failure
    print("\n=== Test 2: Permanent Failure (should fail immediately) ===")
    try:
        result = retry_handler.execute_with_retry(
            simulate_permanent_failure,
            error_context="Test 2"
        )
        print(f"Result: {result}")
    except PermanentError as e:
        print(f"Failed as expected: {e}")
    
    # Test 3: Success on first try
    print("\n=== Test 3: Immediate Success ===")
    try:
        result = retry_handler.execute_with_retry(
            lambda: "Success!",
            error_context="Test 3"
        )
        print(f"Result: {result}")
    except Exception as e:
        print(f"Failed: {e}")
