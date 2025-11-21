#!/usr/bin/env python3
"""
Test suite for RetryHandler
"""

import pytest
import time
import sys
sys.path.append('..')

from src.retry.retry_handler import (
    RetryHandler,
    RetryableError,
    PermanentError
)


def test_retry_handler_initialization():
    """Test retry handler initializes with correct defaults"""
    handler = RetryHandler()
    assert handler.max_retries == 3
    assert handler.initial_delay == 1.0
    assert handler.backoff_multiplier == 2.0


def test_calculate_delay():
    """Test exponential backoff delay calculation"""
    handler = RetryHandler(initial_delay=1.0, backoff_multiplier=2.0, jitter=False)
    
    assert handler.calculate_delay(0) == 1.0   # 1 * 2^0
    assert handler.calculate_delay(1) == 2.0   # 1 * 2^1
    assert handler.calculate_delay(2) == 4.0   # 1 * 2^2


def test_max_delay_cap():
    """Test that delay is capped at max_delay"""
    handler = RetryHandler(
        initial_delay=10.0,
        backoff_multiplier=10.0,
        max_delay=30.0,
        jitter=False
    )
    
    delay = handler.calculate_delay(5)
    assert delay <= 30.0


def test_successful_execution_no_retry():
    """Test successful execution without retries"""
    handler = RetryHandler()
    
    def success_func():
        return "success"
    
    result = handler.execute_with_retry(success_func)
    assert result == "success"


def test_retry_on_retryable_error():
    """Test retries occur for retryable errors"""
    handler = RetryHandler(max_retries=2, initial_delay=0.1)
    attempt_count = [0]
    
    def temp_failure():
        attempt_count[0] += 1
        if attempt_count[0] < 2:
            raise RetryableError("Temporary failure")
        return "success"
    
    result = handler.execute_with_retry(temp_failure)
    assert result == "success"
    assert attempt_count[0] == 2


def test_permanent_error_no_retry():
    """Test permanent errors are not retried"""
    handler = RetryHandler(max_retries=3, initial_delay=0.1)
    attempt_count = [0]
    
    def permanent_failure():
        attempt_count[0] += 1
        raise PermanentError("Invalid data")
    
    with pytest.raises(PermanentError):
        handler.execute_with_retry(permanent_failure)
    
    assert attempt_count[0] == 1  # Only tried once


def test_max_retries_exceeded():
    """Test max retries exceeded raises PermanentError"""
    handler = RetryHandler(max_retries=2, initial_delay=0.1)
    attempt_count = [0]
    
    def always_fails():
        attempt_count[0] += 1
        raise RetryableError("Always fails")
    
    with pytest.raises(PermanentError) as exc_info:
        handler.execute_with_retry(always_fails)
    
    assert "Max retries exceeded" in str(exc_info.value)
    assert attempt_count[0] == 3  # Initial + 2 retries


def test_error_classification():
    """Test error classification as retryable or permanent"""
    handler = RetryHandler()
    
    # Retryable patterns
    assert handler.classify_error(Exception("Connection timeout")) == True
    assert handler.classify_error(Exception("Service unavailable")) == True
    assert handler.classify_error(Exception("Network error")) == True
    assert handler.classify_error(Exception("503 error")) == True
    
    # Permanent (default)
    assert handler.classify_error(Exception("Invalid format")) == False
    assert handler.classify_error(Exception("Schema mismatch")) == False


def test_retry_timing():
    """Test that retry delays are applied"""
    handler = RetryHandler(max_retries=2, initial_delay=0.2, jitter=False)
    attempt_count = [0]
    
    def temp_failure():
        attempt_count[0] += 1
        if attempt_count[0] < 3:
            raise RetryableError("Temporary")
        return "success"
    
    start_time = time.time()
    result = handler.execute_with_retry(temp_failure)
    elapsed = time.time() - start_time
    
    # Should have delays: 0.2 + 0.4 = 0.6 seconds minimum
    assert elapsed >= 0.5
    assert result == "success"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
