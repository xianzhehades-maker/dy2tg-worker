"""Retry decorator with exponential backoff"""
import time
import asyncio
import functools
from typing import Callable, Type, Tuple, Optional
from hf.utils.logger import get_logger

logger = get_logger("retry")


def retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable] = None
):
    """
    Retry decorator with exponential backoff for sync functions
    
    Args:
        max_retries: Maximum number of retries
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Multiplier for exponential backoff
        exceptions: Tuple of exceptions to catch and retry
        on_retry: Callback function called on retry (receives exception, attempt number)
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt >= max_retries:
                        logger.error(f"Max retries ({max_retries}) exceeded for {func.__name__}")
                        raise
                    
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    
                    if on_retry:
                        on_retry(e, attempt + 1)
                    
                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    
                    time.sleep(delay)
            
            raise last_exception
        
        return wrapper
    return decorator


def async_retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable] = None
):
    """
    Retry decorator with exponential backoff for async functions
    
    Args:
        max_retries: Maximum number of retries
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Multiplier for exponential backoff
        exceptions: Tuple of exceptions to catch and retry
        on_retry: Callback function called on retry (receives exception, attempt number)
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt >= max_retries:
                        logger.error(f"Max retries ({max_retries}) exceeded for {func.__name__}")
                        raise
                    
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    
                    if on_retry:
                        on_retry(e, attempt + 1)
                    
                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    
                    await asyncio.sleep(delay)
            
            raise last_exception
        
        return wrapper
    return decorator


def with_resource_monitoring(func: Callable):
    """
    Decorator to monitor resource usage before function execution
    (Simple implementation - checks for basic health)
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        import psutil
        
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            cpu_percent = process.cpu_percent(interval=0.1)
            
            logger.debug(f"Resource usage - Memory: {memory_mb:.1f}MB, CPU: {cpu_percent}%")
            
            if memory_mb > 12000:
                logger.warning(f"High memory usage: {memory_mb:.1f}MB")
            
        except ImportError:
            pass
        except Exception:
            pass
        
        return func(*args, **kwargs)
    
    return wrapper
