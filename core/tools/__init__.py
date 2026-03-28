"""工具模块"""

import re
import time
import asyncio
import logging
from functools import wraps
from typing import Callable, Any


logger = logging.getLogger(__name__)


def async_retry(max_attempts: int = 3, delay: float = 1.0):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(delay * (attempt + 1))
            raise last_exception
        return wrapper
    return decorator


class Cleaner:
    @staticmethod
    def clean_text(text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    @staticmethod
    def clean_filename(filename: str) -> str:
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        filename = filename.strip('. ')
        return filename[:200]


def format_size(size_bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def format_duration(seconds: float) -> str:
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes:02d}:{secs:02d}"


class Timer:
    def __init__(self):
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()

    def elapsed(self) -> float:
        if self.end_time:
            return self.end_time - self.start_time
        return time.time() - self.start_time


def cookie_dict_to_str(cookie_dict: dict) -> str:
    return "; ".join([f"{k}={v}" for k, v in cookie_dict.items()])


def cookie_str_to_dict(cookie_str: str) -> dict:
    result = {}
    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" in item:
            key, value = item.split("=", 1)
            result[key.strip()] = value.strip()
    return result


from .request_params import request_params

__all__ = [
    "async_retry",
    "Cleaner",
    "format_size",
    "format_duration",
    "Timer",
    "cookie_dict_to_str",
    "cookie_str_to_dict",
    "request_params",
]