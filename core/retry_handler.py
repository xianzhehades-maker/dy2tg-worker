"""任务重试策略模块"""

import asyncio
import time
import logging
from typing import Callable, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class RetryStrategy(Enum):
    """重试策略"""
    FIXED = "fixed"           # 固定间隔
    LINEAR = "linear"         # 线性递增
    EXPONENTIAL = "exponential"  # 指数退避


@dataclass
class RetryConfig:
    """重试配置"""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    multiplier: float = 2.0


class RetryHandler:
    """
    任务失败自动重试处理器

    支持策略：
    - FIXED: 固定间隔重试
    - LINEAR: 线性递增间隔
    - EXPONENTIAL: 指数退避（推荐）
    """

    def __init__(self, config: RetryConfig = None):
        self.config = config or RetryConfig()
        self.attempt_counts = {}

    def calculate_delay(self, attempt: int) -> float:
        """计算重试延迟"""
        if self.config.strategy == RetryStrategy.FIXED:
            return self.config.initial_delay

        elif self.config.strategy == RetryStrategy.LINEAR:
            delay = self.config.initial_delay * (1 + attempt)
            return min(delay, self.config.max_delay)

        elif self.config.strategy == RetryStrategy.EXPONENTIAL:
            delay = self.config.initial_delay * (self.config.multiplier ** attempt)
            return min(delay, self.config.max_delay)

        return self.config.initial_delay

    async def execute_with_retry(
        self,
        func: Callable,
        task_id: int,
        *args,
        **kwargs
    ) -> Tuple[bool, Any, Optional[str]]:
        """
        带重试的异步执行

        Returns:
            (success, result, error_msg)
        """
        task_key = f"task_{task_id}"
        self.attempt_counts[task_key] = 0

        last_error = None

        for attempt in range(self.config.max_attempts):
            self.attempt_counts[task_key] = attempt + 1

            try:
                logger.info(f"执行任务 {task_id}，第 {attempt + 1}/{self.config.max_attempts} 次尝试")
                result = await func(*args, **kwargs)
                logger.info(f"任务 {task_id} 执行成功")
                return True, result, None

            except Exception as e:
                last_error = str(e)
                logger.warning(f"任务 {task_id} 第 {attempt + 1} 次失败: {e}")

                if attempt < self.config.max_attempts - 1:
                    delay = self.calculate_delay(attempt)
                    logger.info(f"等待 {delay:.1f} 秒后重试...")
                    await asyncio.sleep(delay)

        logger.error(f"任务 {task_id} 重试 {self.config.max_attempts} 次后仍然失败")
        return False, None, last_error

    def get_attempt_count(self, task_id: int) -> int:
        """获取任务尝试次数"""
        task_key = f"task_{task_id}"
        return self.attempt_counts.get(task_key, 0)

    def reset_attempt(self, task_id: int):
        """重置任务尝试次数"""
        task_key = f"task_{task_id}"
        if task_key in self.attempt_counts:
            del self.attempt_counts[task_key]


class TaskRetryManager:
    """
    任务重试管理器

    管理多个任务的全局重试状态
    """

    def __init__(self):
        self.handlers = {}
        self.failed_tasks = set()
        self.success_tasks = set()

    def get_handler(self, task_id: int, config: RetryConfig = None) -> RetryHandler:
        """获取任务的重试处理器"""
        if task_id not in self.handlers:
            self.handlers[task_id] = RetryHandler(config)
        return self.handlers[task_id]

    def mark_failed(self, task_id: int):
        """标记任务失败"""
        self.failed_tasks.add(task_id)
        if task_id in self.success_tasks:
            self.success_tasks.discard(task_id)

    def mark_success(self, task_id: int):
        """标记任务成功"""
        self.success_tasks.add(task_id)
        if task_id in self.failed_tasks:
            self.failed_tasks.discard(task_id)

        if task_id in self.handlers:
            del self.handlers[task_id]

    def is_failed(self, task_id: int) -> bool:
        """检查任务是否失败"""
        return task_id in self.failed_tasks

    def get_stats(self) -> dict:
        """获取重试统计"""
        return {
            'total_handlers': len(self.handlers),
            'failed_tasks': len(self.failed_tasks),
            'success_tasks': len(self.success_tasks),
        }
