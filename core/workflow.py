"""工作流引擎 - 自动处理视频任务"""

import asyncio
import threading
import time
import logging
from typing import Optional, List
from dataclasses import dataclass
from enum import Enum

from .database import DatabaseManager, Task, get_db
from .client import VideoClient, Platform
from .ai_caption.generator import AICaptionGenerator
from .streaming_opt import StreamingOptimizer
from .uploader import WorkflowUploader
from .config import get_config

logger = logging.getLogger(__name__)

_persistence_manager = None


class TaskStatus(Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    PROCESSING = "processing"
    UPLOADING = "uploading"
    UPLOADED = "uploaded"
    FAILED = "failed"


class WorkflowTrigger:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._immediate = False
        self._lock = threading.Lock()

    def set_immediate(self):
        with self._lock:
            self._immediate = True
            logger.info("流水线立即执行标志已设置")

    def check_and_reset(self) -> bool:
        with self._lock:
            if self._immediate:
                self._immediate = False
                logger.info("流水线立即执行标志已重置")
                return True
            return False


class WorkflowEngine:
    def __init__(
        self,
        db: DatabaseManager = None,
        video_client: VideoClient = None,
        uploader: WorkflowUploader = None,
        workflow_trigger=None,
    ):
        self.db = db or get_db()
        self.video_client = video_client or VideoClient()
        self.uploader = uploader
        self.ai_generator = AICaptionGenerator()
        self.streaming_optimizer = StreamingOptimizer()

        self.config = get_config()
        self.running = False
        self.workflow_trigger = workflow_trigger or WorkflowTrigger()
        self._check_interval = 300
        self._last_check_time = 0

    def set_check_interval(self, interval: int):
        self._check_interval = interval

    def reset_interval(self):
        self._last_check_time = time.time()

    def init_persistence(self):
        """初始化持久化管理"""
        global _persistence_manager
        try:
            import hf_persistence
            _persistence_manager = hf_persistence.create_persistence_manager(auto_pull=True)
            if _persistence_manager:
                logger.info("持久化管理器已初始化")
        except Exception as e:
            logger.warning(f"持久化初始化失败: {e}")
            _persistence_manager = None

    def sync_to_cloud(self):
        """同步数据到云端"""
        if _persistence_manager:
            try:
                _persistence_manager.push_to_hub()
                logger.info("数据已同步到云端")
            except Exception as e:
                logger.error(f"同步失败: {e}")

    async def process_single_task(self, task: Task) -> bool:
        try:
            self.db.update_task_status(task.id, TaskStatus.DOWNLOADING.value)

            video_info = await self.video_client.get_video_info(task.video_url)

            if not video_info or not video_info.video_url:
                self.db.update_task_status(task.id, TaskStatus.FAILED.value, "无法获取视频信息")
                return False

            output_path = f"{self.config.download_path}/{task.video_id}.mp4"
            success, msg = await self.video_client.download_video(
                task.video_url,
                output_path,
            )

            if not success:
                self.db.update_task_status(task.id, TaskStatus.FAILED.value, msg)
                return False

            self.db.update_task_status(task.id, TaskStatus.DOWNLOADED.value)

            if output_path.endswith(".mp4"):
                output_path = self.streaming_optimizer.optimize_if_needed(output_path)

            self.db.update_task_status(task.id, TaskStatus.PROCESSING.value)

            caption = video_info.title or video_info.desc

            if self.config.ai_caption_enabled and video_info.platform == Platform.DOUYIN:
                try:
                    caption = await self.ai_generator.generate_caption(
                        video_path=output_path,
                        original_desc=caption,
                        style=self.config.ai_caption_style or "default",
                        length=self.config.ai_caption_length or 200,
                        language=self.config.ai_caption_language or "chinese",
                    )
                except Exception as e:
                    logger.warning(f"AI文案生成失败: {e}")

            if not self.uploader:
                self.db.update_task_status(task.id, TaskStatus.UPLOADING.value)
                try:
                    from telegram import Bot
                    from .config import get_config
                    config = get_config()
                    if config.bot_token:
                        bot = Bot(token=config.bot_token)
                        with open(output_path, "rb") as f:
                            await bot.send_video(
                                chat_id=task.group_id,
                                video=f,
                                caption=caption[:1024] if caption else None,
                                supports_streaming=True,
                            )
                        upload_result = (True, "")
                    else:
                        upload_result = (False, "未配置Telegram Bot")
                except Exception as e:
                    upload_result = (False, str(e))
            else:
                upload_result = await self.uploader.upload_direct(
                    file_path=output_path,
                    caption=caption,
                    chat_id=task.group_id,
                )

            if upload_result[0]:
                self.db.update_task_status(task.id, TaskStatus.UPLOADED.value)
                self.sync_to_cloud()
                return True
            else:
                self.db.update_task_status(task.id, TaskStatus.FAILED.value, upload_result[1])
                return False

        except Exception as e:
            logger.error(f"处理任务 {task.id} 异常: {e}")
            self.db.update_task_status(task.id, TaskStatus.FAILED.value, str(e))
            return False

    async def run_once(self):
        logger.info("执行一次工作流...")

        tasks = self.db.get_tasks(status=TaskStatus.PENDING.value, limit=10)

        if not tasks:
            logger.info("没有待处理任务")
            return

        logger.info(f"找到 {len(tasks)} 个待处理任务")

        for task in tasks:
            success = await self.process_single_task(task)
            await asyncio.sleep(2)

        self.reset_interval()

    async def run_loop(self):
        self.running = True
        logger.info("工作流开始运行...")

        while self.running:
            try:
                immediate = self.workflow_trigger.check_and_reset()

                if immediate:
                    await self.run_once()
                    continue

                elapsed = time.time() - self._last_check_time

                if elapsed >= self._check_interval:
                    await self.run_once()
                    self._last_check_time = time.time()
                else:
                    await asyncio.sleep(10)

            except Exception as e:
                logger.error(f"工作流异常: {e}")
                await asyncio.sleep(30)

    def start(self):
        self.running = True
        self._last_check_time = time.time()
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.run_loop())
        except RuntimeError:
            asyncio.create_task(self.run_loop())
        logger.info("工作流已启动")

    def stop(self):
        self.running = False
        logger.info("工作流已停止")

    def is_running(self) -> bool:
        return self.running