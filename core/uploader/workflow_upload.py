"""流水线上传器 - 统一上传入口"""

import asyncio
import logging
from typing import Tuple, Optional
from .base import IUploader
from ..streaming_opt import StreamingOptimizer
from .file_mgr import FileManager

logger = logging.getLogger(__name__)


class WorkflowUploader(IUploader):
    """
    流水线上传器 - 唯一上传入口

    整合了：
    - 文件查找
    - 视频流优化
    - Telegram上传
    - 文件清理
    """

    UPLOAD_TIMEOUT = 300
    UPLOAD_RETRY_TIMES = 3
    UPLOAD_RETRY_DELAY = 5

    def __init__(self, db_manager=None, telethon_client=None):
        self.db = db_manager
        self.client = telethon_client
        self.streaming_opt = StreamingOptimizer()
        self.file_mgr = FileManager()

    async def upload(self, task_id: int, caption: str, group_id: int) -> Tuple[bool, str]:
        """
        完整上传流程

        1. 查找视频文件
        2. 视频流优化
        3. 上传到Telegram
        4. 更新任务状态
        5. 清理临时文件
        """
        logger.info(f"开始上传任务 {task_id}")

        video_path = self.file_mgr.find_video_file(task_id)
        if not video_path:
            return False, f"任务 {task_id} 的视频文件不存在"

        optimized_path = await self.streaming_opt.optimize(video_path)
        if not optimized_path:
            return False, "视频流优化失败"

        success = await self.upload_with_retry(optimized_path, task_id)

        if success:
            logger.info(f"任务 {task_id} 上传成功")
            if self.db:
                self.db.update_task_status(task_id, "uploaded")
            self.file_mgr.cleanup(task_id)
            return True, None
        else:
            logger.error(f"任务 {task_id} 上传失败")
            if self.db:
                self.db.update_task_status(task_id, "upload_failed", "上传失败")
            return False, "上传失败"

    async def upload_with_retry(self, file_path: str, task_id: int) -> bool:
        """带重试的上传"""
        for attempt in range(self.UPLOAD_RETRY_TIMES):
            try:
                if self.client:
                    await asyncio.wait_for(
                        self._send_to_telegram(file_path, task_id),
                        timeout=self.UPLOAD_TIMEOUT
                    )
                    return True
            except asyncio.TimeoutError:
                logger.warning(f"上传超时，第 {attempt + 1} 次尝试")
            except Exception as e:
                logger.error(f"上传失败: {e}")

            if attempt < self.UPLOAD_RETRY_TIMES - 1:
                logger.info(f"等待 {self.UPLOAD_RETRY_DELAY} 秒后重试...")
                await asyncio.sleep(self.UPLOAD_RETRY_DELAY)

        return False

    async def _send_to_telegram(self, file_path: str, task_id: int):
        """发送到Telegram"""
        if not self.client:
            raise Exception("Telegram客户端未初始化")

        task = self.db.get_task(task_id) if self.db else None
        group_targets = self.db.get_group_targets(task.group_id) if task and self.db else []

        for target in group_targets:
            await self.client.send_file(
                target,
                file_path,
                supports_streaming=True
            )
            logger.info(f"已发送到 {target}")

    async def upload_file(self, file_path: str, task_id: int) -> bool:
        """上传单个文件"""
        return await self.upload_with_retry(file_path, task_id)

    async def upload_direct(
        self,
        file_path: str,
        caption: str,
        chat_id: int,
        bot_token: str = None,
    ) -> Tuple[bool, str]:
        """
        直接上传（不依赖task_id）

        Args:
            file_path: 视频文件路径
            caption: 视频描述
            chat_id: Telegram chat_id
            bot_token: Telegram bot token
        """
        try:
            from telegram import Bot

            if not bot_token:
                from ..config import get_config
                bot_token = get_config().bot_token

            bot = Bot(token=bot_token)

            with open(file_path, "rb") as video_file:
                message = await bot.send_video(
                    chat_id=chat_id,
                    video=video_file,
                    caption=caption[:1024] if caption else None,
                    supports_streaming=True,
                    read_timeout=120,
                    write_timeout=120,
                )

            logger.info(f"直接上传成功，message_id: {message.message_id}")
            return True, ""

        except Exception as e:
            logger.error(f"直接上传失败: {e}")
            return False, str(e)