"""Telegram 通知模块 - 任务完成/失败时发送通知"""

import os
import sys
import asyncio
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config


class TelegramNotifier:
    """Telegram 通知器"""
    
    def __init__(self):
        self.bot = None
        self.chat_id = None
        self.initialized = False
        self.init_bot()
    
    def init_bot(self):
        """初始化 Bot"""
        try:
            telegram_config = config.TELEGRAM_BOTS
            if not telegram_config:
                print("[通知器] 未配置Telegram Bot，通知功能禁用")
                return
            
            bot_config = list(telegram_config.values())[0] if isinstance(telegram_config, dict) else telegram_config[0]
            
            bot_token = bot_config.get("bot_token") or bot_config.get("BOT_TOKEN")
            chat_id = bot_config.get("chat_id") or bot_config.get("CHAT_ID")
            
            if not bot_token or not chat_id:
                print("[通知器] Bot配置不完整，通知功能禁用")
                return
            
            from telegram import Bot
            self.bot = Bot(token=bot_token)
            self.chat_id = chat_id
            self.initialized = True
            print("[通知器] 初始化成功")
            
        except Exception as e:
            print(f"[通知器] 初始化失败: {e}")
            self.initialized = False
    
    async def send_message(self, text: str, parse_mode: str = "Markdown") -> bool:
        """
        发送消息
        
        Args:
            text: 消息内容
            parse_mode: 解析模式（Markdown或HTML）
            
        Returns:
            True表示成功，False表示失败
        """
        if not self.initialized or not self.bot:
            return False
        
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode=parse_mode,
                disable_web_page_preview=True
            )
            return True
        except Exception as e:
            print(f"[通知器] 发送消息失败: {e}")
            return False
    
    async def notify_task_uploaded(self, task_id: int, video_url: str, up_name: str = "未知") -> bool:
        """
        通知任务上传成功
        
        Args:
            task_id: 任务ID
            video_url: 视频URL
            up_name: UP主名称
            
        Returns:
            True表示成功
        """
        text = f"""✅ *视频上传成功*

📋 任务ID: `{task_id}`
👤 UP主: {up_name}
🔗 视频链接: {video_url}
⏰ 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
        
        return await self.send_message(text)
    
    async def notify_task_failed(self, task_id: int, video_url: str, error_msg: str, up_name: str = "未知") -> bool:
        """
        通知任务处理失败
        
        Args:
            task_id: 任务ID
            video_url: 视频URL
            error_msg: 错误信息
            up_name: UP主名称
            
        Returns:
            True表示成功
        """
        text = f"""❌ *任务处理失败*

📋 任务ID: `{task_id}`
👤 UP主: {up_name}
🔗 视频链接: {video_url}
❌ 错误: {error_msg}
⏰ 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
        
        return await self.send_message(text)
    
    async def notify_new_videos(self, count: int, up_name: str = "未知") -> bool:
        """
        通知发现新视频
        
        Args:
            count: 新视频数量
            up_name: UP主名称
            
        Returns:
            True表示成功
        """
        text = f"""🎬 *发现新视频*

👤 UP主: {up_name}
📊 数量: {count} 个
⏰ 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
        
        return await self.send_message(text)
    
    async def notify_daily_summary(self, uploaded: int, failed: int, pending: int) -> bool:
        """
        通知每日统计
        
        Args:
            uploaded: 已上传数量
            failed: 失败数量
            pending: 待处理数量
            
        Returns:
            True表示成功
        """
        text = f"""📊 *每日统计报告*

✅ 已上传: {uploaded} 个
❌ 失败: {failed} 个
⏳ 待处理: {pending} 个
📅 日期: {datetime.now().strftime('%Y-%m-%d')}"""
        
        return await self.send_message(text)


# 全局通知器实例
_notifier: Optional[TelegramNotifier] = None


def get_notifier() -> TelegramNotifier:
    """获取全局通知器实例"""
    global _notifier
    if _notifier is None:
        _notifier = TelegramNotifier()
    return _notifier


async def notify_task_uploaded_async(task_id: int, video_url: str, up_name: str = "未知"):
    """异步发送上传成功通知（便捷函数）"""
    notifier = get_notifier()
    if notifier.initialized:
        await notifier.notify_task_uploaded(task_id, video_url, up_name)


async def notify_task_failed_async(task_id: int, video_url: str, error_msg: str, up_name: str = "未知"):
    """异步发送失败通知（便捷函数）"""
    notifier = get_notifier()
    if notifier.initialized:
        await notifier.notify_task_failed(task_id, video_url, error_msg, up_name)


def notify_task_uploaded_sync(task_id: int, video_url: str, up_name: str = "未知"):
    """同步发送上传成功通知（便捷函数）"""
    try:
        asyncio.run(notify_task_uploaded_async(task_id, video_url, up_name))
    except:
        pass


def notify_task_failed_sync(task_id: int, video_url: str, error_msg: str, up_name: str = "未知"):
    """同步发送失败通知（便捷函数）"""
    try:
        asyncio.run(notify_task_failed_async(task_id, video_url, error_msg, up_name))
    except:
        pass

