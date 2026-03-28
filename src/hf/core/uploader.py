"""Telegram Bot Uploader - Pure TGBot Version (using python-telegram-bot)"""
import os
import sys
import time
import subprocess
import asyncio
import traceback
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    """Main function - All initialization here"""
    from hf.utils.module_initializer import initialize_module
    initialize_module(__file__)
    
    from database import DatabaseManager
    from hf.utils.notifier import get_notifier
    import config

    DOWNLOAD_PATH = config.DOWNLOAD_PATH
    PROCESSED_PATH = config.PROCESSED_PATH
    UPLOAD_PATH = config.UPLOAD_PATH

    MAX_RETRIES = config.MAX_RETRIES
    RETRY_DELAY = config.RETRY_DELAY

    TELEGRAM_BOTS = config.TELEGRAM_BOTS

    def log(message, level="INFO"):
        """Simple logging output"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] [{level}] {message}")

    def get_ffmpeg_path():
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(script_dir)
        ffmpeg_exe = os.path.join(project_dir, 'ffmpeg.exe')
        if os.path.exists(ffmpeg_exe):
            return ffmpeg_exe
        return 'ffmpeg'

    def fix_video_for_streaming(input_path):
        """Video streaming optimization (Fast Start)"""
        if not input_path.endswith(".mp4") or "_stream.mp4" in input_path:
            return input_path
            
        output_path = input_path.replace(".mp4", "_stream.mp4")
        ffmpeg_path = get_ffmpeg_path()
        cmd = [
            ffmpeg_path, '-y', '-i', input_path,
            '-c', 'copy',
            '-movflags', '+faststart',
            output_path
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, timeout=300)
            log(f"Video optimized: {os.path.basename(output_path)}")
            return output_path
        except Exception as e:
            log(f"Video optimization skipped: {e}", "WARN")
            return input_path

    def cleanup_all_task_files(task_id):
        """Clean up all task-related files"""
        files_to_check = []
        
        raw_file = os.path.join(DOWNLOAD_PATH, f"{task_id}.mp4")
        processed_file1 = os.path.join(PROCESSED_PATH, f"{task_id}.mp4")
        processed_file2 = os.path.join(PROCESSED_PATH, f"{task_id}_watermarked.mp4")
        upload_file1 = os.path.join(UPLOAD_PATH, f"{task_id}.mp4")
        upload_file2 = os.path.join(UPLOAD_PATH, f"{task_id}_watermarked.mp4")
        
        files_to_check.extend([
            raw_file, processed_file1, processed_file2,
            upload_file1, upload_file2
        ])
        
        deleted_count = 0
        for file_path in files_to_check:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    deleted_count += 1
                    log(f"Deleted file: {os.path.basename(file_path)}")
                except Exception as e:
                    log(f"Failed to delete file {os.path.basename(file_path)}: {e}", "WARN")
        
        if deleted_count > 0:
            log(f"Cleaned up {deleted_count} files")


    class TelegramBotUploader:
        """Pure TGBot Uploader - using python-telegram-bot"""
        
        def __init__(self):
            self.bot = None
            self.bot_token = None
            self.chat_id = None
            self.db_manager = DatabaseManager()
            self.is_group_mode = False
            self.current_targets = []
        
        def init_bot(self, bot_config):
            """Initialize Bot"""
            try:
                bot_token = bot_config.get("bot_token") or bot_config.get("BOT_TOKEN")
                chat_id = bot_config.get("chat_id") or bot_config.get("CHAT_ID")
                
                if not bot_token:
                    log("Bot config incomplete", "ERROR")
                    return False
                
                log("Initializing Telegram Bot...")
                
                from telegram import Bot
                self.bot = Bot(token=bot_token)
                self.bot_token = bot_token
                self.chat_id = chat_id
                
                self.is_group_mode = not chat_id
                
                if self.is_group_mode:
                    log("Group mode enabled - will select target channel based on task group")
                else:
                    log("Simple mode enabled - will use fixed target channel")
                
                log("Telegram Bot initialized successfully")
                return True
                
            except Exception as e:
                log(f"Bot initialization failed: {e}", "ERROR")
                traceback.print_exc()
                return False
        
        async def upload_file(self, file_path, task_id):
            """Upload file"""
            if not self.bot:
                log("Bot not initialized", "ERROR")
                return False
            
            try:
                if not os.path.exists(file_path):
                    filename = os.path.basename(file_path)
                    upload_path = os.path.join(UPLOAD_PATH, filename)
                    if os.path.exists(upload_path):
                        file_path = upload_path
                        log(f"Reading from upload directory: {os.path.basename(file_path)}")
                    else:
                        log(f"File not found: {os.path.basename(file_path)}", "ERROR")
                        return False
                
                log(f"Starting upload: {os.path.basename(file_path)}")
                original_path = file_path
                
                log("Optimizing video index...")
                final_path = fix_video_for_streaming(file_path)
                
                task_info = self.db_manager.get_task(task_id)
                if not task_info:
                    log(f"Task {task_id} not found", "ERROR")
                    return False
                
                up_name = "Quality Creator"
                if task_info.customer_id:
                    customer = self.db_manager.get_customer(task_info.customer_id)
                    if customer:
                        up_name = customer.name or "Unknown"
                
                ai_caption = task_info.ai_caption if hasattr(task_info, 'ai_caption') else None
                caption = ai_caption if ai_caption else f"🎬 Author: {up_name}"
                
                log(f"Upload caption: {caption[:100]}...")
                
                target_channels = []
                if self.is_group_mode and task_info.group_id:
                    targets = self.db_manager.get_group_targets(task_info.group_id)
                    if targets:
                        target_channels = [t.target_channel for t in targets if t.target_channel]
                        log(f"Group mode: found {len(target_channels)} target channels")
                    else:
                        log(f"Group {task_info.group_id} has no target channels configured", "WARN")
                        return False
                elif self.chat_id:
                    target_channels = [self.chat_id]
                    log(f"Simple mode: using fixed target channel")
                else:
                    log("No target channel configured", "ERROR")
                    return False
                
                upload_success_count = 0
                for target_channel in target_channels:
                    try:
                        log(f"Uploading to channel: {target_channel}")
                        with open(final_path, 'rb') as video_file:
                            await self.bot.send_video(
                                chat_id=target_channel,
                                video=video_file,
                                caption=caption,
                                supports_streaming=True,
                                read_timeout=300,
                                write_timeout=300,
                                connect_timeout=300,
                                pool_timeout=300
                            )
                        upload_success_count += 1
                        log(f"Successfully uploaded to: {target_channel}")
                    except Exception as e:
                        log(f"Upload to {target_channel} failed: {e}", "ERROR")
                
                if upload_success_count == 0:
                    log("All channel uploads failed", "ERROR")
                    return False
                
                log(f"Upload successful! Uploaded to {upload_success_count}/{len(target_channels)} channels")
                
                temp_file_deleted = False
                if final_path != original_path and os.path.exists(final_path):
                    try:
                        os.remove(final_path)
                        temp_file_deleted = True
                        log(f"Cleaned up temporary optimized file")
                    except Exception as e:
                        log(f"Failed to clean up temporary file: {e}", "WARN")
                
                log("Starting to clean up all related files...")
                cleanup_all_task_files(task_id)
                
                try:
                    notifier = get_notifier()
                    if notifier.initialized:
                        await notifier.notify_task_uploaded(task_id, task_info.video_url, up_name)
                except Exception as e:
                    log(f"Failed to send notification: {e}", "WARN")
                
                return True
                
            except Exception as e:
                log(f"Upload failed: {e}", "ERROR")
                traceback.print_exc()
                
                try:
                    notifier = get_notifier()
                    if notifier.initialized:
                        up_name = "Unknown"
                        task_info = self.db_manager.get_task(task_id)
                        if task_info and task_info.customer_id:
                            customer = self.db_manager.get_customer(task_info.customer_id)
                            if customer:
                                up_name = customer.name or "Unknown"
                        await notifier.notify_task_failed(task_id, task_info.video_url if task_info else "", str(e), up_name)
                except Exception as notify_error:
                    log(f"Failed to send failure notification: {notify_error}", "WARN")
                
                return False
        
        async def upload_with_retry(self, file_path, task_id):
            """Upload with retry"""
            for retry in range(MAX_RETRIES):
                success = await self.upload_file(file_path, task_id)
                
                if success:
                    return True
                
                log(f"Upload failed, retry {retry + 1}/{MAX_RETRIES}...", "WARN")
                await asyncio.sleep(RETRY_DELAY)
            
            log(f"Upload failed, retried {MAX_RETRIES} times", "ERROR")
            return False

        async def close(self):
            """Close Bot (python-telegram-bot doesn't need explicit close)"""
            log("Bot ready to close")


async def upload_single_task(task_id: int, ai_caption: str = None, group_id: int = None) -> tuple:
    """
    上传单个任务到 Telegram（流水线模式用）- 使用 Telethon

    Args:
        task_id: 任务ID
        ai_caption: AI生成的文案
        group_id: 分组ID，用于获取目标频道

    Returns:
        (success: bool, error_msg: str or None)
    """
    from database import DatabaseManager
    from telethon import TelegramClient
    from telethon.errors import ChatWriteForbiddenError, UserBannedInChannelError
    import config

    DOWNLOAD_PATH = config.DOWNLOAD_PATH
    PROCESSED_PATH = config.PROCESSED_PATH
    UPLOAD_PATH = config.UPLOAD_PATH

    db = DatabaseManager()
    task = db.get_task(task_id)

    if not task:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] 任务 {task_id} 不存在", flush=True)
        return (False, f"任务 {task_id} 不存在")

    video_file = None
    for path in [
        os.path.join(UPLOAD_PATH, f"{task_id}_watermarked.mp4"),
        os.path.join(PROCESSED_PATH, f"{task_id}_watermarked.mp4"),
        os.path.join(PROCESSED_PATH, f"{task_id}.mp4"),
        os.path.join(DOWNLOAD_PATH, f"{task_id}.mp4"),
    ]:
        if os.path.exists(path):
            video_file = path
            break

    if not video_file:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] 任务 {task_id} 的视频文件不存在", flush=True)
        return (False, f"任务 {task_id} 的视频文件不存在")

    print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 视频文件: {video_file}", flush=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 文件大小: {os.path.getsize(video_file)} bytes", flush=True)

    caption = ai_caption or task.ai_caption or ""
    if not caption and task.video_url:
        caption = f"视频链接: {task.video_url}"

    last_error = None

    try:
        TELEGRAM_BOTS = config.TELEGRAM_BOTS
        if not TELEGRAM_BOTS:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] 没有配置 Telegram Bot", flush=True)
            last_error = "没有配置 Telegram Bot"
            return (False, last_error)

        bot_name = list(TELEGRAM_BOTS.keys())[0]
        bot_config = TELEGRAM_BOTS[bot_name]
        bot_token = bot_config.get("bot_token")
        api_id = bot_config.get("api_id")
        api_hash = bot_config.get("api_hash")
        session_string = bot_config.get("session_string") or os.environ.get('TELEGRAM_SESSION_STRING')
        default_chat_id = bot_config.get("chat_id")

        if not api_id or not api_hash:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] API_ID 或 API_HASH 未配置", flush=True)
            last_error = "API_ID 或 API_HASH 未配置"
            return (False, last_error)

        target_channels = []
        if group_id:
            targets = db.get_group_targets(group_id)
            if targets:
                target_channels = [t.target_channel for t in targets if t.target_channel]
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [INFO] Group mode: found {len(target_channels)} target channels")

        if not target_channels:
            if default_chat_id:
                target_channels = [default_chat_id]
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [INFO] Using default chat_id: {default_chat_id}")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] 没有配置目标频道", flush=True)
                last_error = "没有配置目标频道"
                return (False, last_error)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 开始 Telegram 上传流程", flush=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] API_ID: {api_id}, API_HASH: {api_hash[:10]}...", flush=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] Session String: {'已配置' if session_string else '未配置'}", flush=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 视频文件: {video_file}", flush=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 目标频道数: {len(target_channels)}", flush=True)

        from telethon.sessions import StringSession
        
        session = StringSession(session_string) if session_string else 'bot_session'
        
        async with TelegramClient(session, api_id, api_hash, timeout=30, connection_retries=0) as client:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] Telethon 客户端已创建，开始认证...", flush=True)
            try:
                if session_string:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 使用 Session String 认证 (UserBot)...", flush=True)
                    await asyncio.wait_for(client.start(), timeout=30)
                elif bot_token:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 使用 Bot Token 认证...", flush=True)
                    await asyncio.wait_for(client.start(bot_token=bot_token), timeout=30)
                else:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] 既没有 Session String 也没有 Bot Token", flush=True)
                    last_error = "既没有 Session String 也没有 Bot Token"
                    return (False, last_error)
            except asyncio.TimeoutError:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] 认证超时 (30秒)", flush=True)
                last_error = "认证超时"
                return (False, last_error)
            except Exception as e:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] 认证失败: {e}", flush=True)
                last_error = f"认证失败: {e}"
                return (False, last_error)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 认证成功!", flush=True)

            me = await client.get_me()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] Bot 用户名: @{me.username}", flush=True)

            upload_success_count = 0
            max_retries = 3

            for target_channel in target_channels:
                upload_success = False
                last_error = None
                for attempt in range(1, max_retries + 1):
                    if attempt > 1:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] [INFO] 重试上传到 {target_channel} (第 {attempt}/{max_retries} 次)", flush=True)
                        await asyncio.sleep(5)

                    try:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] [INFO] 上传到频道: {target_channel}")

                        print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 正在解析频道 entity...", flush=True)
                        try:
                            entity = await asyncio.wait_for(
                                client.get_entity(target_channel),
                                timeout=30
                            )
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 频道解析成功! entity.id={entity.id}, title={getattr(entity, 'title', 'N/A')}", flush=True)
                        except asyncio.TimeoutError:
                            last_error = f"频道 {target_channel} 解析超时"
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] {last_error}", flush=True)
                            continue
                        except Exception as e:
                            last_error = f"频道 {target_channel} 解析失败: {e}"
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] {last_error}", flush=True)
                            continue

                        print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 开始发送文件到 Telegram...", flush=True)

                        video_to_send = video_file
                        if video_file.endswith('.mp4') and '_stream.mp4' not in video_file:
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] [INFO] 开始视频流优化...", flush=True)
                            video_to_send = fix_video_for_streaming(video_file)
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] [INFO] 视频流优化完成，使用: {os.path.basename(video_to_send)}", flush=True)

                        await asyncio.wait_for(
                            client.send_file(
                                entity,
                                video_to_send,
                                caption=caption[:1024] if caption else None,
                                supports_streaming=True
                            ),
                            timeout=300
                        )
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] [INFO] ✅ 成功上传到: {target_channel}")
                        upload_success = True
                        break
                    except asyncio.TimeoutError:
                        last_error = f"上传到 {target_channel} 超时"
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] {last_error}", flush=True)
                        continue
                    except (ChatWriteForbiddenError, UserBannedInChannelError) as e:
                        last_error = f"上传到 {target_channel} 失败: {e}"
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] {last_error}", flush=True)
                        break
                    except Exception as e:
                        last_error = f"上传到 {target_channel} 异常: {e}"
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] {last_error}", flush=True)
                        continue

                if upload_success:
                    upload_success_count += 1

            if upload_success_count == 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] 所有频道上传都失败: {last_error}", flush=True)
                return (False, last_error or "所有频道上传都失败")

            print(f"[{datetime.now().strftime('%H:%M:%S')}] [INFO] 上传成功: task_id={task_id}, 成功 {upload_success_count}/{len(target_channels)} 个频道")

        for suffix in ["_watermarked", "_stream", ""]:
            for dir_path in [UPLOAD_PATH, PROCESSED_PATH, DOWNLOAD_PATH]:
                file_path = os.path.join(dir_path, f"{task_id}{suffix}.mp4")
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass

        return (True, None)

    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] 上传异常: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return (False, f"上传异常: {e}")


async def test_telegram_upload(group_id: int) -> tuple:
    """
    测试 Telegram Bot 能否向目标频道发送消息
    Returns: (success: bool, message: str)
    """
    from database import DatabaseManager
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    import config

    db = DatabaseManager()
    targets = db.get_group_targets(group_id)

    if not targets:
        return False, f"分组 {group_id} 没有配置目标频道"

    target_channel = targets[0].target_channel

    print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 测试 Telegram 上传: 分组={group_id}, 频道={target_channel}", flush=True)

    TELEGRAM_BOTS = config.TELEGRAM_BOTS
    if not TELEGRAM_BOTS:
        return False, "没有配置 Telegram Bot"

    bot_name = list(TELEGRAM_BOTS.keys())[0]
    bot_config = TELEGRAM_BOTS[bot_name]
    bot_token = bot_config.get("bot_token")
    api_id = bot_config.get("api_id")
    api_hash = bot_config.get("api_hash")
    session_string = bot_config.get("session_string") or os.environ.get('TELEGRAM_SESSION_STRING')

    if not api_id or not api_hash:
        return False, "API_ID 或 API_HASH 未配置"

    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] Session String: {'已配置' if session_string else '未配置'}", flush=True)
        
        session = StringSession(session_string) if session_string else 'test_session'
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 创建 Telethon 客户端...", flush=True)
        async with TelegramClient(session, api_id, api_hash, timeout=30, connection_retries=0) as client:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 正在认证...", flush=True)
            if session_string:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 使用 Session String 认证 (UserBot)...", flush=True)
                await asyncio.wait_for(client.start(), timeout=30)
            elif bot_token:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 使用 Bot Token 认证...", flush=True)
                await asyncio.wait_for(client.start(bot_token=bot_token), timeout=30)
            else:
                return False, "既没有 Session String 也没有 Bot Token"
            
            me = await client.get_me()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 用户名: @{me.username}", flush=True)

            print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 正在获取频道 entity: {target_channel}", flush=True)
            entity = await asyncio.wait_for(
                client.get_entity(target_channel),
                timeout=30
            )
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 频道解析成功! entity.id={entity.id}", flush=True)

            print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 发送测试消息...", flush=True)
            test_message = f"✅ 测试消息 from @{me.username}\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            await asyncio.wait_for(
                client.send_message(entity, test_message),
                timeout=30
            )
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [DEBUG] 消息发送成功!", flush=True)
            return True, f"消息发送成功! 频道: {target_channel}, 用户: @{me.username}"

    except asyncio.TimeoutError as e:
        error_msg = f"超时错误"
        if "get_entity" in str(e):
            error_msg = f"获取频道 entity 超时，请检查频道是否存在且有权限访问"
        elif "send_message" in str(e):
            error_msg = f"发送消息超时"
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] {error_msg}", flush=True)
        return False, error_msg

    except Exception as e:
        error_msg = f"错误: {e}"
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] 测试失败: {e}", flush=True)
        return False, error_msg


async def process_upload_task_async(bot_config):
    """Async process upload tasks"""
    try:
        db_manager = DatabaseManager()
        log_separator("Telegram Bot Uploader Started (Pure TGBot Version)")
        log("Using python-telegram-bot library - no userbot needed")

        uploader = TelegramBotUploader()

        if not uploader.init_bot(bot_config):
            log("Bot initialization failed, exiting", "ERROR")
            return

        try:
            consecutive_empty = 0
            while True:
                all_tasks = db_manager.get_tasks(status="ai_captioned", limit=100)
                if not all_tasks:
                    all_tasks = db_manager.get_tasks(status="watermarked", limit=100)

                tasks = all_tasks

                if not tasks:
                    consecutive_empty += 1
                    if consecutive_empty >= 5:
                        log("No tasks to upload, taking a break...")
                        consecutive_empty = 0
                        await asyncio.sleep(30)
                    continue

                consecutive_empty = 0
                for task in tasks:
                    success = await uploader.upload_with_retry(task.file_path, task.id)
                    if not success:
                        log(f"Failed to upload task {task.id}", "ERROR")

                log("Taking a 5 second break before continuing...")
                await asyncio.sleep(5)

        except KeyboardInterrupt:
            log("\nReceived stop signal, exiting...", "WARN")
        finally:
            await uploader.close()
            log("Uploader stopped")
    except Exception as e:
        log(f"Uploader initialization failed: {e}", "ERROR")
        traceback.print_exc()


def process_upload_task(bot_config):
    """Sync entry - run async task"""
    asyncio.run(process_upload_task_async(bot_config))


    def log_separator(title=None):
        """Output separator line"""
        if title:
            print(f"\n{'='*60}")
            print(f"  {title}")
            print(f"{'='*60}")
        else:
            print(f"\n{'='*60}")


    print("="*60)
    print("🚀 Starting Uploader")
    print("="*60)
    
    if TELEGRAM_BOTS and len(TELEGRAM_BOTS) > 0:
        first_bot_name = list(TELEGRAM_BOTS.keys())[0]
        bot_config = TELEGRAM_BOTS[first_bot_name]
        print(f"✅ Config loaded successfully: {first_bot_name}")
        print(f"   Chat ID: {bot_config.get('chat_id')}")
        process_upload_task(bot_config)
    else:
        log("No Telegram Bot configured!", "ERROR")
        time.sleep(10)


if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(f"❌ Uploader main process exception: {e}")
            traceback.print_exc()
            print("⏳ Restarting in 10 seconds...")
            time.sleep(10)
