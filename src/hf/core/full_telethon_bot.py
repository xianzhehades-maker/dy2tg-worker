"""Complete Telegram Bot - All Features + Upload with Telethon"""
import os
import sys
import time
import subprocess
import asyncio
import threading
import traceback
import json
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

print("="*60, flush=True)
print("🚀 COMPLETE TELEGRAM BOT + UPLOAD", flush=True)
print("="*60, flush=True)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    print("[DEBUG] Importing config...", flush=True)
    import config
    print(f"[DEBUG] Config imported!", flush=True)
    
    print("[DEBUG] Importing database...", flush=True)
    from database import DatabaseManager
    from hf.database.models import MonitorGroup, GroupMonitor, GroupTarget
    from hf.utils.notifier import get_notifier
    print("[DEBUG] Database imported", flush=True)
    
    print("[DEBUG] Importing telethon...", flush=True)
    from telethon import TelegramClient, events
    from telethon.types import Message
    print("[DEBUG] Telethon imported", flush=True)
except Exception as e:
    print(f"[DEBUG] Import error: {e}", flush=True)
    traceback.print_exc()
    time.sleep(10)
    sys.exit(1)

TELEGRAM_BOTS = config.TELEGRAM_BOTS
DOWNLOAD_PATH = config.DOWNLOAD_PATH
PROCESSED_PATH = config.PROCESSED_PATH
UPLOAD_PATH = config.UPLOAD_PATH
MAX_RETRIES = config.MAX_RETRIES
RETRY_DELAY = config.RETRY_DELAY

def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}", flush=True)

def log_separator(title=None):
    if title:
        print(f"\n{'='*60}", flush=True)
        print(f"  {title}", flush=True)
        print(f"{'='*60}\n", flush=True)
    else:
        print(f"\n{'='*60}\n", flush=True)

def get_ffmpeg_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    ffmpeg_exe = os.path.join(project_dir, 'ffmpeg.exe')
    if os.path.exists(ffmpeg_exe):
        return ffmpeg_exe
    return 'ffmpeg'

def fix_video_for_streaming(input_path):
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
    log(f"📹 视频流优化开始: {os.path.basename(input_path)}")
    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=300)
        log(f"✅ 视频流优化完成: {os.path.basename(output_path)}")
        return output_path
    except subprocess.TimeoutExpired:
        log(f"⚠️ 视频流优化超时（5分钟），使用原文件: {os.path.basename(input_path)}", "WARN")
        return input_path
    except Exception as e:
        log(f"⚠️ 视频流优化失败: {e}，使用原文件: {os.path.basename(input_path)}", "WARN")
        return input_path

def cleanup_all_task_files(task_id):
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

ALLOWED_CHAT_ID = int(os.environ.get('ALLOWED_CHAT_ID', 0)) if os.environ.get('ALLOWED_CHAT_ID') else None
print(f"[DEBUG] ALLOWED_CHAT_ID: {ALLOWED_CHAT_ID}", flush=True)

def allowed_chat_only(func):
    """只允许在特定群组中执行命令的装饰器"""
    async def wrapper(self, event):
        try:
            if ALLOWED_CHAT_ID:
                chat_id = event.chat_id if event.chat_id else event.message.peer_id.channel_id if hasattr(event.message, 'peer_id') and hasattr(event.message.peer_id, 'channel_id') else None
                if chat_id != ALLOWED_CHAT_ID:
                    return
            return await func(self, event)
        except Exception as e:
            return
    return wrapper

class CompleteTelegramBot:
    def __init__(self):
        self.client = None
        self.db_manager = DatabaseManager()
        self.workflow_process = None
        self.running = True
    
    def parse_args(self, text):
        if not text:
            return []
        parts = text.split()
        return parts if parts else []
    
    def parse_monitor_list(self, text):
        monitors = []
        text = text.replace("：", ":").replace("，", ",").replace(" ", "")
        parts = text.split(",")
        for part in parts:
            if ":" in part:
                name, url = part.split(":", 1)
                if name and url:
                    monitors.append((name.strip(), url.strip()))
        return monitors
    
    @allowed_chat_only
    @allowed_chat_only
    async def handle_help(self, event):
        print("[DEBUG] /help called", flush=True)
        help_text = """
Group Management System - Help

Group Management:
  /add_group group_name - Add new group
  /del_group group_id - Delete group
  /rename_group group_id new_name - Rename group

Monitor Management:
  /addup group_id up1:url1,up2:url2 - Add monitors to group
  /del_monitor group_id up1:url1,up2:url2 - Delete by URL
  /del_monitor group_id index - Delete by index (see /mon_list)
  /mon_list - List all monitors with index

Target Management:
  /add_target group_id @channel - Add target to group
  /del_target group_id @channel - Delete target from group

Other Settings:
  /set_promotion group_id text - Set promotion text
  /set_caption_style group_id style - Set caption style
    - 口播/default: AI优化长文案
    - humor/short/乐子: 短文案
    - none/舞蹈: 无AI文案

System Commands:
  /status - View all groups status
  /queue - View current queue
  /list_errors - List all failed tasks
  /retry <task_id|all> - Retry failed task(s)
  /retry_failed - Process failed tasks immediately
  /skip <task_id> - Skip a task
  /sync - Force sync config to cloud
  /sync_ids - Sync all target channel chat_ids
  /clear_cache - Clear all cache
  /time [interval] - View or set check interval (seconds)

Workflow Control:
  /run_now - Trigger next cycle immediately (skip wait interval)
  /workflow_status - View workflow status
  /test_target <group_id> - Test if bot can send to target channel

Help:
  /help - Show this help message
        """
        await event.reply(help_text.strip())

    @allowed_chat_only
    async def handle_mon_list(self, event):
        print("[DEBUG] /mon_list called", flush=True)
        try:
            groups = self.db_manager.get_monitor_groups()
            if not groups:
                await event.reply("没有监控分组")
                return

            result = "📋 监控对象列表\n"
            result += "=" * 50 + "\n\n"

            for group in groups:
                result += f"📁 分组 {group.id}: {group.name}\n"
                if group.ai_caption_style:
                    result += f"   文案风格: {group.ai_caption_style}\n"

                monitors = self.db_manager.get_group_monitors(group.id)
                if monitors:
                    for i, mon in enumerate(monitors, 1):
                        result += f"   [{i}] {mon.up_name}\n"
                        result += f"       {mon.up_url}\n"
                    result += f"   共 {len(monitors)} 个监控对象\n"
                else:
                    result += "   (无监控对象)\n"
                result += "\n"

                if len(result) > 3500:
                    result += "\n⚠️ 内容过长，已截断...\n"
                    break

            result += "\n💡 使用 /del_monitor <分组ID> <编号> 删除监控对象"

            if len(result) > 4096:
                result = result[:4050] + "\n... (内容过长已截断)"

            await event.reply(result)
        except Exception as e:
            print(f"[ERROR] handle_mon_list: {e}", flush=True)
            import traceback
            traceback.print_exc()
            await event.reply(f"Error: {str(e)}")

    @allowed_chat_only
    async def handle_list_errors(self, event):
        print("[DEBUG] /list_errors called", flush=True)
        try:
            error_statuses = ["error", "download_failed", "upload_failed", "api_error"]
            error_tasks = []

            for status in error_statuses:
                tasks = self.db_manager.get_tasks(status=status, limit=100)
                for task in tasks:
                    if task:
                        error_tasks.append(task)

            if not error_tasks:
                await event.reply("✅ 没有失败任务")
                return

            result = f"❌ 失败任务列表 (共 {len(error_tasks)} 个)\n"
            result += "=" * 40 + "\n\n"

            for i, task in enumerate(error_tasks[:20], 1):
                result += f"{i}. Task ID: {task.id}\n"
                result += f"   Status: {task.status}\n"
                result += f"   Error: {task.error_msg[:100] if task.error_msg else 'N/A'}\n"
                if task.video_id:
                    result += f"   Video: {task.video_id}\n"
                result += "\n"

            if len(error_tasks) > 20:
                result += f"... 还有 {len(error_tasks) - 20} 个失败任务"

            await event.reply(result)
        except Exception as e:
            print(f"[ERROR] handle_list_errors: {e}", flush=True)
            import traceback
            traceback.print_exc()
            await event.reply(f"Error: {str(e)}")

    @allowed_chat_only
    async def handle_retry(self, event):
        print("[DEBUG] /retry called", flush=True)
        args = self.parse_args(event.raw_text)
        if len(args) < 2:
            await event.reply("Usage: /retry <task_id>\n   or /retry all - Retry all failed tasks")
            return

        arg = args[1]

        if arg.lower() == "all":
            error_statuses = ["error", "download_failed", "upload_failed", "api_error"]
            reset_count = 0
            for status in error_statuses:
                tasks = self.db_manager.get_tasks(status=status, limit=1000)
                for task in tasks:
                    if task:
                        self.db_manager.update_task(task.id, status="pending", error_msg="")
                        reset_count += 1
            await event.reply(f"✅ 已重置 {reset_count} 个失败任务为 pending 状态")
            return

        try:
            task_id = int(arg)
            task = self.db_manager.get_task(task_id)
            if not task:
                await event.reply(f"Error: Task {task_id} not found")
                return

            self.db_manager.update_task(task_id, status="pending", error_msg="")
            await event.reply(f"✅ Task {task_id} 已重置为 pending 状态")
        except ValueError:
            await event.reply("Error: Invalid task ID")

    @allowed_chat_only
    async def handle_retry_failed(self, event):
        print("[DEBUG] /retry_failed called", flush=True)
        try:
            error_statuses = ["error", "download_failed", "upload_failed", "api_error"]
            failed_tasks = []

            for status in error_statuses:
                tasks = self.db_manager.get_tasks(status=status, limit=1000)
                failed_tasks.extend([t for t in tasks if t])

            if not failed_tasks:
                await event.reply("✅ 没有失败任务")
                return

            result = f"🔄 开始处理 {len(failed_tasks)} 个失败任务...\n\n"
            success_count = 0
            fail_count = 0

            for task in failed_tasks[:20]:
                try:
                    if task.status == "upload_failed" and task.file_path:
                        from core.uploader import upload_single_task
                        upload_result = await upload_single_task(task.id, task.ai_caption, task.group_id)
                        if upload_result:
                            self.db_manager.update_task(task.id, status="uploaded", error_msg="")
                            success_count += 1
                        else:
                            fail_count += 1
                    elif task.status == "download_failed" and task.video_url:
                        self.db_manager.update_task(task.id, status="pending", error_msg="")
                        success_count += 1
                    else:
                        self.db_manager.update_task(task.id, status="pending", error_msg="")
                        success_count += 1
                except Exception as e:
                    print(f"[ERROR] 处理任务 {task.id} 失败: {e}", flush=True)
                    fail_count += 1

            result += f"✅ 处理完成: {success_count} 个\n"
            if fail_count > 0:
                result += f"❌ 失败: {fail_count} 个\n"
            result += "\n💡 剩余任务将在下一轮流水线自动处理"

            await event.reply(result)
        except Exception as e:
            print(f"[ERROR] handle_retry_failed: {e}", flush=True)
            import traceback
            traceback.print_exc()
            await event.reply(f"Error: {str(e)}")

    @allowed_chat_only
    async def handle_skip(self, event):
        print("[DEBUG] /skip called", flush=True)
        args = self.parse_args(event.raw_text)
        if len(args) < 2:
            await event.reply("Usage: /skip <task_id>")
            return

        try:
            task_id = int(args[1])
            task = self.db_manager.get_task(task_id)
            if not task:
                await event.reply(f"Error: Task {task_id} not found")
                return

            self.db_manager.update_task(task_id, status="skipped", error_msg="手动跳过")
            await event.reply(f"✅ Task {task_id} 已标记为 skipped")
        except ValueError:
            await event.reply("Error: Invalid task ID")

    @allowed_chat_only
    async def handle_set_caption_style(self, event):
        print("[DEBUG] /set_caption_style called", flush=True)
        try:
            args = self.parse_args(event.raw_text)
            if len(args) < 3:
                await event.reply(
                    "Error: Usage: /set_caption_style group_id style\n\n"
                    "Styles:\n"
                    "  - default/口播: AI optimized caption (for talking videos)\n"
                    "  - humor/short/乐子: Short fun caption (for comedy videos)\n"
                    "  - none/舞蹈/null: No AI caption (for dance videos)"
                )
                return

            group_id = int(args[1])
            style = " ".join(args[2:]).lower()

            group = self.db_manager.get_monitor_group(group_id)
            if not group:
                await event.reply("Error: Group not found")
                return

            if style in ["default", "口播"]:
                ai_caption_style = "default"
                style_name = "口播模式 (AI优化文案)"
            elif style in ["humor", "short", "乐子"]:
                ai_caption_style = "humor"
                style_name = "乐子模式 (短文案)"
            elif style in ["none", "舞蹈", "null", ""]:
                ai_caption_style = None
                style_name = "舞蹈模式 (无AI文案)"
            else:
                await event.reply(f"Error: Unknown style '{style}'")
                return

            self.db_manager.update_monitor_group(group_id, ai_caption_style=ai_caption_style)
            await event.reply(
                f"OK! Group {group_id} caption style updated!\n"
                f"Group: {group.name}\n"
                f"Style: {style_name}"
            )
        except ValueError:
            await event.reply("Error: Group ID must be a number")
        except Exception as e:
            print(f"[ERROR] handle_set_caption_style: {e}", flush=True)
            import traceback
            traceback.print_exc()
            await event.reply(f"Error: {str(e)}")

    @allowed_chat_only
    async def handle_test_target(self, event):
        print("[DEBUG] /test_target called", flush=True)
        args = self.parse_args(event.raw_text)
        
        if len(args) < 2:
            await event.reply("Usage: /test_target group_id\n测试机器人是否能向目标频道发送消息")
            return
        
        try:
            group_id = int(args[1])
        except ValueError:
            await event.reply("Error: group_id must be a number")
            return
        
        from core.uploader import test_telegram_upload
        await event.reply(f"Testing upload to group {group_id}...")
        
        success, message = await test_telegram_upload(group_id)
        
        if success:
            await event.reply(f"✅ {message}")
        else:
            await event.reply(f"❌ {message}")
    
    @allowed_chat_only
    async def handle_add_group(self, event):
        args = self.parse_args(event.raw_text)
        if len(args) < 2:
            await event.reply("Error: Usage: /add_group group_name")
            return
        
        group_name = " ".join(args[1:])
        group = MonitorGroup(name=group_name)
        group_id = self.db_manager.add_monitor_group(group)
        
        if group_id:
            await event.reply(f"OK! Group created!\nID: {group_id}\nName: {group_name}")
        else:
            await event.reply("Error: Group creation failed, maybe name exists")
    
    @allowed_chat_only
    async def handle_del_group(self, event):
        args = self.parse_args(event.raw_text)
        if len(args) < 2:
            await event.reply("Error: Usage: /del_group group_id")
            return
        
        try:
            group_id = int(args[1])
            group = self.db_manager.get_monitor_group(group_id)
            if not group:
                await event.reply("Error: Group not found")
                return
            
            self.db_manager.delete_monitor_group(group_id)
            await event.reply(f"OK! Group deleted!\nID: {group_id}\nName: {group.name}")
        except ValueError:
            await event.reply("Error: Group ID must be a number")
    
    @allowed_chat_only
    async def handle_rename_group(self, event):
        args = self.parse_args(event.raw_text)
        if len(args) < 3:
            await event.reply("Error: Usage: /rename_group group_id new_name")
            return
        
        try:
            group_id = int(args[1])
            new_name = " ".join(args[2:])
            
            group = self.db_manager.get_monitor_group(group_id)
            if not group:
                await event.reply("Error: Group not found")
                return
            
            self.db_manager.update_monitor_group(group_id, name=new_name)
            await event.reply(f"OK! Group renamed!\nID: {group_id}\nOld: {group.name}\nNew: {new_name}")
        except ValueError:
            await event.reply("Error: Group ID must be a number")
    
    @allowed_chat_only
    async def handle_add_monitor(self, event):
        args = self.parse_args(event.raw_text)
        if len(args) < 3:
            await event.reply("Error: Usage: /addup group_id up1:url1,up2:url2")
            return
        
        try:
            group_id = int(args[1])
            monitor_text = " ".join(args[2:])
            
            group = self.db_manager.get_monitor_group(group_id)
            if not group:
                await event.reply("Error: Group not found")
                return
            
            monitors = self.parse_monitor_list(monitor_text)
            if not monitors:
                await event.reply("Error: Cannot parse monitor list, check format")
                return
            
            added = []
            skipped = []
            
            for name, url in monitors:
                monitor = GroupMonitor(group_id=group_id, up_name=name, up_url=url)
                monitor_id = self.db_manager.add_group_monitor(monitor)
                if monitor_id:
                    added.append(f"{name}:{url}")
                else:
                    skipped.append(f"{name}:{url}")
            
            result = f"OK! Added monitors to group '{group.name}'!\n\n"
            if added:
                result += f"Added {len(added)}:\n" + "\n".join(added) + "\n\n"
            if skipped:
                result += f"Skipped {len(skipped)} (already exists):\n" + "\n".join(skipped)
            
            await event.reply(result)
        except ValueError:
            await event.reply("Error: Group ID must be a number")
    
    @allowed_chat_only
    async def handle_del_monitor(self, event):
        args = self.parse_args(event.raw_text)
        if len(args) < 3:
            await event.reply("Error: Usage: /del_monitor group_id up1:url1,up2:url2\n   or /del_monitor group_id index")
            return

        try:
            group_id = int(args[1])
            monitor_text = " ".join(args[2:])

            group = self.db_manager.get_monitor_group(group_id)
            if not group:
                await event.reply("Error: Group not found")
                return

            # 如果是纯数字，按编号删除
            if monitor_text.isdigit():
                index = int(monitor_text)
                monitors = self.db_manager.get_group_monitors(group_id)
                if index < 1 or index > len(monitors):
                    await event.reply(f"Error: Index {index} out of range (1-{len(monitors)})")
                    return

                mon = monitors[index - 1]
                self.db_manager.delete_group_monitor_by_url(group_id, mon.up_url)
                await event.reply(f"OK! Deleted [{index}] {mon.up_name}\n   {mon.up_url}")
                return

            # 否则按 URL 删除
            monitors = self.parse_monitor_list(monitor_text)
            if not monitors:
                await event.reply("Error: Cannot parse monitor list, check format")
                return

            deleted = []

            for name, url in monitors:
                self.db_manager.delete_group_monitor_by_url(group_id, url)
                deleted.append(f"{name}:{url}")

            result = f"OK! Deleted monitors from group '{group.name}'!\n\n"
            result += f"Deleted {len(deleted)}:\n" + "\n".join(deleted)

            await event.reply(result)
        except ValueError:
            await event.reply("Error: Group ID must be a number")
    
    @allowed_chat_only
    async def handle_add_target(self, event):
        args = self.parse_args(event.raw_text)
        if len(args) < 3:
            await event.reply("Error: Usage: /add_target group_id @channel")
            return
        
        try:
            group_id = int(args[1])
            target_channel = args[2]
            
            group = self.db_manager.get_monitor_group(group_id)
            if not group:
                await event.reply("Error: Group not found")
                return
            
            target = GroupTarget(group_id=group_id, target_channel=target_channel)
            target_id = self.db_manager.add_group_target(target)
            
            if target_id:
                await event.reply(f"OK! Target added!\nGroup: {group.name}\nTarget: {target_channel}")
            else:
                await event.reply("Error: Target add failed, maybe already exists")
        except ValueError:
            await event.reply("Error: Group ID must be a number")
    
    @allowed_chat_only
    async def handle_del_target(self, event):
        args = self.parse_args(event.raw_text)
        if len(args) < 3:
            await event.reply("Error: Usage: /del_target group_id @channel")
            return
        
        try:
            group_id = int(args[1])
            target_channel = args[2]
            
            group = self.db_manager.get_monitor_group(group_id)
            if not group:
                await event.reply("Error: Group not found")
                return
            
            self.db_manager.delete_group_target_by_channel(group_id, target_channel)
            await event.reply(f"OK! Target deleted!\nGroup: {group.name}\nTarget: {target_channel}")
        except ValueError:
            await event.reply("Error: Group ID must be a number")
    
    @allowed_chat_only
    async def handle_set_promotion(self, event):
        args = self.parse_args(event.raw_text)
        if len(args) < 3:
            await event.reply("Error: Usage: /set_promotion group_id text")
            return
        
        try:
            group_id = int(args[1])
            promotion_text = " ".join(args[2:])
            
            group = self.db_manager.get_monitor_group(group_id)
            if not group:
                await event.reply("Error: Group not found")
                return
            
            self.db_manager.update_monitor_group(group_id, promotion_text=promotion_text)
            await event.reply(f"OK! Promotion text set!\nGroup: {group.name}\nText: {promotion_text}")
        except ValueError:
            await event.reply("Error: Group ID must be a number")
    
    @allowed_chat_only
    async def handle_status(self, event):
        groups = self.db_manager.get_monitor_groups()
        
        if not groups:
            await event.reply("No groups configured")
            return
        
        result = "System Status\n" + "="*40 + "\n\n"
        
        for group in groups:
            monitors = self.db_manager.get_group_monitors(group.id)
            targets = self.db_manager.get_group_targets(group.id)
            
            result += f"Group {group.id}: {group.name}\n"
            if group.ai_caption_style:
                result += f"  Caption Style: {group.ai_caption_style}\n"
            result += f"  Monitors: {len(monitors)}\n"
            result += f"  Targets: {len(targets)}\n"
            
            if monitors:
                result += "  Monitor list:\n"
                for m in monitors[:5]:
                    result += f"    - {m.up_name}: {m.up_url[:30]}...\n"
                if len(monitors) > 5:
                    result += f"    ... and {len(monitors)-5} more\n"
            
            if targets:
                result += "  Target list:\n"
                for t in targets:
                    result += f"    - {t.target_channel}"
                    if t.chat_id:
                        result += f" (ID: {t.chat_id})"
                    result += "\n"
            
            if group.promotion_text:
                result += f"  Promotion: {group.promotion_text[:50]}...\n"
            
            result += "\n"
        
        interval = self.db_manager.get_system_config("check_interval", "3600")
        result += f"Check interval: {interval} seconds\n"
        
        await event.reply(result)
    
    @allowed_chat_only
    async def handle_queue(self, event):
        stats = self.db_manager.get_task_queue_stats()
        
        if not stats:
            await event.reply("Queue is empty")
            return
        
        result = "Task Queue Stats\n" + "="*40 + "\n\n"
        
        total = sum(stats.values())
        pending_count = stats.get("pending", 0) + stats.get("verified", 0) + stats.get("downloaded", 0) + stats.get("watermarked", 0) + stats.get("ai_captioned", 0)
        
        result += f"Pending: {pending_count}\n"
        result += f"Completed: {stats.get('uploaded', 0)}\n"
        result += f"Errors: {stats.get('error', 0)}\n"
        result += f"Total: {total}\n\n"
        
        result += "Detailed status:\n"
        for status, count in sorted(stats.items()):
            result += f"  {status}: {count}\n"
        
        await event.reply(result)
    
    @allowed_chat_only
    async def handle_sync(self, event):
        import json
        try:
            HF_TOKEN = os.environ.get("HF_TOKEN", "")
            REPO_ID = os.environ.get("HF_DATASET_REPO_ID", "dwizwang1/dy2tgdb")

            if not HF_TOKEN:
                await event.reply("⚠️ HF_TOKEN not configured")
                return

            await event.reply(f"📤 Syncing all data to HuggingFace...")

            from huggingface_hub import HfApi, CommitOperationAdd
            import tempfile
            import pandas as pd

            temp_dir = tempfile.mkdtemp()

            try:
                tables = {
                    'customers': lambda: pd.DataFrame([{
                        'id': c.id, 'name': c.name, 'user_id': c.user_id,
                        'homepage_url': c.homepage_url, 'created_at': c.created_at, 'updated_at': c.updated_at
                    } for c in self.db_manager.get_customers()]),

                    'tasks': lambda: pd.DataFrame([{
                        'id': t.id, 'customer_id': t.customer_id, 'video_id': t.video_id,
                        'video_url': t.video_url, 'video_publish_time': t.video_publish_time,
                        'download_url': t.download_url, 'status': t.status,
                        'download_time': t.download_time, 'watermark_time': t.watermark_time,
                        'ai_caption_time': t.ai_caption_time, 'upload_time': t.upload_time,
                        'upload_bot_id': t.upload_bot_id, 'upload_channel_id': t.upload_channel_id,
                        'file_path': t.file_path, 'video_desc': t.video_desc, 'ai_caption': t.ai_caption,
                        'error_msg': t.error_msg, 'created_at': t.created_at, 'updated_at': t.updated_at,
                        'group_id': t.group_id
                    } for t in self.db_manager.get_tasks()]),

                    'discovered_videos': lambda: pd.DataFrame([{
                        'id': v.id, 'customer_id': v.customer_id, 'video_id': v.video_id,
                        'video_url': v.video_url, 'video_publish_time': v.video_publish_time,
                        'discovered_at': v.discovered_at, 'is_qualified': v.is_qualified,
                        'created_at': v.created_at, 'updated_at': v.updated_at
                    } for v in self.db_manager.get_discovered_videos()]),

                    'monitor_groups': lambda: pd.DataFrame([{
                        'id': g.id, 'name': g.name, 'promotion_text': g.promotion_text,
                        'ai_caption_style': g.ai_caption_style, 'created_at': g.created_at, 'updated_at': g.updated_at
                    } for g in self.db_manager.get_monitor_groups()]),

                    'group_monitors': lambda: pd.DataFrame([{
                        'id': m.id, 'group_id': m.group_id, 'up_name': m.up_name,
                        'up_url': m.up_url, 'created_at': m.created_at
                    } for m in self.db_manager.get_group_monitors()]),

                    'group_targets': lambda: pd.DataFrame([{
                        'id': t.id, 'group_id': t.group_id, 'target_channel': t.target_channel,
                        'chat_id': t.chat_id, 'created_at': t.created_at
                    } for t in self.db_manager.get_group_targets()]),

                    'task_plans': lambda: pd.DataFrame([{
                        'id': p.id, 'name': p.name, 'customer_ids': p.customer_ids,
                        'bot_config': p.bot_config, 'execution_type': p.execution_type,
                        'target_count': p.target_count, 'interval_minutes': p.interval_minutes,
                        'upload_template_id': p.upload_template_id, 'enabled': p.enabled,
                        'created_at': p.created_at, 'updated_at': p.updated_at,
                        'workflow_steps': p.workflow_steps
                    } for p in self.db_manager.get_task_plans()]),

                    'executions': lambda: pd.DataFrame([{
                        'id': e.id, 'plan_id': e.plan_id, 'mode': e.mode, 'status': e.status,
                        'current_step': e.current_step, 'start_time': e.start_time, 'end_time': e.end_time,
                        'cycle_count': e.cycle_count, 'cycle_duration': e.cycle_duration,
                        'tasks_created': e.tasks_created, 'tasks_completed': e.tasks_completed,
                        'tasks_failed': e.tasks_failed, 'error_msg': e.error_msg,
                        'created_at': e.created_at, 'updated_at': e.updated_at
                    } for e in self.db_manager.get_executions()]),

                    'upload_templates': lambda: pd.DataFrame([{
                        'id': t.id, 'name': t.name, 'content': t.content,
                        'created_at': t.created_at, 'updated_at': t.updated_at
                    } for t in self.db_manager.get_upload_templates()]),
                }

                file_count = 0
                for table_name, fetch_func in tables.items():
                    try:
                        df = fetch_func()
                        if df is not None and len(df) > 0:
                            csv_path = os.path.join(temp_dir, f"{table_name}.csv")
                            df.to_csv(csv_path, index=False, encoding='utf-8')
                            file_count += 1
                    except Exception as e:
                        pass

                operations = []
                for csv_file in os.listdir(temp_dir):
                    if csv_file.endswith('.csv'):
                        csv_path = os.path.join(temp_dir, csv_file)
                        operations.append(CommitOperationAdd(path_in_repo=csv_file, path_or_fileobj=csv_path))

                if operations:
                    api = HfApi(token=HF_TOKEN)
                    api.create_commit(
                        repo_id=REPO_ID,
                        repo_type="dataset",
                        operations=operations,
                        commit_message=f"Force sync from /sync command - {len(operations)} files"
                    )
                    await event.reply(f"✅ Synced to HuggingFace!\n{len(operations)} CSV files uploaded")
                else:
                    await event.reply("⚠️ No data to sync")

            finally:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)

        except Exception as e:
            await event.reply(f"Error: Config sync failed: {e}")
    
    @allowed_chat_only
    async def handle_sync_ids(self, event):
        targets = self.db_manager.get_group_targets()
        updated = 0
        
        for target in targets:
            if not target.chat_id and target.target_channel.startswith("@"):
                try:
                    chat = await self.client.get_entity(target.target_channel)
                    if chat:
                        self.db_manager.update_group_target_chat_id(target.id, chat.id)
                        updated += 1
                except Exception as e:
                    pass
        
        await event.reply(f"OK! chat_id synced!\nUpdated: {updated}\nTotal: {len(targets)}")
    
    @allowed_chat_only
    async def handle_clear_cache(self, event):
        try:
            import shutil
            
            data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
            dirs_to_clear = ["downloads", "processed", "upload"]
            
            cleared = 0
            for dir_name in dirs_to_clear:
                dir_path = os.path.join(data_dir, dir_name)
                if os.path.exists(dir_path):
                    for filename in os.listdir(dir_path):
                        file_path = os.path.join(dir_path, filename)
                        try:
                            if os.path.isfile(file_path):
                                os.remove(file_path)
                                cleared += 1
                            elif os.path.isdir(file_path):
                                shutil.rmtree(file_path)
                        except:
                            pass
            
            await event.reply(f"OK! Cache cleared!\nFiles cleared: {cleared}")
        except Exception as e:
            await event.reply(f"Error: Cache clear failed: {e}")
    
    @allowed_chat_only
    async def handle_time(self, event):
        args = self.parse_args(event.raw_text)
        if len(args) < 2:
            current = self.db_manager.get_system_config("check_interval", "3600")
            await event.reply(f"Current check interval: {current} seconds\n\nUsage: /time interval_seconds")
            return
        
        try:
            interval = int(args[1])
            if interval < 60:
                await event.reply("Error: Interval must be at least 60 seconds")
                return
            
            self.db_manager.set_system_config("check_interval", str(interval))
            await event.reply(f"OK! Check interval set to {interval} seconds")
        except ValueError:
            await event.reply("Error: Interval must be a number")

    @allowed_chat_only
    async def handle_run_now(self, event):
        print(f"[DEBUG] handle_run_now called, chat_id={event.chat_id}", flush=True)
        try:
            data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
            os.makedirs(data_dir, exist_ok=True)
            flag_file = os.path.join(data_dir, ".run_now_flag")
            with open(flag_file, 'w') as f:
                f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            print(f"[DEBUG] run_now flag created at: {flag_file}", flush=True)
            await event.reply("✅ 已触发立即执行！下一轮流水线将跳过等待间隔立即开始。")
        except Exception as e:
            print(f"[DEBUG] handle_run_now error: {e}", flush=True)
            import traceback
            traceback.print_exc()
            await event.reply(f"❌ Error: {e}")

    @allowed_chat_only
    async def handle_workflow_status(self, event):
        await event.reply("✅ Workflow is managed by Supervisor and should be running!\nCheck logs for details.")
    
    @allowed_chat_only
    async def handle_all_messages(self, event):
        print(f"\n[DEBUG] Received message: {event.raw_text}", flush=True)
    
    async def upload_file(self, file_path, task_id):
        if not self.client:
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
            chat_id = None
            if task_info.group_id:
                targets = self.db_manager.get_group_targets(task_info.group_id)
                if targets:
                    target_channels = [t.target_channel for t in targets if t.target_channel]
                    log(f"Group mode: found {len(target_channels)} target channels")
                else:
                    log(f"Group {task_info.group_id} has no target channels configured", "WARN")
                    return False
            else:
                log("No group_id, skipping upload", "WARN")
                return False
            
            upload_success_count = 0
            for target_channel in target_channels:
                try:
                    log(f"Uploading to channel: {target_channel}")
                    
                    try:
                        entity = await self.client.get_entity(target_channel)
                        chat_id = entity.id
                    except:
                        chat_id = target_channel
                    
                    await self.client.send_file(
                        chat_id,
                        final_path,
                        caption=caption,
                        supports_streaming=True
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
        for retry in range(MAX_RETRIES):
            success = await self.upload_file(file_path, task_id)
            
            if success:
                return True
            
            log(f"Upload failed, retry {retry + 1}/{MAX_RETRIES}...", "WARN")
            await asyncio.sleep(RETRY_DELAY)
        
        log(f"Upload failed, retried {MAX_RETRIES} times", "ERROR")
        return False
    
    async def process_upload_tasks(self):
        try:
            db_manager = DatabaseManager()
            consecutive_empty = 0
            log("🟢 Upload task processor started")
            
            # 初始状态检查
            log_separator("📊 Initial Database Status Check")
            all_stats = db_manager.get_task_queue_stats()
            if all_stats:
                log("Current tasks in database:")
                for status, count in sorted(all_stats.items()):
                    log(f"  - {status}: {count}")
            else:
                log("No tasks found in database")
            log_separator()
            
            while self.running:
                all_tasks = db_manager.get_tasks(status="ai_captioned", limit=100)
                if not all_tasks:
                    all_tasks = db_manager.get_tasks(status="watermarked", limit=100)
                
                tasks = all_tasks
                
                if not tasks:
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        log("⏳ No tasks to upload, taking a longer break...")
                        if consecutive_empty % 5 == 0:
                            stats = db_manager.get_task_queue_stats()
                            if stats:
                                log("📊 Current task status:")
                                for s, c in sorted(stats.items()):
                                    log(f"  - {s}: {c}")
                    await asyncio.sleep(5)
                    continue
                
                consecutive_empty = 0
                task = tasks[0]
                
                log(f"📋 Found task {task.id} with status: {task.status}")
                
                upload_file = task.file_path
                
                if not upload_file:
                    filename = f"{task.id}_watermarked.mp4"
                    upload_file = os.path.join(PROCESSED_PATH, filename)
                    log(f"  Trying processed path: {os.path.basename(upload_file)}")
                    if not os.path.exists(upload_file):
                        filename = f"{task.id}.mp4"
                        upload_file = os.path.join(PROCESSED_PATH, filename)
                        log(f"  Trying processed path (no watermark): {os.path.basename(upload_file)}")
                        if not os.path.exists(upload_file):
                            filename = f"{task.id}_watermarked.mp4"
                            upload_file = os.path.join(UPLOAD_PATH, filename)
                            log(f"  Trying upload path: {os.path.basename(upload_file)}")
                            if not os.path.exists(upload_file):
                                filename = f"{task.id}.mp4"
                                upload_file = os.path.join(UPLOAD_PATH, filename)
                                log(f"  Trying upload path (no watermark): {os.path.basename(upload_file)}")
                
                if not upload_file or not os.path.exists(upload_file):
                    log(f"❌ Task {task.id} file not found", "WARN")
                    db_manager.update_task(task.id, status="error", error_msg="File not found")
                    continue
                
                if upload_file.startswith('./'):
                    upload_file = os.path.abspath(upload_file)
                
                log_separator(f"🚀 Processing task {task.id}")
                log(f"📁 File: {os.path.basename(upload_file)}")
                log(f"📊 File size: {os.path.getsize(upload_file) / (1024*1024):.2f} MB")
                
                success = await self.upload_with_retry(upload_file, task.id)
                
                if success:
                    db_manager.update_task(task.id, status="uploaded", upload_time=datetime.now())
                    log(f"✅ Task {task.id} upload completed")
                    cleanup_all_task_files(task.id)
                else:
                    db_manager.update_task(task.id, status="error", error_msg="Upload failed")
                    log(f"❌ Task {task.id} upload failed", "ERROR")
                
                log("😴 Taking a 5 second break before continuing...")
                await asyncio.sleep(5)
        except Exception as e:
            log(f"💥 Upload task processor error: {e}", "ERROR")
            traceback.print_exc()
    
    async def init_and_run(self, session_string, api_id, api_hash):
        workdir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
        os.makedirs(workdir, exist_ok=True)
        
        from telethon.sessions import StringSession
        self.client = TelegramClient(
            StringSession(session_string),
            api_id,
            api_hash
        )
        
        print("[DEBUG] Registering handlers...", flush=True)
        
        self.client.add_event_handler(self.handle_help, events.NewMessage(pattern='/help'))
        self.client.add_event_handler(self.handle_add_group, events.NewMessage(pattern='/add_group'))
        self.client.add_event_handler(self.handle_del_group, events.NewMessage(pattern='/del_group'))
        self.client.add_event_handler(self.handle_rename_group, events.NewMessage(pattern='/rename_group'))
        self.client.add_event_handler(self.handle_add_monitor, events.NewMessage(pattern='/addup'))
        self.client.add_event_handler(self.handle_del_monitor, events.NewMessage(pattern='/del_monitor'))
        self.client.add_event_handler(self.handle_add_target, events.NewMessage(pattern='/add_target'))
        self.client.add_event_handler(self.handle_del_target, events.NewMessage(pattern='/del_target'))
        self.client.add_event_handler(self.handle_set_promotion, events.NewMessage(pattern='/set_promotion'))
        self.client.add_event_handler(self.handle_status, events.NewMessage(pattern='/status'))
        self.client.add_event_handler(self.handle_queue, events.NewMessage(pattern='/queue'))
        self.client.add_event_handler(self.handle_sync, events.NewMessage(pattern='/sync'))
        self.client.add_event_handler(self.handle_sync_ids, events.NewMessage(pattern='/sync_ids'))
        self.client.add_event_handler(self.handle_clear_cache, events.NewMessage(pattern='/clear_cache'))
        self.client.add_event_handler(self.handle_time, events.NewMessage(pattern='/time'))
        self.client.add_event_handler(self.handle_run_now, events.NewMessage(pattern='/run_now'))
        self.client.add_event_handler(self.handle_workflow_status, events.NewMessage(pattern='/workflow_status'))
        self.client.add_event_handler(self.handle_test_target, events.NewMessage(pattern='/test_target'))
        self.client.add_event_handler(self.handle_mon_list, events.NewMessage(pattern='/mon_list'))
        self.client.add_event_handler(self.handle_list_errors, events.NewMessage(pattern='/list_errors'))
        self.client.add_event_handler(self.handle_retry, events.NewMessage(pattern='/retry'))
        self.client.add_event_handler(self.handle_retry_failed, events.NewMessage(pattern='/retry_failed'))
        self.client.add_event_handler(self.handle_skip, events.NewMessage(pattern='/skip'))
        self.client.add_event_handler(self.handle_set_caption_style, events.NewMessage(pattern='/set_caption_style'))
        self.client.add_event_handler(self.handle_all_messages, events.NewMessage())
        
        print("[DEBUG] Starting client...", flush=True)
        
        await self.client.start()
        
        log_separator("✅ COMPLETE TELEGRAM BOT + UPLOAD STARTED!")
        log("All commands available!")
        log("Upload processor running in background")
        log("Send /help for help")
        log("\n📋 Upload Processor Status:")
        log(f"   - DOWNLOAD_PATH: {DOWNLOAD_PATH}")
        log(f"   - PROCESSED_PATH: {PROCESSED_PATH}")
        log(f"   - UPLOAD_PATH: {UPLOAD_PATH}")
        log(f"   - Checking for tasks every 30 seconds")
        log("   - Will process 'ai_captioned' or 'watermarked' tasks")
        
        upload_task = asyncio.create_task(self.process_upload_tasks())
        
        await self.client.run_until_disconnected()
        
        self.running = False
        await upload_task

async def main():
    print("[DEBUG] Finding bot config...", flush=True)
    
    if not TELEGRAM_BOTS or not isinstance(TELEGRAM_BOTS, dict) or len(TELEGRAM_BOTS) == 0:
        log("ERROR: No TELEGRAM_BOTS configured!", "ERROR")
        return
    
    first_bot_name = list(TELEGRAM_BOTS.keys())[0]
    bot_config = TELEGRAM_BOTS[first_bot_name]
    session_string = bot_config.get("session_string") or os.environ.get('TELEGRAM_SESSION_STRING')
    api_id = bot_config.get("api_id")
    api_hash = bot_config.get("api_hash")
    
    if not session_string:
        log("ERROR: No session_string found!", "ERROR")
        return
    
    if not api_id:
        api_id = 12345
        log("WARNING: Using default api_id=12345", "WARN")
    
    if not api_hash:
        api_hash = "0123456789abcdef0123456789abcdef"
        log("WARNING: Using default api_hash", "WARN")
    
    print(f"[DEBUG] Using bot: {first_bot_name}", flush=True)
    
    bot = CompleteTelegramBot()
    
    import subprocess
    import sys
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "group_workflow.py")
    workflow_process = subprocess.Popen(
        [sys.executable, script_path],
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    print("[DEBUG] Workflow auto-started!", flush=True)
    
    await bot.init_and_run(session_string, api_id, api_hash)

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'OK')
    def log_message(self, format, *args):
        pass

def start_health_server(port=7860):
    try:
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        server.serve_forever()
    except Exception as e:
        print(f"Health server error: {e}", flush=True)

if __name__ == "__main__":
    print("[DEBUG] === MAIN ===", flush=True)

    threading.Thread(target=start_health_server, daemon=True).start()
    print("[DEBUG] Health check server started on port 7860", flush=True)

    while True:
        try:
            import asyncio
            asyncio.run(main())
        except Exception as e:
            print(f"❌ ERROR: {e}", flush=True)
            traceback.print_exc()
            print("⏳ Restarting in 10s...", flush=True)
            time.sleep(10)
