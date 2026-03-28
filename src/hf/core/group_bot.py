"""Group Management Telegram Bot Command Handler"""
import os
import sys
import re
import json
import subprocess
import threading
import time
import traceback
from datetime import datetime
from typing import List, Tuple, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    """Main function - All initialization here"""
    print("=== DISPATCHING BOT VERSION: 2026-03-20-V3 ===")
    from hf.utils.module_initializer import initialize_module
    initialize_module(__file__)
    
    from pyrogram import Client, filters
    from pyrogram.types import Message

    from database import DatabaseManager
    from hf.database.models import MonitorGroup, GroupMonitor, GroupTarget
    import config

    class GroupBotHandler:
        """Group Management Bot Command Handler"""
        
        def __init__(self, client: Client, db: DatabaseManager):
            self.client = client
            self.db = db
            self.workflow_process = None
            self.workflow_thread = None
            self.register_handlers()
        
        def register_handlers(self):
            """Register command handlers - ONE simple handler for everything!"""
            
            @self.client.on_message(filters.text)
            async def handle_all_commands(client, message: Message):
                print(f"[DEBUG] Received message: {message.text}")
                print(f"[DEBUG] From: {message.from_user.id if message.from_user else 'unknown'}")
                print(f"[DEBUG] Chat: {message.chat.id}")
                
                if not message.text:
                    return
                
                text = message.text.strip()

                print(f"[DEBUG] Processing command: {text}", flush=True)

                if text == "/help" or text.startswith("/help "):
                    print(f"[DEBUG] /help detected")
                    await self.handle_help(message)
                elif text.startswith("/add_group "):
                    await self.handle_add_group(message)
                elif text.startswith("/del_group "):
                    await self.handle_del_group(message)
                elif text.startswith("/rename_group "):
                    await self.handle_rename_group(message)
                elif text.startswith("/addup "):
                    await self.handle_add_monitor(message)
                elif text.startswith("/del_monitor "):
                    await self.handle_del_monitor(message)
                elif text.startswith("/add_target "):
                    await self.handle_add_target(message)
                elif text.startswith("/del_target "):
                    await self.handle_del_target(message)
                elif text.startswith("/set_promotion "):
                    await self.handle_set_promotion(message)
                elif text == "/status" or text.startswith("/status "):
                    await self.handle_status(message)
                elif text == "/sync" or text.startswith("/sync "):
                    await self.handle_sync(message)
                elif text == "/sync_ids" or text.startswith("/sync_ids "):
                    await self.handle_sync_ids(message)
                elif text == "/clear_cache" or text.startswith("/clear_cache "):
                    await self.handle_clear_cache(message)
                elif text == "/time" or text.startswith("/time "):
                    await self.handle_set_time(message)
                elif text == "/queue" or text.startswith("/queue "):
                    await self.handle_queue_status(message)
                elif text == "/start" or text.startswith("/start "):
                    await self.handle_start_workflow(message)
                elif text == "/stop" or text.startswith("/stop "):
                    await self.handle_stop_workflow(message)
                elif text == "/run_once" or text.startswith("/run_once "):
                    await self.handle_run_once(message)
                elif text == "/workflow_status" or text.startswith("/workflow_status "):
                    await self.handle_workflow_status(message)
                elif text == "/mon_list" or text.startswith("/mon_list "):
                    await self.handle_mon_list(message)
                elif text == "/list_errors" or text.startswith("/list_errors "):
                    await self.handle_list_errors(message)
                elif text.startswith("/retry "):
                    await self.handle_retry_task(message)
                elif text.startswith("/skip "):
                    await self.handle_skip_task(message)
                elif text.startswith("/set_caption_style "):
                    await self.handle_set_caption_style(message)
        
        def parse_args(self, text: str) -> List[str]:
            """Parse command arguments"""
            if not text:
                return []
            
            parts = text.split()
            if not parts:
                return []
            
            return parts
        
        async def handle_add_group(self, message: Message):
            """Handle /add_group group_name"""
            args = self.parse_args(message.text)
            if len(args) < 2:
                await message.reply_text("Error: Usage: /add_group group_name")
                return
            
            group_name = " ".join(args[1:])
            group = MonitorGroup(name=group_name)
            group_id = self.db.add_monitor_group(group)
            
            if group_id:
                await message.reply_text(f"OK! Group created!\nID: {group_id}\nName: {group_name}")
            else:
                await message.reply_text("Error: Group creation failed, maybe name exists")
        
        async def handle_del_group(self, message: Message):
            """Handle /del_group group_id"""
            args = self.parse_args(message.text)
            if len(args) < 2:
                await message.reply_text("Error: Usage: /del_group group_id")
                return
            
            try:
                group_id = int(args[1])
                group = self.db.get_monitor_group(group_id)
                if not group:
                    await message.reply_text("Error: Group not found")
                    return
                
                self.db.delete_monitor_group(group_id)
                await message.reply_text(f"OK! Group deleted!\nID: {group_id}\nName: {group.name}")
            except ValueError:
                await message.reply_text("Error: Group ID must be a number")
        
        async def handle_rename_group(self, message: Message):
            """Handle /rename_group group_id new_name"""
            args = self.parse_args(message.text)
            if len(args) < 3:
                await message.reply_text("Error: Usage: /rename_group group_id new_name")
                return
            
            try:
                group_id = int(args[1])
                new_name = " ".join(args[2:])
                
                group = self.db.get_monitor_group(group_id)
                if not group:
                    await message.reply_text("Error: Group not found")
                    return
                
                self.db.update_monitor_group(group_id, name=new_name)
                await message.reply_text(f"OK! Group renamed!\nID: {group_id}\nOld: {group.name}\nNew: {new_name}")
            except ValueError:
                await message.reply_text("Error: Group ID must be a number")
        
        def parse_monitor_list(self, text: str) -> List[Tuple[str, str]]:
            """Parse monitor list"""
            monitors = []
            
            text = text.replace("：", ":").replace("，", ",").replace(" ", "")
            parts = text.split(",")
            
            for part in parts:
                if ":" in part:
                    name, url = part.split(":", 1)
                    if name and url:
                        monitors.append((name.strip(), url.strip()))
            
            return monitors
        
        async def handle_add_monitor(self, message: Message):
            """Handle /addup group_id up1:url1,up2:url2"""
            args = message.text.split(maxsplit=2)
            if len(args) < 3:
                await message.reply_text("Error: Usage: /addup group_id up1:url1,up2:url2")
                return
            
            try:
                group_id = int(args[1])
                monitor_text = args[2]
                
                group = self.db.get_monitor_group(group_id)
                if not group:
                    await message.reply_text("Error: Group not found")
                    return
                
                monitors = self.parse_monitor_list(monitor_text)
                if not monitors:
                    await message.reply_text("Error: Cannot parse monitor list, check format")
                    return
                
                added = []
                skipped = []
                
                for name, url in monitors:
                    monitor = GroupMonitor(group_id=group_id, up_name=name, up_url=url)
                    monitor_id = self.db.add_group_monitor(monitor)
                    if monitor_id:
                        added.append(f"{name}:{url}")
                    else:
                        skipped.append(f"{name}:{url}")
                
                result = f"OK! Added monitors to group '{group.name}'!\n\n"
                if added:
                    result += f"Added {len(added)}:\n" + "\n".join(added) + "\n\n"
                if skipped:
                    result += f"Skipped {len(skipped)} (already exists):\n" + "\n".join(skipped)
                
                await message.reply_text(result)
            except ValueError:
                await message.reply_text("Error: Group ID must be a number")
        
        async def handle_del_monitor(self, message: Message):
            """Handle /del_monitor group_id up1:url1,up2:url2 OR /del_monitor group_id index"""
            args = message.text.split(maxsplit=2)
            if len(args) < 3:
                await message.reply_text("Error: Usage: /del_monitor group_id up1:url1,up2:url2\n   or /del_monitor group_id index")
                return

            try:
                group_id = int(args[1])
                monitor_text = args[2]

                group = self.db.get_monitor_group(group_id)
                if not group:
                    await message.reply_text("Error: Group not found")
                    return

                # 如果 monitor_text 是纯数字，按编号删除
                if monitor_text.isdigit():
                    index = int(monitor_text)
                    monitors = self.db.get_group_monitors(group_id)
                    if index < 1 or index > len(monitors):
                        await message.reply_text(f"Error: Index {index} out of range (1-{len(monitors)})")
                        return

                    mon = monitors[index - 1]
                    self.db.delete_group_monitor_by_url(group_id, mon.up_url)
                    await message.reply_text(f"OK! Deleted [{index}] {mon.up_name}\n   {mon.up_url}")
                    return

                # 否则按 url 删除
                monitors = self.parse_monitor_list(monitor_text)
                if not monitors:
                    await message.reply_text("Error: Cannot parse monitor list, check format")
                    return

                deleted = []

                for name, url in monitors:
                    self.db.delete_group_monitor_by_url(group_id, url)
                    deleted.append(f"{name}:{url}")

                result = f"OK! Deleted monitors from group '{group.name}'!\n\n"
                result += f"Deleted {len(deleted)}:\n" + "\n".join(deleted)

                await message.reply_text(result)
            except ValueError:
                await message.reply_text("Error: Group ID must be a number")
        
        async def handle_add_target(self, message: Message):
            """Handle /add_target group_id @channel"""
            args = self.parse_args(message.text)
            if len(args) < 3:
                await message.reply_text("Error: Usage: /add_target group_id @channel")
                return
            
            try:
                group_id = int(args[1])
                target_channel = args[2]
                
                group = self.db.get_monitor_group(group_id)
                if not group:
                    await message.reply_text("Error: Group not found")
                    return
                
                target = GroupTarget(group_id=group_id, target_channel=target_channel)
                target_id = self.db.add_group_target(target)
                
                if target_id:
                    await message.reply_text(f"OK! Target added!\nGroup: {group.name}\nTarget: {target_channel}")
                else:
                    await message.reply_text("Error: Target add failed, maybe already exists")
            except ValueError:
                await message.reply_text("Error: Group ID must be a number")
        
        async def handle_del_target(self, message: Message):
            """Handle /del_target group_id @channel"""
            args = self.parse_args(message.text)
            if len(args) < 3:
                await message.reply_text("Error: Usage: /del_target group_id @channel")
                return
            
            try:
                group_id = int(args[1])
                target_channel = args[2]
                
                group = self.db.get_monitor_group(group_id)
                if not group:
                    await message.reply_text("Error: Group not found")
                    return
                
                self.db.delete_group_target_by_channel(group_id, target_channel)
                await message.reply_text(f"OK! Target deleted!\nGroup: {group.name}\nTarget: {target_channel}")
            except ValueError:
                await message.reply_text("Error: Group ID must be a number")
        
        async def handle_set_promotion(self, message: Message):
            """Handle /set_promotion group_id text"""
            args = message.text.split(maxsplit=2)
            if len(args) < 3:
                await message.reply_text("Error: Usage: /set_promotion group_id text")
                return
            
            try:
                group_id = int(args[1])
                promotion_text = args[2]
                
                group = self.db.get_monitor_group(group_id)
                if not group:
                    await message.reply_text("Error: Group not found")
                    return
                
                self.db.update_monitor_group(group_id, promotion_text=promotion_text)
                await message.reply_text(f"OK! Promotion text set!\nGroup: {group.name}\nText: {promotion_text}")
            except ValueError:
                await message.reply_text("Error: Group ID must be a number")
        
        async def handle_status(self, message: Message):
            """Handle /status - View all groups status"""
            groups = self.db.get_monitor_groups()
            
            if not groups:
                await message.reply_text("No groups configured")
                return
            
            result = "System Status\n" + "="*40 + "\n\n"
            
            for group in groups:
                monitors = self.db.get_group_monitors(group.id)
                targets = self.db.get_group_targets(group.id)
                
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
            
            interval = self.db.get_system_config("check_interval", "3600")
            result += f"Check interval: {interval} seconds\n"
            
            await message.reply_text(result)
        
        async def handle_sync(self, message: Message):
            """Handle /sync - Force sync config to cloud"""
            try:
                groups = self.db.get_monitor_groups()
                config_data = []
                
                for group in groups:
                    monitors = self.db.get_group_monitors(group.id)
                    targets = self.db.get_group_targets(group.id)
                    
                    config_data.append({
                        "id": group.id,
                        "name": group.name,
                        "promotion_text": group.promotion_text,
                        "monitors": [{"name": m.up_name, "url": m.up_url} for m in monitors],
                        "targets": [{"channel": t.target_channel, "chat_id": t.chat_id} for t in targets]
                    })
                
                config_json = json.dumps(config_data, ensure_ascii=False, indent=2)
                
                config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "groups_config.json")
                os.makedirs(os.path.dirname(config_path), exist_ok=True)
                
                with open(config_path, "w", encoding="utf-8") as f:
                    f.write(config_json)
                
                await message.reply_text(f"OK! Config synced!\nSaved {len(groups)} groups\nFile: {config_path}")
            except Exception as e:
                await message.reply_text(f"Error: Config sync failed: {e}")
        
        async def handle_sync_ids(self, message: Message):
            """Handle /sync_ids - Sync all target channel chat_ids"""
            targets = self.db.get_group_targets()
            updated = 0
            
            for target in targets:
                if not target.chat_id and target.target_channel.startswith("@"):
                    try:
                        chat = await self.client.get_chat(target.target_channel)
                        if chat:
                            self.db.update_group_target_chat_id(target.id, chat.id)
                            updated += 1
                    except Exception as e:
                        pass
            
            await message.reply_text(f"OK! chat_id synced!\nUpdated: {updated}\nTotal: {len(targets)}")
        
        async def handle_clear_cache(self, message: Message):
            """Handle /clear_cache - Clear all cache"""
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
                
                await message.reply_text(f"OK! Cache cleared!\nFiles cleared: {cleared}")
            except Exception as e:
                await message.reply_text(f"Error: Cache clear failed: {e}")
        
        async def handle_set_time(self, message: Message):
            """Handle /time interval_seconds"""
            args = self.parse_args(message.text)
            if len(args) < 2:
                current = self.db.get_system_config("check_interval", "3600")
                await message.reply_text(f"Current check interval: {current} seconds\n\nUsage: /time interval_seconds")
                return
            
            try:
                interval = int(args[1])
                if interval < 60:
                    await message.reply_text("Error: Interval must be at least 60 seconds")
                    return
                
                self.db.set_system_config("check_interval", str(interval))
                await message.reply_text(f"OK! Check interval set to {interval} seconds")
            except ValueError:
                await message.reply_text("Error: Interval must be a number")
        
        async def handle_queue_status(self, message: Message):
            """Handle /queue - View current queue"""
            stats = self.db.get_task_queue_stats()
            
            if not stats:
                await message.reply_text("Queue is empty")
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
            
            await message.reply_text(result)
        
        async def handle_start_workflow(self, message: Message):
            """Handle /start - Start workflow"""
            if self.workflow_process and self.workflow_process.poll() is None:
                await message.reply_text("Warning: Workflow already running")
                return
            
            try:
                script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "group_workflow.py")
                self.workflow_process = subprocess.Popen(
                    [sys.executable, script_path],
                    cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True
                )
                await message.reply_text("OK! Workflow started!")
            except Exception as e:
                await message.reply_text(f"Error: Start workflow failed: {e}")
        
        async def handle_stop_workflow(self, message: Message):
            """Handle /stop - Stop workflow"""
            if not self.workflow_process or self.workflow_process.poll() is not None:
                await message.reply_text("Warning: Workflow not running")
                return
            
            try:
                self.workflow_process.terminate()
                self.workflow_process.wait(timeout=10)
                await message.reply_text("OK! Workflow stopped")
            except subprocess.TimeoutExpired:
                self.workflow_process.kill()
                await message.reply_text("Warning: Workflow force stopped")
            except Exception as e:
                await message.reply_text(f"Error: Stop workflow failed: {e}")
        
        async def handle_run_once(self, message: Message):
            """Handle /run_once - Run workflow once"""
            if self.workflow_process and self.workflow_process.poll() is None:
                await message.reply_text("Warning: Workflow already running, stop first")
                return
            
            try:
                script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "group_workflow.py")
                result = subprocess.run(
                    [sys.executable, script_path, "--once"],
                    cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    capture_output=True,
                    text=True,
                    timeout=7200
                )
                if result.returncode == 0:
                    await message.reply_text("OK! Workflow run once completed!")
                else:
                    await message.reply_text(f"Warning: Workflow run once completed (exit code: {result.returncode})")
            except subprocess.TimeoutExpired:
                await message.reply_text("Error: Workflow run once timeout")
            except Exception as e:
                await message.reply_text(f"Error: Run workflow once failed: {e}")
        
        async def handle_workflow_status(self, message: Message):
            """Handle /workflow_status - View workflow status"""
            if self.workflow_process and self.workflow_process.poll() is None:
                await message.reply_text("Workflow is running")
            else:
                await message.reply_text("Workflow is not running")

        async def handle_mon_list(self, message: Message):
            """Handle /mon_list - List all monitors with index"""
            try:
                groups = self.db.get_monitor_groups()
                if not groups:
                    await message.reply_text("没有监控分组")
                    return

                result = "📋 监控对象列表\n"
                result += "=" * 50 + "\n\n"

                for group in groups:
                    result += f"📁 分组 {group.id}: {group.name}\n"
                    if group.ai_caption_style:
                        result += f"   文案风格: {group.ai_caption_style}\n"

                    monitors = self.db.get_group_monitors(group.id)
                    if monitors:
                        for i, mon in enumerate(monitors, 1):
                            result += f"   [{i}] {mon.up_name}\n"
                            result += f"       {mon.up_url}\n"
                        result += f"   共 {len(monitors)} 个监控对象\n"
                    else:
                        result += "   (无监控对象)\n"
                    result += "\n"

                result += "\n💡 使用 /del_monitor <分组ID> <编号> 删除监控对象"

                if len(result) > 4096:
                    result = result[:4050] + "\n... (内容过长已截断)"

                await message.reply_text(result)
            except Exception as e:
                print(f"[ERROR] handle_mon_list: {e}", flush=True)
                import traceback
                traceback.print_exc()
                await message.reply_text(f"Error: {str(e)}")

        async def handle_list_errors(self, message: Message):
            """Handle /list_errors - View failed tasks"""
            try:
                error_statuses = ["error", "download_failed", "upload_failed", "api_error"]
                error_tasks = []

                for status in error_statuses:
                    tasks = self.db.get_tasks(status=status, limit=100)
                    for task in tasks:
                        if task:
                            error_tasks.append(task)

                if not error_tasks:
                    await message.reply_text("✅ 没有失败任务")
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

                await message.reply_text(result)
            except Exception as e:
                print(f"[ERROR] handle_list_errors: {e}", flush=True)
                import traceback
                traceback.print_exc()
                await message.reply_text(f"Error: {str(e)}")

        async def handle_retry_task(self, message: Message):
            """Handle /retry - Retry failed tasks"""
            args = self.parse_args(message.text)
            if len(args) < 2:
                await message.reply_text("Usage: /retry <task_id>\n   or /retry all - Retry all failed tasks")
                return

            arg = args[1]

            if arg.lower() == "all":
                error_statuses = ["error", "download_failed", "upload_failed", "api_error"]
                reset_count = 0
                for status in error_statuses:
                    tasks = self.db.get_tasks(status=status, limit=1000)
                    for task in tasks:
                        if task:
                            self.db.update_task(task.id, status="pending", error_msg="")
                            reset_count += 1
                await message.reply_text(f"✅ 已重置 {reset_count} 个失败任务为 pending 状态")
                return

            try:
                task_id = int(arg)
                task = self.db.get_task(task_id)
                if not task:
                    await message.reply_text(f"Error: Task {task_id} not found")
                    return

                self.db.update_task(task_id, status="pending", error_msg="")
                await message.reply_text(f"✅ Task {task_id} 已重置为 pending 状态")
            except ValueError:
                await message.reply_text("Error: Invalid task ID")

        async def handle_skip_task(self, message: Message):
            """Handle /skip - Skip a task"""
            args = self.parse_args(message.text)
            if len(args) < 2:
                await message.reply_text("Usage: /skip <task_id>")
                return

            try:
                task_id = int(args[1])
                task = self.db.get_task(task_id)
                if not task:
                    await message.reply_text(f"Error: Task {task_id} not found")
                    return

                self.db.update_task(task_id, status="skipped", error_msg="手动跳过")
                await message.reply_text(f"✅ Task {task_id} 已标记为 skipped")
            except ValueError:
                await message.reply_text("Error: Invalid task ID")

        async def handle_set_caption_style(self, message: Message):
            """Handle /set_caption_style - Set caption style for group"""
            try:
                args = self.parse_args(message.text)
                if len(args) < 3:
                    await message.reply_text(
                        "Error: Usage: /set_caption_style group_id style\n\n"
                        "Styles:\n"
                        "  - default/口播: AI optimized caption (for talking videos)\n"
                        "  - humor/short/乐子: Short fun caption (for comedy videos)\n"
                        "  - none/舞蹈/null: No AI caption (for dance videos)"
                    )
                    return

                group_id = int(args[1])
                style = " ".join(args[2:]).lower()

                print(f"[DEBUG] set_caption_style: group_id={group_id}, style={style}", flush=True)

                group = self.db.get_monitor_group(group_id)
                if not group:
                    await message.reply_text("Error: Group not found")
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
                    await message.reply_text(f"Error: Unknown style '{style}'")
                    return

                self.db.update_monitor_group(group_id, ai_caption_style=ai_caption_style)
                await message.reply_text(
                    f"OK! Group {group_id} caption style updated!\n"
                    f"Group: {group.name}\n"
                    f"Style: {style_name}"
                )
            except ValueError:
                await message.reply_text("Error: Group ID must be a number")
            except Exception as e:
                print(f"[ERROR] handle_set_caption_style: {e}", flush=True)
                import traceback
                traceback.print_exc()
                await message.reply_text(f"Error: {str(e)}")

        async def handle_help(self, message: Message):
            """Handle /help - Show help message"""
            print(f"[DEBUG] handle_help called")
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
  /skip <task_id> - Skip a task
  /sync - Force sync config to cloud
  /sync_ids - Sync all target channel chat_ids
  /clear_cache - Clear all cache
  /time [interval] - View or set check interval (seconds)

Workflow Control:
  /start - Start workflow (infinite loop)
  /stop - Stop workflow
  /run_once - Run workflow once
  /workflow_status - View workflow status

Help:
  /help - Show this help message
        """
            print(f"[DEBUG] Sending help message...")
            try:
                await message.reply_text(help_text.strip())
                print(f"[DEBUG] Help message sent successfully")
            except Exception as e:
                print(f"[DEBUG] Error sending help message: {e}")
                import traceback
                traceback.print_exc()


    print("="*60)
    print("Starting Group Management Bot")
    print("="*60)
    
    print("[DEBUG] Step 1: Initializing database...")
    try:
        db = DatabaseManager()
        print("[DEBUG] OK: Database connected")
    except Exception as e:
        print(f"[DEBUG] Error: Database connect failed: {e}")
        traceback.print_exc()
        time.sleep(5)
        return
    
    print("[DEBUG] Step 2: Loading Telegram config...")
    try:
        telegram_config = config.TELEGRAM_BOTS
        print(f"[DEBUG] OK: Config loaded: {telegram_config}")
    except Exception as e:
        print(f"[DEBUG] Error: Config load failed: {e}")
        traceback.print_exc()
        time.sleep(5)
        return
    
    if not telegram_config:
        print("[DEBUG] Error: No Telegram Bot configured")
        time.sleep(5)
        return
    
    print("[DEBUG] Step 3: Extracting bot config...")
    bot_config = list(telegram_config.values())[0] if isinstance(telegram_config, dict) else telegram_config[0]
    print(f"[DEBUG] Bot config: {bot_config}")
    
    api_id = bot_config.get("api_id")
    api_hash = bot_config.get("api_hash")
    bot_token = bot_config.get("bot_token")
    
    print(f"[DEBUG] api_id: {api_id}")
    print(f"[DEBUG] api_hash: {api_hash[:10] if api_hash else None}...")
    print(f"[DEBUG] bot_token: {bot_token[:10] if bot_token else None}...")
    
    if not bot_token:
        print("[DEBUG] Error: Telegram Bot config incomplete, missing bot_token")
        time.sleep(5)
        return
    
    if api_id and api_hash:
        print("[DEBUG] Step 4: Starting Pyrogram Client...")
        print("Starting Group Management Bot with Pyrogram...")
        print(f"   API ID: {api_id}")
        print(f"   Bot Token: {bot_token[:10]}...")
        
        try:
            workdir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
            print(f"[DEBUG] Work dir: {workdir}")
            print(f"[DEBUG] Creating Client instance...")
            
            app = Client(
                "group_bot",
                api_id=api_id,
                api_hash=api_hash,
                bot_token=bot_token,
                in_memory=True,
                workdir=workdir
            )
            
            print("[DEBUG] Client created, creating handler...")
            handler = GroupBotHandler(app, db)
            print("[DEBUG] Handler created, registering handlers...")
            
            print("OK: Bot started, waiting for commands...")
            print("Tip: Send /help for help")
            print("[DEBUG] Calling app.run()...")
            
            app.run()
        except Exception as e:
            print(f"Error: Bot start failed: {e}")
            traceback.print_exc()
            time.sleep(10)
    else:
        print("Warning: No api_id/api_hash configured, some features may be limited")
        print("Tip: Configure full Telegram Bot parameters for best experience")
        print("Error: Pyrogram requires api_id and api_hash to run")
        print("Tip: Please configure in config/telegram.json or environment variables")
        print("   Or use pure TGBot mode (needs code refactor)")
        time.sleep(10)


if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(f"Error: Main process exception: {e}")
            traceback.print_exc()
            print("Waiting 10 seconds before restart...")
            time.sleep(10)
