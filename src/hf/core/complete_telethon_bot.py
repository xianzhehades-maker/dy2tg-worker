"""Complete Telegram Bot - All Features with Telethon"""
import os
import sys
import time
import subprocess
import asyncio
import traceback
import json
from datetime import datetime

print("="*60, flush=True)
print("🚀 COMPLETE TELEGRAM BOT", flush=True)
print("="*60, flush=True)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    print("[DEBUG] Importing config...", flush=True)
    import config
    print(f"[DEBUG] Config imported!", flush=True)
    
    print("[DEBUG] Importing database...", flush=True)
    from database import DatabaseManager
    from hf.database.models import MonitorGroup, GroupMonitor, GroupTarget
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

class CompleteTelegramBot:
    def __init__(self):
        self.client = None
        self.db_manager = DatabaseManager()
        self.workflow_process = None
    
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
    
    async def handle_help(self, event):
        print("[DEBUG] /help called", flush=True)
        help_text = """
Group Management System - Help

Group Management:
  /add_group group_name - Add new group
  /del_group group_id - Delete group
  /rename_group group_id new_name - Rename group

Monitor Management:
  /add group_id up1:url1,up2:url2 - Add monitors to group
  /del_monitor group_id up1:url1,up2:url2 - Delete monitors from group

Target Management:
  /add_target group_id @channel - Add target to group
  /del_target group_id @channel - Delete target from group

Other Settings:
  /set_promotion group_id text - Set promotion text

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
        await event.reply(help_text.strip())
    
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
    
    async def handle_add_monitor(self, event):
        args = self.parse_args(event.raw_text)
        if len(args) < 3:
            await event.reply("Error: Usage: /add group_id up1:url1,up2:url2")
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
    
    async def handle_del_monitor(self, event):
        args = self.parse_args(event.raw_text)
        if len(args) < 3:
            await event.reply("Error: Usage: /del_monitor group_id up1:url1,up2:url2")
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
            
            deleted = []
            
            for name, url in monitors:
                self.db_manager.delete_group_monitor_by_url(group_id, url)
                deleted.append(f"{name}:{url}")
            
            result = f"OK! Deleted monitors from group '{group.name}'!\n\n"
            result += f"Deleted {len(deleted)}:\n" + "\n".join(deleted)
            
            await event.reply(result)
        except ValueError:
            await event.reply("Error: Group ID must be a number")
    
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
    
    async def handle_sync(self, event):
        import json
        try:
            groups = self.db_manager.get_monitor_groups()
            config_data = []
            
            for group in groups:
                monitors = self.db_manager.get_group_monitors(group.id)
                targets = self.db_manager.get_group_targets(group.id)
                
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
            
            await event.reply(f"OK! Config synced!\nSaved {len(groups)} groups\nFile: {config_path}")
        except Exception as e:
            await event.reply(f"Error: Config sync failed: {e}")
    
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
    
    async def handle_start_workflow(self, event):
        if self.workflow_process and self.workflow_process.poll() is None:
            await event.reply("Warning: Workflow already running")
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
            await event.reply("OK! Workflow started!")
        except Exception as e:
            await event.reply(f"Error: Start workflow failed: {e}")
    
    async def handle_stop_workflow(self, event):
        if not self.workflow_process or self.workflow_process.poll() is not None:
            await event.reply("Warning: Workflow not running")
            return
        
        try:
            self.workflow_process.terminate()
            self.workflow_process.wait(timeout=10)
            await event.reply("OK! Workflow stopped")
        except subprocess.TimeoutExpired:
            self.workflow_process.kill()
            await event.reply("Warning: Workflow force stopped")
        except Exception as e:
            await event.reply(f"Error: Stop workflow failed: {e}")
    
    async def handle_run_once(self, event):
        if self.workflow_process and self.workflow_process.poll() is None:
            await event.reply("Warning: Workflow already running, stop first")
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
                await event.reply("OK! Workflow run once completed!")
            else:
                await event.reply(f"Warning: Workflow run once completed (exit code: {result.returncode})")
        except subprocess.TimeoutExpired:
            await event.reply("Error: Workflow run once timeout")
        except Exception as e:
            await event.reply(f"Error: Run workflow once failed: {e}")
    
    async def handle_workflow_status(self, event):
        if self.workflow_process and self.workflow_process.poll() is None:
            await event.reply("Workflow is running")
        else:
            await event.reply("Workflow is not running")
    
    async def handle_all_messages(self, event):
        print(f"\n[DEBUG] Received message: {event.raw_text}", flush=True)
    
    async def init_and_run(self, bot_token, api_id, api_hash):
        workdir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
        os.makedirs(workdir, exist_ok=True)
        
        self.client = TelegramClient(
            os.path.join(workdir, "complete_bot"),
            api_id,
            api_hash
        )
        
        print("[DEBUG] Registering handlers...", flush=True)
        
        self.client.add_event_handler(self.handle_help, events.NewMessage(pattern='/help'))
        self.client.add_event_handler(self.handle_add_group, events.NewMessage(pattern='/add_group'))
        self.client.add_event_handler(self.handle_del_group, events.NewMessage(pattern='/del_group'))
        self.client.add_event_handler(self.handle_rename_group, events.NewMessage(pattern='/rename_group'))
        self.client.add_event_handler(self.handle_add_monitor, events.NewMessage(pattern='/add'))
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
        self.client.add_event_handler(self.handle_start_workflow, events.NewMessage(pattern='/start'))
        self.client.add_event_handler(self.handle_stop_workflow, events.NewMessage(pattern='/stop'))
        self.client.add_event_handler(self.handle_run_once, events.NewMessage(pattern='/run_once'))
        self.client.add_event_handler(self.handle_workflow_status, events.NewMessage(pattern='/workflow_status'))
        self.client.add_event_handler(self.handle_all_messages, events.NewMessage())
        
        print("[DEBUG] Starting client...", flush=True)
        
        await self.client.start(bot_token=bot_token)
        
        log_separator("✅ COMPLETE TELEGRAM BOT STARTED!")
        log("All commands available!")
        log("Send /help for help")
        
        await self.client.run_until_disconnected()

async def main():
    print("[DEBUG] Finding bot config...", flush=True)
    
    if not TELEGRAM_BOTS or not isinstance(TELEGRAM_BOTS, dict) or len(TELEGRAM_BOTS) == 0:
        log("ERROR: No TELEGRAM_BOTS configured!", "ERROR")
        return
    
    first_bot_name = list(TELEGRAM_BOTS.keys())[0]
    bot_config = TELEGRAM_BOTS[first_bot_name]
    bot_token = bot_config.get("bot_token") or bot_config.get("BOT_TOKEN")
    api_id = bot_config.get("api_id")
    api_hash = bot_config.get("api_hash")
    
    if not bot_token:
        log("ERROR: No bot_token found!", "ERROR")
        return
    
    if not api_id:
        api_id = 12345
        log("WARNING: Using default api_id=12345", "WARN")
    
    if not api_hash:
        api_hash = "0123456789abcdef0123456789abcdef"
        log("WARNING: Using default api_hash", "WARN")
    
    print(f"[DEBUG] Using bot: {first_bot_name}", flush=True)
    
    bot = CompleteTelegramBot()
    await bot.init_and_run(bot_token, api_id, api_hash)

if __name__ == "__main__":
    print("[DEBUG] === MAIN ===", flush=True)
    
    while True:
        try:
            import asyncio
            asyncio.run(main())
        except Exception as e:
            print(f"❌ ERROR: {e}", flush=True)
            traceback.print_exc()
            print("⏳ Restarting in 10s...", flush=True)
            time.sleep(10)
