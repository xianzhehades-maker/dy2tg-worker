"""Complete Telegram Bot with Command Handling and Upload - using python-telegram-bot"""
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
    from hf.database.models import MonitorGroup, GroupMonitor, GroupTarget
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

    def log_separator(title=None):
        """Output separator line"""
        if title:
            print(f"\n{'='*60}")
            print(f"  {title}")
            print(f"{'='*60}")
        else:
            print(f"\n{'='*60}")

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


    class CompleteTelegramBot:
        """Complete Telegram Bot - Command Handling + Upload"""
        
        def __init__(self):
            self.application = None
            self.bot_token = None
            self.chat_id = None
            self.db_manager = DatabaseManager()
            self.is_group_mode = False
            self.current_targets = []
            self.workflow_process = None
        
        async def init_bot(self, bot_config):
            """Initialize Bot with python-telegram-bot"""
            print("[DEBUG] === init_bot starting ===")
            try:
                bot_token = bot_config.get("bot_token") or bot_config.get("BOT_TOKEN")
                chat_id = bot_config.get("chat_id") or bot_config.get("CHAT_ID")
                
                print(f"[DEBUG] Bot token: {bot_token[:10] if bot_token else None}...")
                print(f"[DEBUG] Chat ID: {chat_id}")
                
                if not bot_token:
                    log("Bot config incomplete - missing bot_token", "ERROR")
                    return False
                
                log("Initializing Telegram Bot (python-telegram-bot)...")
                
                from telegram import Update, Bot
                from telegram.ext import (
                    ApplicationBuilder,
                    CommandHandler,
                    MessageHandler,
                    filters,
                    ContextTypes
                )
                
                print("[DEBUG] Creating ApplicationBuilder...")
                self.application = ApplicationBuilder().token(bot_token).build()
                self.bot_token = bot_token
                self.chat_id = chat_id
                
                self.is_group_mode = not chat_id
                
                if self.is_group_mode:
                    log("Group mode enabled - will select target channel based on task group")
                else:
                    log("Simple mode enabled - will use fixed target channel")
                
                print("[DEBUG] Registering command handlers...")
                self.register_handlers()
                
                log("Telegram Bot initialized successfully")
                print("[DEBUG] === init_bot completed ===")
                return True
                
            except Exception as e:
                print(f"[DEBUG] Bot initialization failed: {e}")
                log(f"Bot initialization failed: {e}", "ERROR")
                traceback.print_exc()
                return False
        
        def register_handlers(self):
            """Register all command handlers"""
            print("[DEBUG] Registering /help handler...")
            self.application.add_handler(CommandHandler("help", self.handle_help))
            
            print("[DEBUG] Registering /add_group handler...")
            self.application.add_handler(CommandHandler("add_group", self.handle_add_group))
            
            print("[DEBUG] Registering /del_group handler...")
            self.application.add_handler(CommandHandler("del_group", self.handle_del_group))
            
            print("[DEBUG] Registering /rename_group handler...")
            self.application.add_handler(CommandHandler("rename_group", self.handle_rename_group))
            
            print("[DEBUG] Registering /add handler...")
            self.application.add_handler(CommandHandler("add", self.handle_add_monitor))
            
            print("[DEBUG] Registering /del_monitor handler...")
            self.application.add_handler(CommandHandler("del_monitor", self.handle_del_monitor))
            
            print("[DEBUG] Registering /add_target handler...")
            self.application.add_handler(CommandHandler("add_target", self.handle_add_target))
            
            print("[DEBUG] Registering /del_target handler...")
            self.application.add_handler(CommandHandler("del_target", self.handle_del_target))
            
            print("[DEBUG] Registering /set_promotion handler...")
            self.application.add_handler(CommandHandler("set_promotion", self.handle_set_promotion))

            print("[DEBUG] Registering /set_caption_style handler...")
            self.application.add_handler(CommandHandler("set_caption_style", self.handle_set_caption_style))
            
            print("[DEBUG] Registering /status handler...")
            self.application.add_handler(CommandHandler("status", self.handle_status))
            
            print("[DEBUG] Registering /queue handler...")
            self.application.add_handler(CommandHandler("queue", self.handle_queue_status))

            print("[DEBUG] Registering /list_errors handler...")
            self.application.add_handler(CommandHandler("list_errors", self.handle_list_errors))

            print("[DEBUG] Registering /retry handler...")
            self.application.add_handler(CommandHandler("retry", self.handle_retry_task))

            print("[DEBUG] Registering /skip handler...")
            self.application.add_handler(CommandHandler("skip", self.handle_skip_task))

            print("[DEBUG] Registering /sync handler...")
            self.application.add_handler(CommandHandler("sync", self.handle_sync))
            
            print("[DEBUG] Registering /sync_ids handler...")
            self.application.add_handler(CommandHandler("sync_ids", self.handle_sync_ids))
            
            print("[DEBUG] Registering /clear_cache handler...")
            self.application.add_handler(CommandHandler("clear_cache", self.handle_clear_cache))
            
            print("[DEBUG] Registering /time handler...")
            self.application.add_handler(CommandHandler("time", self.handle_set_time))
            
            print("[DEBUG] Registering /start handler...")
            self.application.add_handler(CommandHandler("start", self.handle_start_workflow))
            
            print("[DEBUG] Registering /stop handler...")
            self.application.add_handler(CommandHandler("stop", self.handle_stop_workflow))
            
            print("[DEBUG] Registering /run_once handler...")
            self.application.add_handler(CommandHandler("run_once", self.handle_run_once))
            
            print("[DEBUG] Registering /workflow_status handler...")
            self.application.add_handler(CommandHandler("workflow_status", self.handle_workflow_status))
            
            print("[DEBUG] Registering catch-all message handler...")
            self.application.add_handler(MessageHandler(filters.ALL, self.handle_all_messages))
            
            print("[DEBUG] All handlers registered!")
        
        async def handle_all_messages(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle all messages for debugging"""
            print(f"\n[DEBUG] ========================================")
            print(f"[DEBUG] Received message!")
            if update.message:
                print(f"[DEBUG] Message text: {update.message.text}")
                print(f"[DEBUG] From user: {update.effective_user.id if update.effective_user else 'unknown'}")
                print(f"[DEBUG] Chat ID: {update.effective_chat.id if update.effective_chat else 'unknown'}")
                print(f"[DEBUG] Message ID: {update.message.message_id}")
            print(f"[DEBUG] ========================================\n")
        
        async def handle_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /help command"""
            print("[DEBUG] === handle_help called ===")
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
  /set_caption_style group_id style - Set caption style
    - 口播/default: AI优化长文案 (口播视频)
    - humor/short/乐子: 20字以内短文案 (段子视频)
    - none/舞蹈/null: 不使用AI文案 (舞蹈视频)

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
            print("[DEBUG] Sending help message...")
            try:
                await update.message.reply_text(help_text.strip())
                print("[DEBUG] Help message sent successfully")
            except Exception as e:
                print(f"[DEBUG] Error sending help message: {e}")
                traceback.print_exc()
        
        async def handle_add_group(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /add_group command"""
            if not context.args or len(context.args) < 1:
                await update.message.reply_text("Error: Usage: /add_group group_name")
                return
            
            group_name = " ".join(context.args)
            group = MonitorGroup(name=group_name)
            group_id = self.db_manager.add_monitor_group(group)
            
            if group_id:
                await update.message.reply_text(f"OK! Group created!\nID: {group_id}\nName: {group_name}")
            else:
                await update.message.reply_text("Error: Group creation failed, maybe name exists")
        
        async def handle_del_group(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /del_group command"""
            if not context.args or len(context.args) < 1:
                await update.message.reply_text("Error: Usage: /del_group group_id")
                return
            
            try:
                group_id = int(context.args[0])
                group = self.db_manager.get_monitor_group(group_id)
                if not group:
                    await update.message.reply_text("Error: Group not found")
                    return
                
                self.db_manager.delete_monitor_group(group_id)
                await update.message.reply_text(f"OK! Group deleted!\nID: {group_id}\nName: {group.name}")
            except ValueError:
                await update.message.reply_text("Error: Group ID must be a number")
        
        async def handle_rename_group(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /rename_group command"""
            if not context.args or len(context.args) < 2:
                await update.message.reply_text("Error: Usage: /rename_group group_id new_name")
                return
            
            try:
                group_id = int(context.args[0])
                new_name = " ".join(context.args[1:])
                
                group = self.db_manager.get_monitor_group(group_id)
                if not group:
                    await update.message.reply_text("Error: Group not found")
                    return
                
                self.db_manager.update_monitor_group(group_id, name=new_name)
                await update.message.reply_text(f"OK! Group renamed!\nID: {group_id}\nOld: {group.name}\nNew: {new_name}")
            except ValueError:
                await update.message.reply_text("Error: Group ID must be a number")

        async def handle_set_caption_style(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /set_caption_style command"""
            if not context.args or len(context.args) < 2:
                await update.message.reply_text(
                    "Error: Usage: /set_caption_style group_id style\n\n"
                    "Styles:\n"
                    "  - default/口播: AI optimized caption (for talking videos)\n"
                    "  - humor/short: Short fun caption (for comedy videos)\n"
                    "  - none: No AI caption (for dance videos)"
                )
                return

            try:
                group_id = int(context.args[0])
                style = " ".join(context.args[1:]).lower()

                group = self.db_manager.get_monitor_group(group_id)
                if not group:
                    await update.message.reply_text("Error: Group not found")
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
                    await update.message.reply_text(f"Error: Unknown style '{style}'")
                    return

                self.db_manager.update_monitor_group(group_id, ai_caption_style=ai_caption_style)
                await update.message.reply_text(
                    f"OK! Group {group_id} caption style updated!\n"
                    f"Group: {group.name}\n"
                    f"Style: {style_name}"
                )
            except ValueError:
                await update.message.reply_text("Error: Group ID must be a number")

        def parse_monitor_list(self, text: str):
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
        
        async def handle_add_monitor(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /add command"""
            if not context.args or len(context.args) < 2:
                await update.message.reply_text("Error: Usage: /add group_id up1:url1,up2:url2")
                return
            
            try:
                group_id = int(context.args[0])
                monitor_text = " ".join(context.args[1:])
                
                group = self.db_manager.get_monitor_group(group_id)
                if not group:
                    await update.message.reply_text("Error: Group not found")
                    return
                
                monitors = self.parse_monitor_list(monitor_text)
                if not monitors:
                    await update.message.reply_text("Error: Cannot parse monitor list, check format")
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
                
                await update.message.reply_text(result)
            except ValueError:
                await update.message.reply_text("Error: Group ID must be a number")
        
        async def handle_del_monitor(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /del_monitor command"""
            if not context.args or len(context.args) < 2:
                await update.message.reply_text("Error: Usage: /del_monitor group_id up1:url1,up2:url2")
                return
            
            try:
                group_id = int(context.args[0])
                monitor_text = " ".join(context.args[1:])
                
                group = self.db_manager.get_monitor_group(group_id)
                if not group:
                    await update.message.reply_text("Error: Group not found")
                    return
                
                monitors = self.parse_monitor_list(monitor_text)
                if not monitors:
                    await update.message.reply_text("Error: Cannot parse monitor list, check format")
                    return
                
                deleted = []
                
                for name, url in monitors:
                    self.db_manager.delete_group_monitor_by_url(group_id, url)
                    deleted.append(f"{name}:{url}")
                
                result = f"OK! Deleted monitors from group '{group.name}'!\n\n"
                result += f"Deleted {len(deleted)}:\n" + "\n".join(deleted)
                
                await update.message.reply_text(result)
            except ValueError:
                await update.message.reply_text("Error: Group ID must be a number")
        
        async def handle_add_target(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /add_target command"""
            if not context.args or len(context.args) < 2:
                await update.message.reply_text("Error: Usage: /add_target group_id @channel")
                return
            
            try:
                group_id = int(context.args[0])
                target_channel = context.args[1]
                
                group = self.db_manager.get_monitor_group(group_id)
                if not group:
                    await update.message.reply_text("Error: Group not found")
                    return
                
                target = GroupTarget(group_id=group_id, target_channel=target_channel)
                target_id = self.db_manager.add_group_target(target)
                
                if target_id:
                    await update.message.reply_text(f"OK! Target added!\nGroup: {group.name}\nTarget: {target_channel}")
                else:
                    await update.message.reply_text("Error: Target add failed, maybe already exists")
            except ValueError:
                await update.message.reply_text("Error: Group ID must be a number")
        
        async def handle_del_target(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /del_target command"""
            if not context.args or len(context.args) < 2:
                await update.message.reply_text("Error: Usage: /del_target group_id @channel")
                return
            
            try:
                group_id = int(context.args[0])
                target_channel = context.args[1]
                
                group = self.db_manager.get_monitor_group(group_id)
                if not group:
                    await update.message.reply_text("Error: Group not found")
                    return
                
                self.db_manager.delete_group_target_by_channel(group_id, target_channel)
                await update.message.reply_text(f"OK! Target deleted!\nGroup: {group.name}\nTarget: {target_channel}")
            except ValueError:
                await update.message.reply_text("Error: Group ID must be a number")
        
        async def handle_set_promotion(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /set_promotion command"""
            if not context.args or len(context.args) < 2:
                await update.message.reply_text("Error: Usage: /set_promotion group_id text")
                return
            
            try:
                group_id = int(context.args[0])
                promotion_text = " ".join(context.args[1:])
                
                group = self.db_manager.get_monitor_group(group_id)
                if not group:
                    await update.message.reply_text("Error: Group not found")
                    return
                
                self.db_manager.update_monitor_group(group_id, promotion_text=promotion_text)
                await update.message.reply_text(f"OK! Promotion text set!\nGroup: {group.name}\nText: {promotion_text}")
            except ValueError:
                await update.message.reply_text("Error: Group ID must be a number")
        
        async def handle_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /status command"""
            groups = self.db_manager.get_monitor_groups()
            
            if not groups:
                await update.message.reply_text("No groups configured")
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
            
            await update.message.reply_text(result)
        
        async def handle_sync(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /sync command"""
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
                
                await update.message.reply_text(f"OK! Config synced!\nSaved {len(groups)} groups\nFile: {config_path}")
            except Exception as e:
                await update.message.reply_text(f"Error: Config sync failed: {e}")
        
        async def handle_sync_ids(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /sync_ids command"""
            targets = self.db_manager.get_group_targets()
            updated = 0
            
            for target in targets:
                if not target.chat_id and target.target_channel.startswith("@"):
                    try:
                        chat = await context.bot.get_chat(target.target_channel)
                        if chat:
                            self.db_manager.update_group_target_chat_id(target.id, chat.id)
                            updated += 1
                    except Exception as e:
                        pass
            
            await update.message.reply_text(f"OK! chat_id synced!\nUpdated: {updated}\nTotal: {len(targets)}")
        
        async def handle_clear_cache(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /clear_cache command"""
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
                
                await update.message.reply_text(f"OK! Cache cleared!\nFiles cleared: {cleared}")
            except Exception as e:
                await update.message.reply_text(f"Error: Cache clear failed: {e}")
        
        async def handle_set_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /time command"""
            if not context.args or len(context.args) < 1:
                current = self.db_manager.get_system_config("check_interval", "3600")
                await update.message.reply_text(f"Current check interval: {current} seconds\n\nUsage: /time interval_seconds")
                return
            
            try:
                interval = int(context.args[0])
                if interval < 60:
                    await update.message.reply_text("Error: Interval must be at least 60 seconds")
                    return
                
                self.db_manager.set_system_config("check_interval", str(interval))
                await update.message.reply_text(f"OK! Check interval set to {interval} seconds")
            except ValueError:
                await update.message.reply_text("Error: Interval must be a number")
        
        async def handle_queue_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /queue command"""
            stats = self.db_manager.get_task_queue_stats()
            
            if not stats:
                await update.message.reply_text("Queue is empty")
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

            await update.message.reply_text(result)

        async def handle_list_errors(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /list_errors command - 查看失败任务"""
            error_statuses = ["error", "download_failed", "upload_failed", "api_error"]
            error_tasks = []

            for status in error_statuses:
                tasks = self.db_manager.get_tasks(status=status, limit=100)
                for task in tasks:
                    if task and task.error_msg:
                        error_tasks.append(task)

            if not error_tasks:
                await update.message.reply_text("✅ 没有失败任务")
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

            await update.message.reply_text(result)

        async def handle_retry_task(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /retry command - 重试失败任务"""
            if not context.args:
                await update.message.reply_text(
                    "Usage: /retry <task_id>\n"
                    "   or /retry all - 重试所有失败任务"
                )
                return

            arg = context.args[0]

            if arg.lower() == "all":
                error_statuses = ["error", "download_failed", "upload_failed", "api_error"]
                reset_count = 0
                for status in error_statuses:
                    tasks = self.db_manager.get_tasks(status=status, limit=1000)
                    for task in tasks:
                        if task:
                            self.db_manager.update_task(task.id, status="pending", error_msg=None)
                            reset_count += 1
                await update.message.reply_text(f"✅ 已重置 {reset_count} 个失败任务为 pending 状态")
                return

            try:
                task_id = int(arg)
                task = self.db_manager.get_task(task_id)
                if not task:
                    await update.message.reply_text(f"Error: Task {task_id} not found")
                    return

                self.db_manager.update_task(task_id, status="pending", error_msg=None)
                await update.message.reply_text(f"✅ Task {task_id} 已重置为 pending 状态")
            except ValueError:
                await update.message.reply_text("Error: Invalid task ID")

        async def handle_skip_task(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /skip command - 跳过任务"""
            if not context.args:
                await update.message.reply_text("Usage: /skip <task_id>")
                return

            try:
                task_id = int(context.args[0])
                task = self.db_manager.get_task(task_id)
                if not task:
                    await update.message.reply_text(f"Error: Task {task_id} not found")
                    return

                self.db_manager.update_task(task_id, status="skipped", error_msg="手动跳过")
                await update.message.reply_text(f"✅ Task {task_id} 已标记为 skipped")
            except ValueError:
                await update.message.reply_text("Error: Invalid task ID")

        async def handle_start_workflow(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /start command"""
            if self.workflow_process and self.workflow_process.poll() is None:
                await update.message.reply_text("Warning: Workflow already running")
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
                await update.message.reply_text("OK! Workflow started!")
            except Exception as e:
                await update.message.reply_text(f"Error: Start workflow failed: {e}")
        
        async def handle_stop_workflow(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /stop command"""
            if not self.workflow_process or self.workflow_process.poll() is not None:
                await update.message.reply_text("Warning: Workflow not running")
                return
            
            try:
                self.workflow_process.terminate()
                self.workflow_process.wait(timeout=10)
                await update.message.reply_text("OK! Workflow stopped")
            except subprocess.TimeoutExpired:
                self.workflow_process.kill()
                await update.message.reply_text("Warning: Workflow force stopped")
            except Exception as e:
                await update.message.reply_text(f"Error: Stop workflow failed: {e}")
        
        async def handle_run_once(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /run_once command"""
            if self.workflow_process and self.workflow_process.poll() is None:
                await update.message.reply_text("Warning: Workflow already running, stop first")
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
                    await update.message.reply_text("OK! Workflow run once completed!")
                else:
                    await update.message.reply_text(f"Warning: Workflow run once completed (exit code: {result.returncode})")
            except subprocess.TimeoutExpired:
                await update.message.reply_text("Error: Workflow run once timeout")
            except Exception as e:
                await update.message.reply_text(f"Error: Run workflow once failed: {e}")
        
        async def handle_workflow_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Handle /workflow_status command"""
            if self.workflow_process and self.workflow_process.poll() is None:
                await update.message.reply_text("Workflow is running")
            else:
                await update.message.reply_text("Workflow is not running")
        
        async def upload_file(self, file_path, task_id):
            """Upload file to Telegram"""
            if not self.application:
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
                            await self.application.bot.send_video(
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
        
        async def process_upload_tasks(self):
            """Process upload tasks in background"""
            try:
                db_manager = DatabaseManager()
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
                    task = tasks[0]
                    
                    upload_file = task.file_path
                    
                    if not upload_file:
                        filename = f"{task.id}_watermarked.mp4"
                        upload_file = os.path.join(PROCESSED_PATH, filename)
                        if not os.path.exists(upload_file):
                            filename = f"{task.id}.mp4"
                            upload_file = os.path.join(PROCESSED_PATH, filename)
                            if not os.path.exists(upload_file):
                                filename = f"{task.id}_watermarked.mp4"
                                upload_file = os.path.join(UPLOAD_PATH, filename)
                                if not os.path.exists(upload_file):
                                    filename = f"{task.id}.mp4"
                                    upload_file = os.path.join(UPLOAD_PATH, filename)
                    
                    if not upload_file or not os.path.exists(upload_file):
                        log(f"Task {task.id} file not found", "WARN")
                        db_manager.update_task(task.id, status="error", error_msg="File not found")
                        continue
                    
                    if upload_file.startswith('./'):
                        upload_file = os.path.abspath(upload_file)
                    
                    log_separator(f"Processing task {task.id}")
                    log(f"File: {os.path.basename(upload_file)}")
                    
                    success = await self.upload_with_retry(upload_file, task.id)
                    
                    if success:
                        db_manager.update_task(task.id, status="uploaded", upload_time=datetime.now())
                        log(f"Task {task.id} upload completed")
                    else:
                        db_manager.update_task(task.id, status="error", error_msg="Upload failed")
                        log(f"Task {task.id} upload failed", "ERROR")
                    
                    log("Taking a 5 second break before continuing...")
                    await asyncio.sleep(5)
            except Exception as e:
                log(f"Upload task processor error: {e}", "ERROR")
                traceback.print_exc()
        
        async def run(self):
            """Run the bot - command handling + upload"""
            print("[DEBUG] === Bot.run() starting ===")
            try:
                print("[DEBUG] Initializing application...")
                
                upload_task = asyncio.create_task(self.process_upload_tasks())
                
                print("[DEBUG] Starting polling...")
                await self.application.initialize()
                await self.application.start()
                await self.application.updater.start_polling()
                
                log_separator("Complete Telegram Bot Started")
                log("✅ Bot is running and ready to receive commands!")
                log("📝 Send /help for available commands")
                log("📤 Upload processor running in background")
                print("[DEBUG] === Bot running ===")
                
                await upload_task
                
            except KeyboardInterrupt:
                log("\nReceived stop signal, shutting down...", "WARN")
            finally:
                if self.application.updater:
                    await self.application.updater.stop()
                await self.application.stop()
                await self.application.shutdown()
                log("Bot stopped")


    async def main_async(bot_config):
        """Main async function"""
        try:
            log_separator("Complete Telegram Bot (python-telegram-bot)")
            log("Initializing...")
            
            bot = CompleteTelegramBot()
            
            if not await bot.init_bot(bot_config):
                log("Bot initialization failed, exiting", "ERROR")
                return
            
            await bot.run()
            
        except Exception as e:
            log(f"Main process error: {e}", "ERROR")
            traceback.print_exc()


    print("="*60)
    print("🚀 Starting Complete Telegram Bot")
    print("="*60)
    
    if TELEGRAM_BOTS and len(TELEGRAM_BOTS) > 0:
        print(f"[DEBUG] TELEGRAM_BOTS: {TELEGRAM_BOTS}")
        first_bot_name = list(TELEGRAM_BOTS.keys())[0]
        bot_config = TELEGRAM_BOTS[first_bot_name]
        print(f"✅ Config loaded successfully: {first_bot_name}")
        print(f"   Chat ID: {bot_config.get('chat_id')}")
        asyncio.run(main_async(bot_config))
    else:
        log("No Telegram Bot configured!", "ERROR")
        time.sleep(10)


if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(f"❌ Main process exception: {e}")
            traceback.print_exc()
            print("⏳ Restarting in 10 seconds...")
            time.sleep(10)
