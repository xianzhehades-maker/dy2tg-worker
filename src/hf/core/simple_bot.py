"""Simple Telegram Bot - Minimal Version with Full Debug Logging"""
import os
import sys
import time
import subprocess
import asyncio
import traceback
from datetime import datetime

print("="*60)
print("🚀 Starting Simple Telegram Bot")
print("="*60)
print(f"[DEBUG] Python path: {sys.path}")
print(f"[DEBUG] Current dir: {os.getcwd()}")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(f"[DEBUG] Python path after insert: {sys.path}")

try:
    print("[DEBUG] Importing config...")
    import config
    print(f"[DEBUG] Config imported")
    
    print("[DEBUG] Importing database...")
    from database import DatabaseManager
    from hf.database.models import MonitorGroup, GroupMonitor, GroupTarget
    print("[DEBUG] Database imported")
    
    print("[DEBUG] Importing telegram...")
    from telegram import Update, Bot
    from telegram.ext import (
        ApplicationBuilder,
        CommandHandler,
        MessageHandler,
        filters,
        ContextTypes
    )
    print("[DEBUG] Telegram imported")
except Exception as e:
    print(f"[DEBUG] Import error: {e}")
    traceback.print_exc()
    time.sleep(10)
    sys.exit(1)

DOWNLOAD_PATH = config.DOWNLOAD_PATH
PROCESSED_PATH = config.PROCESSED_PATH
UPLOAD_PATH = config.UPLOAD_PATH

MAX_RETRIES = config.MAX_RETRIES
RETRY_DELAY = config.RETRY_DELAY

TELEGRAM_BOTS = config.TELEGRAM_BOTS

print(f"[DEBUG] TELEGRAM_BOTS: {TELEGRAM_BOTS}")

def log(message, level="INFO"):
    """Simple logging output - flush immediately"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}", flush=True)

def log_separator(title=None):
    """Output separator line"""
    if title:
        print(f"\n{'='*60}", flush=True)
        print(f"  {title}", flush=True)
        print(f"{'='*60}\n", flush=True)
    else:
        print(f"\n{'='*60}\n", flush=True)

class SimpleTelegramBot:
    """Simple Telegram Bot with Command Handling"""
    
    def __init__(self):
        self.application = None
        self.bot_token = None
        self.chat_id = None
        self.db_manager = DatabaseManager()
        print("[DEBUG] SimpleTelegramBot initialized")
    
    async def init_bot(self, bot_config):
        """Initialize Bot"""
        print("[DEBUG] === init_bot starting ===")
        try:
            bot_token = bot_config.get("bot_token") or bot_config.get("BOT_TOKEN")
            chat_id = bot_config.get("chat_id") or bot_config.get("CHAT_ID")
            
            print(f"[DEBUG] Bot token: {bot_token[:20] if bot_token else None}...")
            print(f"[DEBUG] Chat ID: {chat_id}")
            
            if not bot_token:
                log("Bot config incomplete - missing bot_token", "ERROR")
                return False
            
            log("Initializing Telegram Bot...")
            
            print("[DEBUG] Creating ApplicationBuilder...")
            self.application = ApplicationBuilder().token(bot_token).build()
            self.bot_token = bot_token
            self.chat_id = chat_id
            
            print("[DEBUG] Registering handlers...")
            self.register_handlers()
            
            log("Telegram Bot initialized successfully")
            print("[DEBUG] === init_bot completed ===")
            return True
            
        except Exception as e:
            print(f"[DEBUG] Bot initialization failed: {e}")
            traceback.print_exc()
            return False
    
    def register_handlers(self):
        """Register all command handlers"""
        print("[DEBUG] Registering /help handler...")
        self.application.add_handler(CommandHandler("help", self.handle_help))
        
        print("[DEBUG] Registering /start handler...")
        self.application.add_handler(CommandHandler("start", self.handle_start))
        
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
        print(f"[DEBUG] ========================================\n", flush=True)
    
    async def handle_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        print("[DEBUG] === handle_start called ===")
        try:
            await update.message.reply_text("Bot is running! Send /help for commands.")
            print("[DEBUG] Start reply sent")
        except Exception as e:
            print(f"[DEBUG] Error sending start reply: {e}")
            traceback.print_exc()
    
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

System Commands:
  /status - View all groups status
  /queue - View current queue
  /list_errors - List all failed tasks
  /retry <task_id|all> - Retry failed task(s)
  /skip <task_id> - Skip a task
  /help - Show this help message
        """
        print("[DEBUG] Sending help message...")
        try:
            await update.message.reply_text(help_text.strip())
            print("[DEBUG] Help message sent successfully")
        except Exception as e:
            print(f"[DEBUG] Error sending help message: {e}")
            traceback.print_exc()
    
    async def run(self):
        """Run the bot"""
        print("[DEBUG] === Bot.run() starting ===")
        try:
            print("[DEBUG] Starting polling...")
            
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            
            log_separator("Simple Telegram Bot Started")
            log("✅ Bot is running and ready to receive commands!")
            log("📝 Send /help for available commands")
            print("[DEBUG] === Bot running ===")
            
            while True:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            log("\nReceived stop signal, shutting down...", "WARN")
        except Exception as e:
            print(f"[DEBUG] Run error: {e}")
            traceback.print_exc()
        finally:
            try:
                if self.application.updater:
                    await self.application.updater.stop()
                await self.application.stop()
                await self.application.shutdown()
            except:
                pass
            log("Bot stopped")

async def main_async(bot_config):
    """Main async function"""
    print("[DEBUG] === main_async starting ===")
    try:
        log_separator("Simple Telegram Bot (python-telegram-bot)")
        log("Initializing...")
        
        bot = SimpleTelegramBot()
        
        if not await bot.init_bot(bot_config):
            log("Bot initialization failed, exiting", "ERROR")
            return
        
        await bot.run()
        
    except Exception as e:
        print(f"[DEBUG] Main async error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    print("[DEBUG] === __main__ starting ===")
    
    while True:
        try:
            print("[DEBUG] Checking TELEGRAM_BOTS...")
            if TELEGRAM_BOTS and len(TELEGRAM_BOTS) > 0:
                print(f"[DEBUG] TELEGRAM_BOTS is not empty")
                first_bot_name = list(TELEGRAM_BOTS.keys())[0]
                bot_config = TELEGRAM_BOTS[first_bot_name]
                print(f"✅ Config loaded successfully: {first_bot_name}")
                print(f"   Chat ID: {bot_config.get('chat_id')}")
                
                print("[DEBUG] Calling asyncio.run...")
                asyncio.run(main_async(bot_config))
            else:
                log("No Telegram Bot configured!", "ERROR")
                print(f"[DEBUG] TELEGRAM_BOTS was empty: {TELEGRAM_BOTS}")
                time.sleep(10)
                
        except Exception as e:
            print(f"❌ Main process exception: {e}", flush=True)
            traceback.print_exc()
            print("⏳ Restarting in 10 seconds...", flush=True)
            time.sleep(10)
