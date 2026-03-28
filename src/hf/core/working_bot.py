"""Working Telegram Bot - Full Version with Proxy Support"""
import os
import sys
import time
import asyncio
import traceback
from datetime import datetime

print("="*60, flush=True)
print("🚀 WORKING TELEGRAM BOT STARTING", flush=True)
print("="*60, flush=True)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    print("[DEBUG] Importing config...", flush=True)
    import config
    print(f"[DEBUG] Config imported! TELEGRAM_BOTS = {config.TELEGRAM_BOTS}", flush=True)
    print(f"[DEBUG] TELEGRAM_BOTS type = {type(config.TELEGRAM_BOTS)}", flush=True)
    print(f"[DEBUG] PROXY_URL = {config.PROXY_URL}", flush=True)
    
    print("[DEBUG] Importing database...", flush=True)
    from database import DatabaseManager
    print("[DEBUG] Database imported", flush=True)
    
    print("[DEBUG] Importing telegram...", flush=True)
    from telegram import Update
    from telegram.ext import (
        ApplicationBuilder,
        CommandHandler,
        MessageHandler,
        filters,
        ContextTypes
    )
    print("[DEBUG] Telegram imported", flush=True)
except Exception as e:
    print(f"[DEBUG] Import error: {e}", flush=True)
    traceback.print_exc()
    time.sleep(10)
    sys.exit(1)

TELEGRAM_BOTS = config.TELEGRAM_BOTS
PROXY_URL = config.PROXY_URL

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

class WorkingTelegramBot:
    """Working Telegram Bot"""
    
    def __init__(self):
        self.application = None
        self.bot_token = None
        self.db_manager = DatabaseManager()
        print("[DEBUG] WorkingTelegramBot initialized", flush=True)
    
    async def init_bot(self, bot_config):
        """Initialize Bot"""
        print("[DEBUG] === init_bot starting ===", flush=True)
        try:
            bot_token = bot_config.get("bot_token") or bot_config.get("BOT_TOKEN")
            
            print(f"[DEBUG] Bot token: {bot_token[:20] if bot_token else None}...", flush=True)
            print(f"[DEBUG] Proxy URL: {PROXY_URL}", flush=True)
            
            if not bot_token:
                log("Bot config incomplete - missing bot_token", "ERROR")
                return False
            
            log("Initializing Telegram Bot...")
            
            print("[DEBUG] Creating ApplicationBuilder...", flush=True)
            builder = ApplicationBuilder().token(bot_token)
            
            if PROXY_URL and PROXY_URL != 'system':
                print(f"[DEBUG] Using proxy: {PROXY_URL}", flush=True)
                builder = builder.proxy_url(PROXY_URL)
            
            self.application = builder.build()
            self.bot_token = bot_token
            
            print("[DEBUG] Registering handlers...", flush=True)
            self.register_handlers()
            
            log("Telegram Bot initialized successfully")
            print("[DEBUG] === init_bot completed ===", flush=True)
            return True
            
        except Exception as e:
            print(f"[DEBUG] Bot initialization failed: {e}", flush=True)
            traceback.print_exc()
            return False
    
    def register_handlers(self):
        """Register all command handlers"""
        print("[DEBUG] Registering /help handler...", flush=True)
        self.application.add_handler(CommandHandler("help", self.handle_help))
        
        print("[DEBUG] Registering /start handler...", flush=True)
        self.application.add_handler(CommandHandler("start", self.handle_start))
        
        print("[DEBUG] Registering catch-all message handler...", flush=True)
        self.application.add_handler(MessageHandler(filters.ALL, self.handle_all_messages))
        
        print("[DEBUG] All handlers registered!", flush=True)
    
    async def handle_all_messages(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle all messages for debugging"""
        print(f"\n[DEBUG] ========================================", flush=True)
        print(f"[DEBUG] Received message!", flush=True)
        if update.message:
            print(f"[DEBUG] Message text: {update.message.text}", flush=True)
            print(f"[DEBUG] From user: {update.effective_user.id if update.effective_user else 'unknown'}", flush=True)
            print(f"[DEBUG] Chat ID: {update.effective_chat.id if update.effective_chat else 'unknown'}", flush=True)
            print(f"[DEBUG] Message ID: {update.message.message_id}", flush=True)
        print(f"[DEBUG] ========================================\n", flush=True)
    
    async def handle_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        print("[DEBUG] === handle_start called ===", flush=True)
        try:
            await update.message.reply_text("✅ Bot is running! Send /help for commands.")
            print("[DEBUG] Start reply sent", flush=True)
        except Exception as e:
            print(f"[DEBUG] Error sending start reply: {e}", flush=True)
            traceback.print_exc()
    
    async def handle_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        print("[DEBUG] === handle_help called ===", flush=True)
        help_text = """
🤖 Bot Help!

Available commands:
  /start - Check if bot is running
  /help - Show this help message

More commands coming soon!
        """
        print("[DEBUG] Sending help message...", flush=True)
        try:
            await update.message.reply_text(help_text.strip())
            print("[DEBUG] Help message sent successfully", flush=True)
        except Exception as e:
            print(f"[DEBUG] Error sending help message: {e}", flush=True)
            traceback.print_exc()
    
    async def run(self):
        """Run the bot"""
        print("[DEBUG] === Bot.run() starting ===", flush=True)
        try:
            print("[DEBUG] Starting polling...", flush=True)
            
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            
            log_separator("✅ Working Telegram Bot Started!")
            log("Bot is running and ready to receive commands!")
            log("Send /start or /help to test!")
            if PROXY_URL and PROXY_URL != 'system':
                log(f"Using proxy: {PROXY_URL}")
            print("[DEBUG] === Bot running ===", flush=True)
            
            while True:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            log("\nReceived stop signal, shutting down...", "WARN")
        except Exception as e:
            print(f"[DEBUG] Run error: {e}", flush=True)
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
    print("[DEBUG] === main_async starting ===", flush=True)
    try:
        log_separator("Working Telegram Bot")
        log("Initializing...")
        
        bot = WorkingTelegramBot()
        
        if not await bot.init_bot(bot_config):
            log("Bot initialization failed, exiting", "ERROR")
            return
        
        await bot.run()
        
    except Exception as e:
        print(f"[DEBUG] Main async error: {e}", flush=True)
        traceback.print_exc()

if __name__ == "__main__":
    print("[DEBUG] === __main__ starting ===", flush=True)
    
    while True:
        try:
            print("[DEBUG] Checking TELEGRAM_BOTS...", flush=True)
            print(f"[DEBUG] TELEGRAM_BOTS = {TELEGRAM_BOTS}", flush=True)
            print(f"[DEBUG] TELEGRAM_BOTS is dict? {isinstance(TELEGRAM_BOTS, dict)}", flush=True)
            
            if TELEGRAM_BOTS and isinstance(TELEGRAM_BOTS, dict) and len(TELEGRAM_BOTS) > 0:
                print(f"[DEBUG] TELEGRAM_BOTS is not empty and is a dict", flush=True)
                first_bot_name = list(TELEGRAM_BOTS.keys())[0]
                bot_config = TELEGRAM_BOTS[first_bot_name]
                print(f"✅ Config loaded successfully: {first_bot_name}", flush=True)
                print(f"   Bot token: {bot_config.get('bot_token', '')[:20]}...", flush=True)
                
                print("[DEBUG] Calling asyncio.run...", flush=True)
                asyncio.run(main_async(bot_config))
            else:
                log("No Telegram Bot configured!", "ERROR")
                print(f"[DEBUG] TELEGRAM_BOTS was invalid: {TELEGRAM_BOTS}", flush=True)
                time.sleep(10)
                
        except Exception as e:
            print(f"❌ Main process exception: {e}", flush=True)
            traceback.print_exc()
            print("⏳ Restarting in 10 seconds...", flush=True)
            time.sleep(10)
