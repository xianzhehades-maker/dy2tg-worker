"""Ultra Simple Telegram Bot - No Proxy"""
import os
import sys
import time
import asyncio
import traceback
from datetime import datetime

print("="*60, flush=True)
print("🚀 ULTRA SIMPLE TELEGRAM BOT STARTING", flush=True)
print("="*60, flush=True)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    print("[DEBUG] Importing config...", flush=True)
    import config
    print(f"[DEBUG] Config imported! TELEGRAM_BOTS = {config.TELEGRAM_BOTS}", flush=True)
    
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

def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}", flush=True)

class UltraSimpleBot:
    def __init__(self):
        self.application = None
    
    async def init_and_run(self, bot_token):
        print(f"[DEBUG] Creating bot with token: {bot_token[:20]}...", flush=True)
        
        self.application = ApplicationBuilder().token(bot_token).build()
        
        print("[DEBUG] Registering handlers...", flush=True)
        
        async def handle_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
            print("[DEBUG] /help called!", flush=True)
            await update.message.reply_text("🤖 Bot is working!\n/help - This message\n/start - Check status")
        
        async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
            print("[DEBUG] /start called!", flush=True)
            await update.message.reply_text("✅ Bot is running perfectly!")
        
        async def handle_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
            print(f"\n[DEBUG] Got message: {update.message.text if update.message else 'None'}", flush=True)
        
        self.application.add_handler(CommandHandler("help", handle_help))
        self.application.add_handler(CommandHandler("start", handle_start))
        self.application.add_handler(MessageHandler(filters.ALL, handle_all))
        
        print("[DEBUG] Starting bot...", flush=True)
        
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        
        log("✅ ULTRA SIMPLE BOT STARTED!")
        log("Send /start or /help to test!")
        
        while True:
            await asyncio.sleep(1)

async def main_async():
    print("[DEBUG] Finding bot token...", flush=True)
    
    if not TELEGRAM_BOTS or not isinstance(TELEGRAM_BOTS, dict) or len(TELEGRAM_BOTS) == 0:
        log("ERROR: No TELEGRAM_BOTS configured!", "ERROR")
        return
    
    first_bot_name = list(TELEGRAM_BOTS.keys())[0]
    bot_config = TELEGRAM_BOTS[first_bot_name]
    bot_token = bot_config.get("bot_token") or bot_config.get("BOT_TOKEN")
    
    if not bot_token:
        log("ERROR: No bot_token found!", "ERROR")
        return
    
    print(f"[DEBUG] Using bot: {first_bot_name}", flush=True)
    
    bot = UltraSimpleBot()
    await bot.init_and_run(bot_token)

if __name__ == "__main__":
    print("[DEBUG] === MAIN ===", flush=True)
    while True:
        try:
            asyncio.run(main_async())
        except Exception as e:
            print(f"❌ ERROR: {e}", flush=True)
            traceback.print_exc()
            print("⏳ Restarting in 10 seconds...", flush=True)
            time.sleep(10)
