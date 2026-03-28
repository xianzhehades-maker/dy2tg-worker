"""Simple Bot - Try Different Init"""
import os
import sys
import time
import asyncio
import traceback
from datetime import datetime

print("="*60, flush=True)
print("🚀 SIMPLE BOT - DIFFERENT INIT", flush=True)
print("="*60, flush=True)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    print("[DEBUG] Importing config...", flush=True)
    import config
    print(f"[DEBUG] Config imported! TELEGRAM_BOTS = {config.TELEGRAM_BOTS}", flush=True)
    
    print("[DEBUG] Importing telegram...", flush=True)
    from telegram import Update, Bot
    from telegram.ext import (
        Application,
        ApplicationBuilder,
        CommandHandler,
        MessageHandler,
        filters,
        ContextTypes,
        Updater
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

async def main():
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
    print(f"[DEBUG] Token: {bot_token[:20]}...", flush=True)
    
    print("[DEBUG] Creating Application...", flush=True)
    
    application = ApplicationBuilder().token(bot_token).build()
    
    print("[DEBUG] Registering handlers...", flush=True)
    
    async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        print("[DEBUG] /help received!", flush=True)
        await update.message.reply_text("🤖 Bot Help!\n/help - This message\n/start - Check status")
    
    async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        print("[DEBUG] /start received!", flush=True)
        await update.message.reply_text("✅ Bot is running!")
    
    async def all_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        print(f"\n[DEBUG] MSG: {update.message.text if update.message else 'None'}", flush=True)
    
    application.add_handler(CommandHandler("help", help_handler))
    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(MessageHandler(filters.ALL, all_handler))
    
    print("[DEBUG] Starting polling WITHOUT initialize()...", flush=True)
    
    await application.start()
    await application.updater.start_polling()
    
    log("✅ BOT STARTED!")
    log("Send /start or /help")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        log("Stopping...")
    finally:
        await application.updater.stop()
        await application.stop()

if __name__ == "__main__":
    print("[DEBUG] === STARTING ===", flush=True)
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            print(f"❌ ERROR: {e}", flush=True)
            traceback.print_exc()
            print("⏳ Restarting in 10s...", flush=True)
            time.sleep(10)
