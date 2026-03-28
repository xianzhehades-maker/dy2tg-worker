"""Pyrogram Test Bot"""
import os
import sys
import time
import traceback
from datetime import datetime

print("="*60, flush=True)
print("🚀 PYROGRAM TEST BOT", flush=True)
print("="*60, flush=True)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    print("[DEBUG] Importing config...", flush=True)
    import config
    print(f"[DEBUG] Config imported! TELEGRAM_BOTS = {config.TELEGRAM_BOTS}", flush=True)
    
    print("[DEBUG] Importing pyrogram...", flush=True)
    from pyrogram import Client, filters
    from pyrogram.types import Message
    print("[DEBUG] Pyrogram imported", flush=True)
except Exception as e:
    print(f"[DEBUG] Import error: {e}", flush=True)
    traceback.print_exc()
    time.sleep(10)
    sys.exit(1)

TELEGRAM_BOTS = config.TELEGRAM_BOTS

def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}", flush=True)

def main():
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
    
    print(f"[DEBUG] Using bot: {first_bot_name}", flush=True)
    print(f"[DEBUG] Token: {bot_token[:20]}...", flush=True)
    print(f"[DEBUG] API ID: {api_id}", flush=True)
    print(f"[DEBUG] API Hash: {api_hash[:10] if api_hash else None}...", flush=True)
    
    workdir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
    print(f"[DEBUG] Work dir: {workdir}", flush=True)
    
    print("[DEBUG] Creating Client...", flush=True)
    
    app = Client(
        "test_bot",
        bot_token=bot_token,
        api_id=api_id if api_id else 12345,
        api_hash=api_hash if api_hash else "0123456789abcdef0123456789abcdef",
        in_memory=True,
        workdir=workdir
    )
    
    print("[DEBUG] Registering handlers...", flush=True)
    
    @app.on_message(filters.command("help"))
    async def help_cmd(client, message: Message):
        print("[DEBUG] /help received!", flush=True)
        await message.reply_text("🤖 Pyrogram Bot Help!\n/help - This message\n/start - Check status")
    
    @app.on_message(filters.command("start"))
    async def start_cmd(client, message: Message):
        print("[DEBUG] /start received!", flush=True)
        await message.reply_text("✅ Pyrogram Bot is running!")
    
    @app.on_message(filters.text)
    async def all_txt(client, message: Message):
        print(f"\n[DEBUG] TEXT: {message.text}", flush=True)
    
    @app.on_message(filters.all)
    async def all_msg(client, message: Message):
        print(f"\n[DEBUG] ANY MSG: {message}", flush=True)
    
    print("[DEBUG] Starting app...", flush=True)
    
    log("✅ PYROGRAM BOT STARTING...")
    app.run()

if __name__ == "__main__":
    print("[DEBUG] === MAIN ===", flush=True)
    while True:
        try:
            main()
        except Exception as e:
            print(f"❌ ERROR: {e}", flush=True)
            traceback.print_exc()
            print("⏳ Restarting in 10s...", flush=True)
            time.sleep(10)
