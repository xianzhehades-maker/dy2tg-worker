"""Telethon Test Bot - Fixed Bot Token"""
import os
import sys
import time
import traceback
from datetime import datetime

print("="*60, flush=True)
print("🚀 TELETHON TEST BOT - FIXED", flush=True)
print("="*60, flush=True)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    print("[DEBUG] Importing config...", flush=True)
    import config
    print(f"[DEBUG] Config imported! TELEGRAM_BOTS = {config.TELEGRAM_BOTS}", flush=True)
    
    print("[DEBUG] Importing telethon...", flush=True)
    from telethon import TelegramClient, events
    print("[DEBUG] Telethon imported", flush=True)
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
    print(f"[DEBUG] Token: {bot_token[:20]}...", flush=True)
    print(f"[DEBUG] API ID: {api_id}", flush=True)
    print(f"[DEBUG] API Hash: {api_hash[:10]}...", flush=True)
    
    workdir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
    print(f"[DEBUG] Work dir: {workdir}", flush=True)
    os.makedirs(workdir, exist_ok=True)
    
    print("[DEBUG] Creating TelegramClient...", flush=True)
    
    client = TelegramClient(
        os.path.join(workdir, "telethon_bot"),
        api_id,
        api_hash
    )
    
    print("[DEBUG] Registering handlers...", flush=True)
    
    @client.on(events.NewMessage(pattern='/help'))
    async def help_cmd(event):
        print("[DEBUG] /help received!", flush=True)
        await event.reply("🤖 Telethon Bot Help!\n/help - This message\n/start - Check status")
    
    @client.on(events.NewMessage(pattern='/start'))
    async def start_cmd(event):
        print("[DEBUG] /start received!", flush=True)
        await event.reply("✅ Telethon Bot is running!")
    
    @client.on(events.NewMessage())
    async def all_msg(event):
        print(f"\n[DEBUG] MSG: {event.message.text}", flush=True)
    
    print("[DEBUG] Starting client with bot token...", flush=True)
    
    await client.start(bot_token=bot_token)
    
    log("✅ TELETHON BOT STARTED!")
    log("Send /start or /help")
    
    print("[DEBUG] Running until disconnected...", flush=True)
    await client.run_until_disconnected()

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
