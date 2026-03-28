"""TEST BOT - Ultra Simple Version"""
import os
import sys
import time

print("="*60, flush=True)
print("🚀 TEST BOT STARTING", flush=True)
print("="*60, flush=True)
print(f"[DEBUG] Python version: {sys.version}", flush=True)
print(f"[DEBUG] Current dir: {os.getcwd()}", flush=True)
print(f"[DEBUG] Files in current dir: {os.listdir('.')}", flush=True)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(f"[DEBUG] Python path: {sys.path}", flush=True)

print("[DEBUG] Sleeping for 5 seconds...", flush=True)
time.sleep(5)

try:
    print("[DEBUG] Trying to import config...", flush=True)
    import config
    print(f"[DEBUG] Config imported! TELEGRAM_BOTS = {config.TELEGRAM_BOTS}", flush=True)
except Exception as e:
    print(f"[DEBUG] Config import error: {e}", flush=True)
    import traceback
    traceback.print_exc()

print("[DEBUG] Sleeping forever...", flush=True)
while True:
    print(f"[DEBUG] Still alive at {time.ctime()}", flush=True)
    time.sleep(10)
