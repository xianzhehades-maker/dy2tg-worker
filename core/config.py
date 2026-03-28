"""配置模块"""

import os
from pathlib import Path
from typing import Optional


PROJECT_ROOT = Path(__file__).parent.parent
DOWNLOAD_PATH = PROJECT_ROOT / "data" / "downloads"
CONFIG_PATH = PROJECT_ROOT / "config"

USERAGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"

TIMEOUT = 30
RETRY = 3


class Config:
    def __init__(self):
        self.douyin_cookie = os.getenv("DOUYIN_COOKIE", "")
        self.tiktok_cookie = os.getenv("TIKTOK_COOKIE", "")
        self.bot_token = os.getenv("BOT_TOKEN", "")
        self.proxy = os.getenv("PROXY", "")

        self.check_interval = 300
        self.max_retries = 3
        self.download_path = str(DOWNLOAD_PATH)

        self.ai_caption_enabled = True
        self.ai_caption_style = "default"
        self.ai_caption_length = 200
        self.ai_caption_language = "chinese"

        self.caption_styles = {
            "default": "口播风格，长文案",
            "humor": "乐子风格，短文案",
            "none": "无AI文案",
            "bilingual": "中英双语",
        }

    def get(self, key: str, default=None):
        return getattr(self, key, default)


config = Config()


def get_config() -> Config:
    return config