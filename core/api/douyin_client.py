"""轻量级抖音 API 客户端 - 基于 TikTokDownloader 项目"""

import re
import json
import asyncio
import logging
from typing import Dict, List, Optional, Any

import httpx

logger = logging.getLogger("douyin_api_client")


def async_retry(max_attempts: int = 3, delay: float = 1.0):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(delay * (attempt + 1))
            raise last_exception
        return wrapper
    return decorator


class DouyinAPIClient:
    """轻量级抖音 API 客户端"""

    DOMAIN = "https://www.douyin.com"
    VIDEO_DETAIL_API = f"{DOMAIN}/aweme/v1/web/aweme/detail/"
    USER_AWEME_API = f"{DOMAIN}/aweme/v1/web/aweme/post/"

    DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Referer": DOMAIN,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }

    BASE_PARAMS = {
        "device_platform": "webapp",
        "aid": "6383",
        "channel": "channel_pc_web",
        "update_version_code": "170400",
        "pc_client_type": "1",
        "pc_libra_divert": "Windows",
        "support_h265": "1",
        "support_dash": "1",
        "version_code": "290100",
        "version_name": "29.1.0",
        "cookie_enabled": "true",
        "screen_width": "1536",
        "screen_height": "864",
        "browser_language": "zh-CN",
        "browser_platform": "Win32",
        "browser_name": "Chrome",
        "browser_version": "139.0.0.0",
        "browser_online": "true",
        "engine_name": "Blink",
        "engine_version": "139.0.0.0",
        "os_name": "Windows",
        "os_version": "10",
        "cpu_core_num": "16",
        "device_memory": "8",
        "platform": "PC",
        "downlink": "10",
        "effective_type": "4g",
        "round_trip_time": "200",
        "uifid": "",
        "msToken": "",
    }

    def __init__(self, cookie: str = None):
        self.cookie = cookie
        self.headers = self.DEFAULT_HEADERS.copy()
        if cookie:
            self.headers["Cookie"] = cookie

            if "msToken=" in cookie:
                ms_token_match = re.search(r'msToken=([^;]+)', cookie)
                if ms_token_match:
                    self.BASE_PARAMS["msToken"] = ms_token_match.group(1)

        try:
            from ..encrypt import ABogus
            self.ab = ABogus(self.DEFAULT_HEADERS["User-Agent"], "Win32")
        except ImportError:
            logger.warning("ABogus 不可用，将跳过签名")
            self.ab = None

    def _generate_a_bogus(self, params: dict, cookies: str = "") -> str:
        if not self.ab:
            return ""

        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        try:
            return self.ab.generate_result(query_string)
        except Exception:
            return ""

    @async_retry(max_attempts=3, delay=2)
    async def fetch_video_detail(self, video_id: str) -> Optional[Dict[str, Any]]:
        params = self.BASE_PARAMS.copy()
        params["aweme_id"] = video_id

        a_bogus = self._generate_a_bogus(params, self.cookie or "")
        if a_bogus:
            params["a_bogus"] = a_bogus

        try:
            async with httpx.AsyncClient(
                headers=self.headers,
                timeout=30,
                follow_redirects=True,
            ) as client:
                response = await client.get(self.VIDEO_DETAIL_API, params=params)

                if response.status_code != 200:
                    logger.error(f"API请求失败: HTTP {response.status_code}")
                    return None

                data = response.json()

                if data.get("status_code") != 0:
                    logger.error(f"API返回错误: {data.get('status_msg', '未知错误')}")
                    return None

                aweme_detail = data.get("aweme_detail", {})
                if not aweme_detail:
                    logger.error("未获取到视频详情")
                    return None

                video_data = aweme_detail.get("video", {})
                play_addr = video_data.get("play_addr", {})

                download_addr = play_addr.get("url_list", [])
                video_url = download_addr[0] if download_addr else ""

                music_data = aweme_detail.get("music", {})
                music_url = music_data.get("play_url", "")

                author_data = aweme_detail.get("author", {})

                cover_url = video_data.get("cover", {}).get("url_list", [""])[0]

                return {
                    "aweme_id": video_id,
                    "title": aweme_detail.get("desc", ""),
                    "desc": aweme_detail.get("desc", ""),
                    "author_nickname": author_data.get("nickname", ""),
                    "author_id": author_data.get("sec_uid", ""),
                    "video_url": video_url,
                    "music_url": music_url,
                    "cover_url": cover_url,
                }

        except Exception as e:
            logger.error(f"获取视频详情异常: {e}")
            return None

    @async_retry(max_attempts=3, delay=2)
    async def fetch_user_videos(self, sec_user_id: str, count: int = 5) -> Optional[List[Dict[str, Any]]]:
        """获取用户最新发布的视频列表"""
        params = self.BASE_PARAMS.copy()
        params["sec_user_id"] = sec_user_id
        params["count"] = str(count)
        params["max_cursor"] = "0"

        a_bogus = self._generate_a_bogus(params, self.cookie or "")
        if a_bogus:
            params["a_bogus"] = a_bogus

        try:
            async with httpx.AsyncClient(
                headers=self.headers,
                timeout=30,
                follow_redirects=True,
            ) as client:
                response = await client.get(self.USER_AWEME_API, params=params)

                if response.status_code != 200:
                    logger.error(f"获取用户视频失败: HTTP {response.status_code}")
                    return None

                data = response.json()

                if data.get("status_code") != 0:
                    logger.error(f"API返回错误: {data.get('status_msg', '未知错误')}")
                    return None

                aweme_list = data.get("aweme_list", [])
                videos = []

                for aweme in aweme_list:
                    video_data = aweme.get("video", {})
                    play_addr = video_data.get("play_addr", {})
                    download_addr = play_addr.get("url_list", [])
                    video_url = download_addr[0] if download_addr else ""
                    author_data = aweme.get("author", {})

                    videos.append({
                        "aweme_id": aweme.get("aweme_id", ""),
                        "title": aweme.get("desc", ""),
                        "desc": aweme.get("desc", ""),
                        "author_nickname": author_data.get("nickname", ""),
                        "author_id": author_data.get("sec_uid", ""),
                        "video_url": video_url,
                        "cover_url": video_data.get("cover", {}).get("url_list", [""])[0],
                        "create_time": aweme.get("create_time", 0),
                    })

                return videos

        except Exception as e:
            logger.error(f"获取用户视频异常: {e}")
            return None

    def extract_sec_user_id(self, url: str) -> Optional[str]:
        """从用户主页URL中提取sec_user_id"""
        import re
        # 匹配抖音用户URL中的sec_user_id
        # 格式: https://www.douyin.com/user/MS4wLjABAAAA47NXjVL94k27knoWi7HoCROzMAxMm9eko0JCrYW2YO8
        pattern = r"/user/([a-zA-Z0-9_-]+)"
        match = re.search(pattern, url)
        if match:
            return match.group(1)
        return None