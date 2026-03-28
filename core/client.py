"""统一视频客户端 - 支持抖音和TikTok"""

import re
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Tuple, List
from enum import Enum

import httpx

logger = logging.getLogger(__name__)


class Platform(Enum):
    DOUYIN = "douyin"
    TIKTOK = "tiktok"


@dataclass
class VideoInfo:
    video_id: str
    platform: Platform
    title: str
    desc: str
    author_nickname: str
    author_id: str
    video_url: str
    music_url: Optional[str]
    cover_url: str
    aweme_id: str


class VideoClientError(Exception):
    pass


class VideoClient:
    DOUYIN_VIDEO_PATTERN = re.compile(r"https?://(?:www\.)?douyin\.com/video/(\d+)")
    TIKTOK_VIDEO_PATTERN = re.compile(r"https?://(?:www\.)?tiktok\.com/@[\w.]+/video/(\d+)")

    def __init__(self, cookie: str = None, cookie_tiktok: str = None, proxy: str = None):
        self.cookie = cookie or ""
        self.cookie_tiktok = cookie_tiktok or ""
        self.proxy = proxy

    def detect_platform(self, url: str) -> Tuple[Platform, str]:
        if "douyin.com" in url:
            match = self.DOUYIN_VIDEO_PATTERN.search(url)
            if match:
                return Platform.DOUYIN, match.group(1)
        elif "tiktok.com" in url:
            match = self.TIKTOK_VIDEO_PATTERN.search(url)
            if match:
                return Platform.TIKTOK, match.group(1)

        raise VideoClientError(f"无法识别的视频URL: {url}")

    async def get_video_info(self, url: str) -> VideoInfo:
        platform, video_id = self.detect_platform(url)

        if platform == Platform.TIKTOK:
            return await self._get_tiktok_info(url, video_id)
        else:
            return await self._get_douyin_info(url, video_id)

    async def _get_douyin_info(self, url: str, video_id: str) -> VideoInfo:
        try:
            from .api.douyin_client import DouyinAPIClient

            client = DouyinAPIClient(self.cookie)
            data = await client.fetch_video_detail(video_id)

            if not data:
                raise VideoClientError(f"无法获取抖音视频信息: {video_id}")

            return VideoInfo(
                video_id=video_id,
                platform=Platform.DOUYIN,
                title=data.get("title", ""),
                desc=data.get("desc", ""),
                author_nickname=data.get("author_nickname", ""),
                author_id=data.get("author_id", ""),
                video_url=data.get("video_url", ""),
                music_url=data.get("music_url"),
                cover_url=data.get("cover_url", ""),
                aweme_id=data.get("aweme_id", video_id),
            )

        except Exception as e:
            raise VideoClientError(f"获取抖音视频信息失败: {str(e)}")

    async def _get_tiktok_info(self, url: str, video_id: str) -> VideoInfo:
        try:
            api = TikTokAPI(
                cookie=self.cookie_tiktok,
                proxy=self.proxy,
            )

            data = await api.get_video_info(video_id)

            return VideoInfo(
                video_id=video_id,
                platform=Platform.TIKTOK,
                title=data.get("title", ""),
                desc=data.get("desc", ""),
                author_nickname=data.get("author_nickname", ""),
                author_id=data.get("author_id", ""),
                video_url=data.get("video_url", ""),
                music_url=data.get("music_url"),
                cover_url=data.get("cover_url", ""),
                aweme_id=video_id,
            )

        except Exception as e:
            raise VideoClientError(f"获取TikTok视频信息失败: {str(e)}")

    async def download_video(
        self,
        url: str,
        output_path: str,
        progress_callback=None,
    ) -> Tuple[bool, str]:
        try:
            video_info = await self.get_video_info(url)

            if not video_info.video_url:
                return False, "无法获取视频下载地址"

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://www.douyin.com/" if video_info.platform == Platform.DOUYIN else "https://www.tiktok.com/",
            }

            async with httpx.AsyncClient(
                headers=headers,
                timeout=60,
                follow_redirects=True,
            ) as client:
                async with client.stream("GET", video_info.video_url) as response:
                    if response.status_code != 200:
                        return False, f"下载失败: HTTP {response.status_code}"

                    total_size = int(response.headers.get("Content-Length", 0))
                    downloaded = 0

                    with open(output_path, "wb") as f:
                        async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                            f.write(chunk)
                            downloaded += len(chunk)
                            if progress_callback:
                                progress_callback(downloaded, total_size)

                    return True, video_info.title

        except Exception as e:
            return False, str(e)


class TikTokAPI:
    """TikTok API 客户端 - 使用第三方API"""

    TIKTOK_API_URL = "https://www.tikwm.com/api/"

    HEADERS = {
        "Accept": "application/json",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    def __init__(self, cookie: str = None, proxy: str = None):
        self.cookie = cookie
        self.proxy = proxy

    async def get_video_info(self, video_id: str) -> dict:
        params = {
            "url": f"https://www.tiktok.com/discover?videoId={video_id}",
            "hd": "1",
        }

        async with httpx.AsyncClient(
            headers=self.HEADERS,
            timeout=30,
            follow_redirects=True,
        ) as client:
            response = await client.get(self.TIKTOK_API_URL, params=params)

            if response.status_code != 200:
                raise VideoClientError(f"API请求失败: HTTP {response.status_code}")

            data = response.json()

            if data.get("code") != 0:
                raise VideoClientError(f"API返回错误: {data.get('msg', '未知错误')}")

            return self._parse_response(data["data"])

    def _parse_response(self, data: dict) -> dict:
        author = data.get("author", {})
        music = data.get("music", {})
        video = data.get("play_addr", {})

        return {
            "title": data.get("title", ""),
            "desc": data.get("title", ""),
            "author_nickname": author.get("nickname", ""),
            "author_id": author.get("id", ""),
            "video_url": video.get("url", "") or data.get("play", ""),
            "music_url": music.get("play_url", ""),
            "cover_url": data.get("origin_cover", ""),
        }


def extract_video_id(url: str) -> Tuple[str, Platform]:
    douyin_match = re.search(r"/video/(\d{18,20})", url)
    if douyin_match:
        return douyin_match.group(1), Platform.DOUYIN

    tiktok_match = re.search(r"/video/(\d+)", url)
    if tiktok_match:
        return tiktok_match.group(1), Platform.TIKTOK

    raise VideoClientError(f"无法从URL提取视频ID: {url}")