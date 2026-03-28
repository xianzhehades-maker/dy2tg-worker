"""轻量级抖音 API 客户端 - 基于 TikTokDownloader 项目"""

import os
import sys
import re
import json
import asyncio
from datetime import datetime
from typing import List, Dict, Optional, Any
from urllib.parse import urlparse, parse_qs, urlencode, quote

import httpx
import aiofiles

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from hf.utils.logger import get_logger
from hf.utils.retry import async_retry
from hf.encrypt import ABogus
from hf import config

logger = get_logger("douyin_api_client")

PROXY_URL = os.getenv("PROXY_URL", "")


class DouyinAPIClient:
    """轻量级抖音 API 客户端"""

    # API 端点
    DOMAIN = "https://www.douyin.com"
    USER_POST_API = f"{DOMAIN}/aweme/v1/web/aweme/post/"
    VIDEO_DETAIL_API = f"{DOMAIN}/aweme/v1/web/aweme/detail/"

    # 请求头 - 与 TikTokDownloader 保持一致
    DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Referer": DOMAIN,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }

    # 基础请求参数 - 与 TikTokDownloader 保持一致
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

    # URL 正则表达式
    PATTERN_USER_HOME = re.compile(r"https?://(?:www\.)?douyin\.com/user/([A-Za-z0-9_.=-]+)")
    PATTERN_USER_SEC_ID = re.compile(r'"sec_user_id":"([^"]+)"')
    PATTERN_USER_SEC_ID_2 = re.compile(r'sec_user_id\s*[:=]\s*["\']([^"\']+)["\']')
    PATTERN_USER_SEC_ID_3 = re.compile(r'"uid":"([^"]+)"')
    PATTERN_AWEME_ID = re.compile(r"\b(\d{18,20})\b")

    def __init__(self, cookie: str = None):
        """
        初始化抖音 API 客户端

        Args:
            cookie: 抖音 Cookie 字符串
        """
        self.cookie = cookie
        self.headers = self.DEFAULT_HEADERS.copy()
        if cookie:
            # 去除 Cookie 中的换行符和空白字符
            clean_cookie = cookie.strip().replace('\n', '').replace('\r', '')
            self.headers["Cookie"] = clean_cookie
            
            # 从Cookie中提取msToken
            if "msToken=" in clean_cookie:
                ms_token_match = re.search(r'msToken=([^;]+)', clean_cookie)
                if ms_token_match:
                    self.BASE_PARAMS["msToken"] = ms_token_match.group(1)

        # 初始化a_bogus加密器
        self.ab = ABogus(
            self.DEFAULT_HEADERS["User-Agent"],
            "Win32"
        )

        self.client = httpx.AsyncClient(
            headers=self.headers,
            timeout=30.0,
            follow_redirects=True
        )

    async def close(self):
        """关闭客户端"""
        await self.client.aclose()
    
    def _encrypt_params(self, params: dict) -> str:
        """
        加密URL参数，添加a_bogus
        
        Args:
            params: 参数字典
            
        Returns:
            加密后的URL参数字符串
        """
        # 注意：ABogus.get_value会自己处理URL编码，所以直接传字典
        a_bogus_value = self.ab.get_value(params, "GET")
        
        # 编码参数
        encoded_params = urlencode(
            params,
            safe="=",
            quote_via=quote,
        )
        
        # 添加a_bogus加密参数
        encoded_params += f"&a_bogus={a_bogus_value}"
        
        return encoded_params

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @async_retry(max_retries=3, base_delay=2.0, max_delay=30.0)
    async def extract_sec_user_id(self, homepage_url: str) -> Optional[str]:
        """
        从抖音主页 URL 中提取 sec_user_id

        Args:
            homepage_url: 抖音主页 URL

        Returns:
            sec_user_id 或 None
        """
        try:
            logger.info(f"正在提取 sec_user_id: {homepage_url}")

            # 清理URL，移除查询参数和片段
            from urllib.parse import urlparse, urlunparse
            parsed_url = urlparse(homepage_url)
            clean_url = urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                '',  # params
                '',  # query
                ''   # fragment
            ))
            
            if clean_url != homepage_url:
                logger.info(f"清理后的URL: {clean_url}")
                homepage_url = clean_url

            # 先尝试直接从 URL 匹配
            # 确保 URL 有 scheme 前缀
            if not homepage_url.startswith('http'):
                homepage_url = 'https://' + homepage_url
                logger.info(f"添加 https:// 前缀: {homepage_url}")

            match = self.PATTERN_USER_HOME.search(homepage_url)
            if match:
                # 从 URL 中获取了 user_id
                user_id = match.group(1)
                logger.info(f"从 URL 中获取到 user_id: {user_id}")
                
                # 检查 user_id 是否已经是 sec_user_id 格式（通常很长，超过30个字符）
                if len(user_id) > 30:
                    logger.info(f"URL中的user_id看起来像sec_user_id，直接使用: {user_id}")
                    return user_id

                # 访问用户主页
                response = await self.client.get(homepage_url)
                if response.status_code == 200:
                    html = response.text
                    
                    # 保存HTML用于调试（可选，注释掉以避免文件堆积）
                    # import hashlib
                    # import time
                    # html_hash = hashlib.md5(homepage_url.encode()).hexdigest()[:8]
                    # debug_file = f"debug_{int(time.time())}_{html_hash}.html"
                    # with open(debug_file, 'w', encoding='utf-8') as f:
                    #     f.write(html)
                    # logger.info(f"HTML已保存到: {debug_file}")
                    
                    # 尝试多种方式提取 sec_user_id
                    sec_user_match = self.PATTERN_USER_SEC_ID.search(html)
                    if sec_user_match:
                        sec_user_id = sec_user_match.group(1)
                        logger.info(f"成功提取 sec_user_id (方式1): {sec_user_id}")
                        return sec_user_id
                    
                    sec_user_match_2 = self.PATTERN_USER_SEC_ID_2.search(html)
                    if sec_user_match_2:
                        sec_user_id = sec_user_match_2.group(1)
                        logger.info(f"成功提取 sec_user_id (方式2): {sec_user_id}")
                        return sec_user_id
                    
                    sec_user_match_3 = self.PATTERN_USER_SEC_ID_3.search(html)
                    if sec_user_match_3:
                        sec_user_id = sec_user_match_3.group(1)
                        logger.info(f"成功提取 sec_user_id (方式3): {sec_user_id}")
                        return sec_user_id
                    
                    # 如果都没找到，尝试从页面中搜索所有可能的模式
                    logger.warning(f"未能从HTML中提取sec_user_id，HTML长度: {len(html)}")
                    
                    # 打印一些关键信息用于调试
                    if len(html) < 5000:
                        logger.debug(f"HTML内容预览: {html[:1000]}")
                    else:
                        # 只检查是否包含关键字符串
                        has_sec = 'sec_user_id' in html
                        has_uid = 'uid' in html
                        logger.debug(f"HTML包含sec_user_id: {has_sec}, 包含uid: {has_uid}")

            logger.warning(f"未能提取 sec_user_id")
            return None

        except Exception as e:
            logger.error(f"提取 sec_user_id 失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    DEFAULT_MAX_VIDEO_COUNT = 5

    @async_retry(max_retries=3, base_delay=3.0, max_delay=60.0)
    async def fetch_user_videos(
        self,
        sec_user_id: str,
        max_days_old: int = 3,
        max_pages: int = 5,
        max_video_count: int = None
    ) -> List[Dict[str, Any]]:
        """
        获取用户视频列表

        Args:
            sec_user_id: 用户 sec_user_id
            max_days_old: 只获取最近 N 天的视频（用于缓冲）
            max_pages: 最多获取页数
            max_video_count: 最多获取视频数量

        Returns:
            视频列表，每个视频包含：
            - video_id: 视频ID
            - video_url: 视频URL
            - publish_time: 发布时间（datetime）
            - desc: 视频描述
            - aweme_data: 原始数据
        """
        try:
            if max_video_count is None:
                max_video_count = self.DEFAULT_MAX_VIDEO_COUNT

            logger.info(f"正在获取用户视频: sec_user_id={sec_user_id}, 目标数量: {max_video_count}")

            videos = []
            cursor = 0
            has_more = True
            pages_fetched = 0

            logger.info(f"数量过滤: 只保留最近 {max_video_count} 个视频")

            while has_more and pages_fetched < max_pages and len(videos) < max_video_count:
                pages_fetched += 1
                logger.info(f"正在获取第 {pages_fetched} 页数据...")

                # 构建请求参数 - 基于TikTokDownloader的参数
                params = self.BASE_PARAMS.copy()
                params.update({
                    "sec_user_id": sec_user_id,
                    "max_cursor": str(cursor),
                    "locate_query": "false",
                    "show_live_replay_strategy": "1",
                    "need_time_list": "1",
                    "time_list_query": "0",
                    "whale_cut_token": "",
                    "cut_version": "1",
                    "count": "18",
                    "publish_video_strategy_type": "2",
                })

                # 设置 Referer
                headers = self.headers.copy()
                headers["Referer"] = f"{self.DOMAIN}/user/{sec_user_id}"

                try:
                    # 加密参数
                    encrypted_params = self._encrypt_params(params)
                    full_url = f"{self.USER_POST_API}?{encrypted_params}"

                    client_kwargs = {
                        "timeout": 60.0,
                        "follow_redirects": True,
                        "verify": False
                    }
                    if PROXY_URL:
                        client_kwargs["proxy"] = PROXY_URL

                    async with httpx.AsyncClient(**client_kwargs) as client:
                        response = await client.get(full_url, headers=headers)

                    if response.status_code != 200:
                        logger.warning(f"API 请求失败，状态码: {response.status_code}")
                        logger.warning(f"响应内容: {response.text[:1000]}")
                        break

                    try:
                        data = response.json()
                    except Exception as e:
                        logger.error(f"解析 JSON 失败: {e}")
                        logger.error(f"响应内容: {response.text[:2000]}")
                        break

                    # 检查响应状态
                    if data.get("status_code") != 0:
                        logger.warning(f"API 返回错误: {data}")
                        break

                    # 提取视频列表
                    aweme_list = data.get("aweme_list", [])
                    if not aweme_list:
                        logger.info("没有更多视频了")
                        break

                    logger.info(f"本页获取到 {len(aweme_list)} 个视频")

                    # 处理每个视频
                    page_qualified_count = 0
                    for aweme in aweme_list:
                        video_info = self._parse_aweme_data(aweme)
                        if video_info:
                            videos.append(video_info)
                            page_qualified_count += 1

                            if len(videos) >= max_video_count:
                                logger.info(f"已达到目标数量 {max_video_count}，停止获取")
                                break

                    logger.info(f"本页符合条件的视频: {page_qualified_count} 个")

                    # 更新游标
                    cursor = data.get("max_cursor", 0)
                    has_more = data.get("has_more", False)

                    if not has_more:
                        logger.info("没有更多视频了")
                        break

                    # 避免请求过快
                    await asyncio.sleep(1.0)

                except Exception as e:
                    logger.error(f"获取第 {pages_fetched} 页失败: {e}")
                    import traceback
                    traceback.print_exc()
                    if 'response' in locals():
                        logger.error(f"响应状态码: {response.status_code}")
                        logger.error(f"响应内容: {response.text[:3000]}")
                    break

            logger.info(f"总共获取到 {len(videos)} 个视频")
            return videos

        except Exception as e:
            logger.error(f"获取用户视频失败: {e}")
            import traceback
            traceback.print_exc()
            return []

    @async_retry(max_retries=3, base_delay=2.0, max_delay=30.0)
    async def fetch_video_detail(self, video_id: str) -> Optional[Dict[str, Any]]:
        """
        通过视频ID获取视频详情（包括实时下载链接）

        Args:
            video_id: 视频ID (aweme_id)

        Returns:
            视频详情信息，包含 download_url，或 None
        """
        try:
            logger.info(f"正在获取视频详情: video_id={video_id}")

            params = self.BASE_PARAMS.copy()
            params["aweme_id"] = video_id

            headers = self.headers.copy()
            headers["Referer"] = f"{self.DOMAIN}/video/{video_id}"

            encrypted_params = self._encrypt_params(params)
            full_url = f"{self.VIDEO_DETAIL_API}?{encrypted_params}"

            if PROXY_URL:
                logger.info(f"使用代理: {PROXY_URL}")
            
            client_kwargs = {
                "timeout": 60.0,
                "follow_redirects": True,
                "verify": False
            }
            if PROXY_URL:
                client_kwargs["proxy"] = PROXY_URL

            async with httpx.AsyncClient(**client_kwargs) as client:
                response = await client.get(full_url, headers=headers)

            if response.status_code != 200:
                logger.warning(f"获取视频详情失败，状态码: {response.status_code}")
                return None

            data = response.json()

            if data.get("status_code") != 0:
                logger.warning(f"API 返回错误: {data}")
                return None

            aweme_detail = data.get("aweme_detail")
            if not aweme_detail:
                logger.warning(f"未找到 aweme_detail")
                return None

            video_info = self._parse_aweme_data(aweme_detail)
            if video_info:
                logger.info(f"成功获取视频详情，download_url可用: {video_info.get('download_url') is not None}")

            return video_info

        except Exception as e:
            logger.error(f"获取视频详情失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _parse_aweme_data(self, aweme: Dict) -> Optional[Dict[str, Any]]:
        """
        解析单个视频数据

        Args:
            aweme: API 返回的原始数据

        Returns:
            解析后的视频信息
        """
        try:
            aweme_id = aweme.get("aweme_id", "")
            if not aweme_id:
                logger.debug("缺少 aweme_id，跳过")
                return None

            # 提取发布时间
            create_time = aweme.get("create_time", 0)
            publish_time = None
            if create_time:
                publish_time = datetime.fromtimestamp(create_time)
                logger.debug(f"视频 {aweme_id} 的 create_time: {create_time} -> {publish_time}")
            else:
                logger.debug(f"视频 {aweme_id} 没有 create_time")

            # 提取描述
            desc = aweme.get("desc", "")

            # 提取视频下载链接
            download_url = self._extract_download_url(aweme)

            # 构建视频信息
            video_info = {
                "video_id": aweme_id,
                "video_url": f"https://www.douyin.com/video/{aweme_id}",
                "publish_time": publish_time,
                "desc": desc,
                "download_url": download_url,
                "aweme_data": aweme
            }

            return video_info

        except Exception as e:
            logger.warning(f"解析 aweme_data 失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _extract_download_url(self, aweme: Dict) -> Optional[str]:
        """
        从 aweme 数据中提取视频下载链接

        Args:
            aweme: API 返回的原始数据

        Returns:
            视频下载链接或 None
        """
        try:
            video = aweme.get("video", {})
            bit_rate = video.get("bit_rate", [])
            
            if not bit_rate:
                return None
            
            # 按分辨率排序，选择最高质量
            def get_bitrate_info(br):
                play_addr = br.get("play_addr", {})
                return (
                    play_addr.get("height", 0),
                    play_addr.get("width", 0),
                    br.get("FPS", 0),
                    br.get("bit_rate", 0),
                    play_addr.get("data_size", 0)
                )
            
            # 排序：先按高度，再按宽度，再按帧率，再按码率
            bit_rate.sort(key=get_bitrate_info, reverse=True)
            
            best_bitrate = bit_rate[0]
            play_addr = best_bitrate.get("play_addr", {})
            url_list = play_addr.get("url_list", [])
            
            if url_list:
                return url_list[0]
            
            return None
            
        except Exception as e:
            logger.warning(f"提取下载链接失败: {e}")
            return None

    @async_retry(max_retries=5, base_delay=5.0, max_delay=120.0)
    async def download_video(self, download_url: str, save_path: str) -> bool:
        """
        下载视频 - 流式写入，支持断点续传（与 TikTokDownloader 一致）
        """
        try:
            logger.info(f"开始下载视频: {download_url[:80]}...")

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
                "Referer": self.DOMAIN,
            }

            chunk_size = 1024 * 1024  # 1MB chunks
            temp_path = save_path + ".tmp"
            resume_position = 0

            if os.path.exists(temp_path):
                resume_position = os.path.getsize(temp_path)
                logger.info(f"检测到断点续传，已下载 {resume_position / 1024 / 1024:.2f} MB")

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
                "Referer": self.DOMAIN,
            }
            if resume_position > 0:
                headers["Range"] = f"bytes={resume_position}-"

            async with self.client.stream("GET", download_url, headers=headers, timeout=300.0) as response:
                if response.status_code == 416:
                    logger.error("下载范围无效（文件已完整？）")
                    if os.path.exists(temp_path):
                        os.rename(temp_path, save_path)
                    return True

                if response.status_code not in (200, 206):
                    logger.error(f"下载失败，状态码: {response.status_code}")
                    return False

                content_length = response.headers.get("Content-Length")
                if content_length:
                    total_size = int(content_length) + resume_position
                else:
                    total_size = None

                os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)

                async with aiofiles.open(temp_path, "ab") as f:
                    async for chunk in response.aiter_bytes(chunk_size):
                        await f.write(chunk)

            if os.path.exists(temp_path):
                actual_size = os.path.getsize(temp_path)
                os.rename(temp_path, save_path)
                logger.info(f"视频下载成功: {save_path} (大小: {actual_size / 1024 / 1024:.2f} MB)")
            return True

        except Exception as e:
            logger.error(f"下载视频失败: {e}")
            import traceback
            traceback.print_exc()
            return False


# 全局客户端实例缓存
_client_instance: Optional[DouyinAPIClient] = None


def get_douyin_client(cookie: str = None) -> DouyinAPIClient:
    """
    获取抖音 API 客户端实例（单例模式）

    Args:
        cookie: 抖音 Cookie

    Returns:
        DouyinAPIClient 实例
    """
    global _client_instance

    if _client_instance is None:
        _client_instance = DouyinAPIClient(cookie)
    elif cookie and _client_instance.cookie != cookie:
        _client_instance = DouyinAPIClient(cookie)

    return _client_instance
