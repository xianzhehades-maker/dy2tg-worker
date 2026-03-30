"""
UP Polling Script - GitHub Actions
从 CF Worker 获取监控UP列表，轮询新视频，触发处理流水线
"""

import os
import sys
import asyncio
import logging
import httpx
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WORKERS_URL = os.getenv("WORKERS_URL", "").rstrip("/")
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "")
GH_REPO = os.getenv("GH_REPO", "")
GH_PAT = os.getenv("GH_PAT", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DOUYIN_COOKIE = os.getenv("DOUYIN_COOKIE", "")
TIKTOK_COOKIE = os.getenv("TIKTOK_COOKIE", "")
PROXY_URL = os.getenv("PROXY_URL", "")

TIKTOK_DOWNLOADER_DIR = str(Path(__file__).resolve().parent.parent.parent / "tiktok_downloader")
sys.path.insert(0, TIKTOK_DOWNLOADER_DIR)
logger.info(f"TikTokDownloader 目录: {TIKTOK_DOWNLOADER_DIR}")
logger.info(f"当前工作目录: {os.getcwd()}")


async def get_monitors():
    if not WORKERS_URL:
        logger.error("WORKERS_URL 未配置")
        return [], []

    url = f"{WORKERS_URL}/api/monitors"
    headers = {}
    if AUTH_TOKEN:
        headers["X-Auth-Token"] = AUTH_TOKEN

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=headers)
        logger.info(f"Monitors 响应: {resp.status_code}")
        resp.raise_for_status()
        data = resp.json()
        return data.get("monitors", []), data.get("groups", [])


async def get_task_history():
    if not WORKERS_URL:
        return {}

    url = f"{WORKERS_URL}/api/task_history"
    headers = {}
    if AUTH_TOKEN:
        headers["X-Auth-Token"] = AUTH_TOKEN

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        result = {}
        for t in data.get("tasks", []):
            vid = t.get("video_id") or t.get("task_id")
            status = t.get("status", "")
            if vid:
                result[vid] = status
        return result


async def create_task_history(video_id: str, source_url: str, chat_id: int, group_id: int):
    if not WORKERS_URL:
        logger.error("WORKERS_URL 未配置，无法创建 task_history")
        return False

    url = f"{WORKERS_URL}/api/task_history"
    headers = {}
    if AUTH_TOKEN:
        headers["X-Auth-Token"] = AUTH_TOKEN

    payload = {
        "video_id": video_id,
        "source_url": source_url,
        "chat_id": chat_id,
        "group_id": group_id,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code in (200, 204):
                result = resp.json() if resp.status_code == 200 else {}
                if result.get("message") == "Video already exists":
                    existing_status = result.get("status", "")
                    logger.info(f"视频 {video_id} 已存在 (status={existing_status})")
                    return existing_status
                logger.info(f"✅ 已创建 task_history: {video_id}")
                return "pending"
            else:
                logger.warning(f"创建 task_history 失败: {resp.status_code}")
                return None
    except Exception as e:
        logger.warning(f"创建 task_history 时出错: {e}")
        return None


async def dispatch_video_process(video_url: str, task_id: str, chat_id: int, video_desc: str, group_id: int):
    if not GH_REPO or not GH_PAT:
        logger.error("GH_REPO 或 GH_PAT 未配置")
        return False

    owner, repo = GH_REPO.split("/")
    url = f"https://api.github.com/repos/{owner}/{repo}/dispatches"

    headers = {
        "Authorization": f"Bearer {GH_PAT}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
        "User-Agent": "dy2tg-bot",
    }

    payload = {
        "event_type": "video-process",
        "client_payload": {
            "video_url": video_url,
            "task_id": task_id,
            "chat_id": chat_id,
            "video_desc": video_desc,
            "group_id": group_id,
        },
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code in (200, 204):
            logger.info(f"✅ 已触发视频处理: {task_id}")
            return True
        else:
            logger.error(f"触发失败: {resp.status_code} {resp.text}")
            return False


def extract_sec_user_id(url: str) -> str:
    import re
    match = re.search(r'/user/([A-Za-z0-9_-]+)', url)
    if match:
        return match.group(1)
    return None


async def fetch_user_videos(sec_user_id: str, platform: str = "douyin"):
    is_tiktok = platform.lower() == "tiktok"
    cookie = TIKTOK_COOKIE if is_tiktok else DOUYIN_COOKIE

    try:
        from src.testers.params import Params
        from src.testers.logger import Logger
        from src.interface.account import Account
        from src.interface.account_tiktok import AccountTikTok
        from src.interface import API
        from src.extract.extractor import Extractor
        from src.tools.format import cookie_str_to_dict
        from src.encrypt.msToken import MsToken
        from src.custom import PARAMS_HEADERS

        class DummyRecorder:
            def __init__(self):
                self.field_keys = []
            async def save(self, *args, **kwargs):
                pass

        API.init_progress_object(server_mode=True)

        async with Params() as params:
            if cookie:
                params.cookie_str = cookie
                params.headers["Cookie"] = cookie

                cookie_dict = cookie_str_to_dict(cookie)

                ms_token = cookie_dict.get("msToken") or cookie_dict.get("mstoken") or cookie_dict.get("MSTOKEN")
                if not ms_token:
                    logger.info(f"从 cookie 中未找到 msToken，正在获取真实的 msToken...")
                    try:
                        real_ms_token = await MsToken.get_long_ms_token(
                            Logger(),
                            PARAMS_HEADERS,
                            proxy=None
                        )
                        if real_ms_token and "msToken" in real_ms_token:
                            ms_token = real_ms_token["msToken"]
                            logger.info(f"已获取到真实的 msToken: {ms_token[:20]}...")
                        else:
                            fake_ms_token = MsToken.get_fake_ms_token()
                            ms_token = fake_ms_token["msToken"]
                            logger.info(f"获取真实 msToken 失败，已生成假的 msToken: {ms_token[:20]}...")
                    except Exception as e:
                        logger.warning(f"获取真实 msToken 时出错: {e}")
                        fake_ms_token = MsToken.get_fake_ms_token()
                        ms_token = fake_ms_token["msToken"]
                        logger.info(f"已生成假的 msToken: {ms_token[:20]}...")
                params.msToken = ms_token
                API.params["msToken"] = ms_token
                if is_tiktok:
                    params.msToken_tiktok = ms_token
                if cookie:
                    updated_cookie = cookie
                    if "msToken=" in updated_cookie:
                        import re
                        updated_cookie = re.sub(r'msToken=[^;]*', f'msToken={ms_token}', updated_cookie)
                    else:
                        if not updated_cookie.endswith(';'):
                            updated_cookie += ';'
                        updated_cookie += f' msToken={ms_token}'
                    params.cookie_str = updated_cookie
                    params.headers["Cookie"] = updated_cookie

                uifid = cookie_dict.get("UIFID") or cookie_dict.get("uifid")
                if uifid:
                    params.uifid = uifid
                    API.params["uifid"] = uifid

            AccountClass = AccountTikTok if is_tiktok else Account
            account = AccountClass(
                params,
                cookie=updated_cookie,
                proxy=PROXY_URL if PROXY_URL else None,
                sec_user_id=sec_user_id,
                tab="post",
                cursor=0,
                count=20
            )

            raw_videos = await account.run(single_page=True)
            logger.info(f"从 Account{'TikTok' if is_tiktok else '（抖音）'}获取到 {len(raw_videos)} 个原始视频")

            if not raw_videos:
                return []

            extractor = Extractor(params)
            dummy_recorder = DummyRecorder()
            formatted_videos = await extractor.run(raw_videos, dummy_recorder, type_="detail", tiktok=is_tiktok)

            logger.info(f"从 Extractor 转换后得到 {len(formatted_videos)} 个视频，包含 downloads 字段")

            result_videos = []
            for video in formatted_videos:
                try:
                    video_id = video.get("id", "")
                    desc = video.get("desc", "")

                    if video_id:
                        result_videos.append({
                            "video_id": video_id,
                            "desc": desc,
                            "raw_video": video,
                            "downloads": video.get("downloads", [])
                        })
                except Exception as e:
                    logger.warning(f"处理视频数据时出错: {e}")
                    continue

            return result_videos
    except Exception as e:
        logger.error(f"获取UP视频失败 {sec_user_id}: {e}")
        import traceback
        traceback.print_exc()
        return []


async def notify_telegram(text: str):
    if not BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for chat_id in [os.getenv("NOTIFY_CHAT_ID", "0")]:
        if str(chat_id) == "0":
            continue
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(url, json={"chat_id": chat_id, "text": text})


async def poll_single_up(monitor, task_history: dict):
    sec_user_id = extract_sec_user_id(monitor.get("up_url", ""))
    if not sec_user_id:
        logger.warning(f"无法提取 sec_user_id: {monitor['up_url']}")
        return 0

    platform = monitor.get("platform", "douyin")
    videos = await fetch_user_videos(sec_user_id, platform)
    if not videos:
        logger.info(f"UP {monitor['up_name']} 无新视频")
        return 0

    dispatched = 0
    group_id = monitor.get("group_id")

    max_videos_per_up = 5
    processed_count = 0

    for video in videos:
        if processed_count >= max_videos_per_up:
            logger.info(f"已达到每个UP最大处理数 {max_videos_per_up}，停止处理")
            break

        video_id = str(video.get("video_id", ""))
        chat_id = monitor.get("target_chat_id") or 0
        desc = video.get("desc", "")[:100]
        existing_status = task_history.get(video_id, "")

        if existing_status == "completed":
            logger.info(f"视频 {video_id} 已完成发送，跳过")
            continue

        logger.info(f"视频 {video_id} 状态={existing_status or '新视频'}，准备处理")

        success = await dispatch_video_process(
            video_url=monitor.get("up_url"),
            task_id=video_id,
            chat_id=chat_id,
            video_desc=desc,
            group_id=group_id
        )

        if success:
            dispatched += 1
            processed_count += 1

    return dispatched


async def main():
    logger.info("=" * 50)
    logger.info("UP Polling 开始 (使用 TikTokDownloader)...")
    logger.info(f"WORKERS_URL: {WORKERS_URL}")
    logger.info("=" * 50)

    monitors, groups = await get_monitors()
    if not monitors:
        logger.warning("没有监控的UP")
        await notify_telegram("⚠️ /run_now: 没有监控的UP")
        return

    group_map = {g["id"]: g for g in groups}

    monitor_list = []
    for m in monitors:
        g = group_map.get(m.get("group_id"), {})
        target_chat = g.get("target_channels", "")
        if target_chat:
            try:
                import json
                channels = json.loads(target_chat)
                if isinstance(channels, list) and len(channels) > 0:
                    tc = int(channels[0])
                else:
                    tc = 0
            except:
                try:
                    tc = int(target_chat)
                except:
                    tc = 0
        else:
            tc = 0
        m["target_chat_id"] = tc
        monitor_list.append(m)

    task_history = await get_task_history()
    logger.info(f"已记录视频数: {len(task_history)}")

    total_dispatched = 0
    for m in monitor_list:
        d = await poll_single_up(m, task_history)
        total_dispatched += d

    logger.info(f"轮询完成，新视频: {total_dispatched}")

    if total_dispatched > 0:
        await notify_telegram(f"✅ /run_now: 发现 {total_dispatched} 个新视频，已触发处理")


if __name__ == "__main__":
    asyncio.run(main())