"""
GitHub Actions / CLI 视频处理器
从UP主页下载视频，添加水印，上传到 R2
"""

import os

os.environ["LOG_DIR"] = "/tmp/logs"

try:
    os.makedirs("/tmp/logs", exist_ok=True)
except:
    pass

import sys
import asyncio
import logging
import uuid
import hashlib
import time
import re
import httpx
import shutil
import subprocess
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, BackgroundTasks, HTTPException, Header, Depends
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="dy2tg Video Processor")

OUTPUT_DIR = Path("/tmp/exports")
OUTPUT_DIR.mkdir(exist_ok=True)

R2_CONFIG = {
    "endpoint_url": os.getenv("R2_ENDPOINT_URL", ""),
    "aws_access_key_id": os.getenv("R2_ACCESS_KEY_ID", ""),
    "aws_secret_access_key": os.getenv("R2_SECRET_ACCESS_KEY", ""),
}
R2_BUCKET = os.getenv("R2_BUCKET_NAME", "video-assets")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "")

CALLBACK_URL = os.getenv("WORKERS_URL", "")
if CALLBACK_URL and not CALLBACK_URL.endswith("/callback"):
    if CALLBACK_URL.endswith("/"):
        CALLBACK_URL = CALLBACK_URL + "callback"
    else:
        CALLBACK_URL = CALLBACK_URL + "/callback"
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "") or os.getenv("HF_AUTH_TOKEN", "")

logger.info("=" * 50)
logger.info("GitHub Actions Worker 启动中...")
logger.info(f"Python: {sys.version}")
logger.info(f"工作目录: {os.getcwd()}")
logger.info("=" * 50)

WATERMARK_PATH = "/tmp/watermark.png"

def generate_watermark():
    try:
        from PIL import Image, ImageDraw, ImageFont
        width, height = 500, 60
        img = Image.new('RGBA', (width, height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        font = ImageFont.load_default()
        text = os.getenv("WATERMARK_TEXT", "艺术家防走失频道@ARTDaliy")
        bbox = draw.textbbox((0, 0), text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        x = 10
        y = (height - text_height) // 2
        draw.text((x, y), text, font=font, fill=(255, 255, 255, 255))
        img.save(WATERMARK_PATH, "PNG")
        logger.info(f"水印图片已生成: {WATERMARK_PATH}")
    except Exception as e:
        logger.warning(f"水印生成失败: {e}")

generate_watermark()

def start_xray_proxy():
    """启动 Xray 代理"""
    import subprocess
    import time
    import os

    xray_path = "./xray"
    config_path = "./config.json"

    if not os.path.exists(xray_path):
        logger.warning("Xray 二进制文件不存在，跳过代理启动")
        return False

    if not os.path.exists(config_path):
        logger.warning("Xray config.json 不存在，跳过代理启动")
        return False

    try:
        logger.info("正在启动 Xray 代理...")
        with open("/tmp/xray.log", "w") as f:
            proc = subprocess.Popen(
                [xray_path, "-c", config_path],
                stdout=f,
                stderr=subprocess.STDOUT
            )

        time.sleep(3)

        if proc.poll() is None:
            logger.info("Xray 代理启动成功 (PID: %d)", proc.pid)
            return True
        else:
            logger.error("Xray 代理启动失败")
            return False
    except Exception as e:
        logger.error("启动 Xray 代理失败: %s", e)
        return False

xray_started = start_xray_proxy()
if xray_started:
    os.environ["PROXY_URL"] = "http://127.0.0.1:1080"
    logger.info("代理环境变量已设置: %s", os.environ.get("PROXY_URL", ""))
else:
    logger.warning("Xray 未启动，将不使用代理")

import os
import sys
from pathlib import Path

# 把 tiktok_downloader 目录加到 sys.path 里，这样可以直接导入 src
TIKTOK_DOWNLOADER_DIR = str(Path(__file__).resolve().parent / "tiktok_downloader")
sys.path.insert(0, TIKTOK_DOWNLOADER_DIR)
logger.info(f"TikTokDownloader 目录: {TIKTOK_DOWNLOADER_DIR}")
logger.info(f"当前工作目录: {os.getcwd()}")

PROXY_URL = os.getenv("PROXY_URL", "http://127.0.0.1:1080")

# 旧模块已不再使用，直接移除


class TaskRequest(BaseModel):
    video_url: str
    chat_id: int
    task_id: str
    caption: Optional[str] = None
    watermark_text: Optional[str] = None
    generate_ai_caption: bool = False
    video_desc: Optional[str] = None
    caption_style: Optional[str] = None


class TaskResponse(BaseModel):
    status: str
    task_id: str
    message: str


def extract_sec_user_id(url: str) -> Optional[str]:
    match = re.search(r'/user/([A-Za-z0-9_-]+)', url)
    if match:
        return match.group(1)
    return None


async def download_video_real(url: str, output_path: str, target_video_id: str = None) -> tuple[bool, str]:
    """
    从UP主页下载视频 (使用 TikTokDownloader 项目 - 抖音版)
    url: UP主页URL
    target_video_id: 要下载的视频ID（来自CF传入的task_id）
    返回: (是否成功, video_desc)
    """
    try:
        from src.testers.params import Params
        from src.testers.logger import Logger
        from src.interface.account import Account  # 注意：这里用 Account（抖音的），不是 AccountTikTok
        from src.interface import API
        from src.extract.extractor import Extractor
        from src.tools.format import cookie_str_to_dict
        from src.encrypt.msToken import MsToken
        from src.custom import PARAMS_HEADERS
        
        # 创建一个 dummy recorder 类，只需要有 save 方法和 field_keys 属性
        class DummyRecorder:
            def __init__(self):
                self.field_keys = []
            async def save(self, *args, **kwargs):
                pass
        
        API.init_progress_object(server_mode=True)

        logger.info(f"开始下载视频, URL: {url}, target_video_id: {target_video_id}")

        sec_user_id = extract_sec_user_id(url)
        if not sec_user_id:
            logger.error(f"无法从 URL 提取 sec_user_id: {url}")
            return False, ""

        logger.info(f"sec_user_id: {sec_user_id}")

        douyin_cookie = os.getenv('DOUYIN_COOKIE', '') or os.getenv('DOUYIN_COOKIES', '')
        
        async with Params() as params:
            # 设置 cookie
            if douyin_cookie:
                params.cookie_str = douyin_cookie
                params.headers["Cookie"] = douyin_cookie
                
                # 从 cookie 字符串中提取 msToken 和 uifid（同时尝试大小写）
                cookie_dict = cookie_str_to_dict(douyin_cookie)
                
                # 提取 msToken（同时尝试大小写，如果没有就获取真实的）
                ms_token = cookie_dict.get("msToken") or cookie_dict.get("mstoken") or cookie_dict.get("MSTOKEN")
                if not ms_token:
                    # 如果 cookie 里没有 msToken，就获取真实的 msToken
                    logger.info("从 cookie 中未找到 msToken，正在获取真实的 msToken...")
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
                            # 如果获取失败，就生成假的
                            fake_ms_token = MsToken.get_fake_ms_token()
                            ms_token = fake_ms_token["msToken"]
                            logger.info(f"获取真实 msToken 失败，已生成假的 msToken: {ms_token[:20]}...")
                    except Exception as e:
                        logger.warning(f"获取真实 msToken 时出错: {e}")
                        # 如果出错，就生成假的
                        fake_ms_token = MsToken.get_fake_ms_token()
                        ms_token = fake_ms_token["msToken"]
                        logger.info(f"已生成假的 msToken: {ms_token[:20]}...")
                params.msToken = ms_token
                API.params["msToken"] = ms_token
                # 把 msToken 也加到 Cookie 字符串里
                if douyin_cookie:
                    updated_cookie = douyin_cookie
                    if "msToken=" in updated_cookie:
                        # 如果已有 msToken，替换它
                        import re
                        updated_cookie = re.sub(r'msToken=[^;]*', f'msToken={ms_token}', updated_cookie)
                    else:
                        # 如果没有，追加到末尾
                        if not updated_cookie.endswith(';'):
                            updated_cookie += ';'
                        updated_cookie += f' msToken={ms_token}'
                    params.cookie_str = updated_cookie
                    params.headers["Cookie"] = updated_cookie
                
                # 提取 uifid（同时尝试 UIFID 和 uifid）
                uifid = cookie_dict.get("UIFID") or cookie_dict.get("uifid")
                if uifid:
                    params.uifid = uifid
                    API.params["uifid"] = uifid
            
            # 使用 Account（抖音的），不是 AccountTikTok
            account = Account(
                params,
                cookie=douyin_cookie,
                proxy=None,
                sec_user_id=sec_user_id,
                tab="post",
                cursor=0,
                count=20
            )
            
            raw_videos = await account.run(single_page=True)
            logger.info(f"从 Account（抖音）获取到 {len(raw_videos)} 个原始视频")
            
            if not raw_videos:
                logger.error("获取视频列表为空")
                return False, ""
            
            # 使用 Extractor 转换数据，添加 downloads 字段（特殊下载链接）
            extractor = Extractor(params)
            dummy_recorder = DummyRecorder()
            formatted_videos = await extractor.run(raw_videos, dummy_recorder, type_="detail", tiktok=False)
            
            logger.info(f"从 Extractor 转换后得到 {len(formatted_videos)} 个视频，包含 downloads 字段")

            # 找到目标视频
            target_video = None
            if target_video_id:
                for v in formatted_videos:
                    if str(v.get("id")) == str(target_video_id):
                        target_video = v
                        break
                if not target_video:
                    logger.error(f"未找到视频 {target_video_id}，可能已被删除或在列表中不存在")
                    return False, ""
            else:
                target_video = formatted_videos[0]
            
            # 提取下载链接 - 使用 downloads 字段（特殊下载链接）
            video_id = target_video.get("id")
            video_desc = target_video.get("desc", "")
            
            download_url = None
            downloads = target_video.get("downloads", [])
            if downloads and len(downloads) > 0:
                # downloads 可能是字符串或者列表
                if isinstance(downloads, str):
                    download_url = downloads
                elif isinstance(downloads, list) and len(downloads) > 0:
                    download_url = downloads[0]
            
            if not download_url:
                logger.error(f"视频 {video_id} 没有 downloads 字段（特殊下载链接）")
                logger.error(f"视频数据: {target_video}")
                return False, video_desc
            
            logger.info(f"找到视频 {video_id}，使用特殊下载链接: {download_url[:80]}...")
            
            # 直接用 httpx 下载视频
            async with httpx.AsyncClient(timeout=300.0) as client:
                resp = await client.get(download_url, follow_redirects=True)
                resp.raise_for_status()
                
                with open(output_path, "wb") as f:
                    f.write(resp.content)
                
                file_size = os.path.getsize(output_path)
                logger.info(f"视频下载成功: {output_path}, 大小: {file_size / 1024 / 1024:.2f} MB")
                return True, video_desc

    except Exception as e:
        logger.error(f"视频下载失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False, ""


def upload_to_r2_simple(file_path: str, object_name: str) -> Optional[str]:
    if not all(R2_CONFIG.values()):
        logger.error("R2 配置不完整")
        return None

    try:
        import boto3
        from botocore.config import Config

        logger.info(f"开始上传到 R2: {object_name}")

        s3 = boto3.client(
            "s3",
            endpoint_url=R2_CONFIG["endpoint_url"],
            aws_access_key_id=R2_CONFIG["aws_access_key_id"],
            aws_secret_access_key=R2_CONFIG["aws_secret_access_key"],
            config=Config(signature_version="s3v4"),
            region_name="auto",
        )

        s3.upload_file(file_path, R2_BUCKET, object_name)

        if R2_PUBLIC_URL:
            download_url = f"{R2_PUBLIC_URL.rstrip('/')}/{object_name}"
        else:
            download_url = f"{R2_CONFIG['endpoint_url']}/{R2_BUCKET}/{object_name}"

        logger.info(f"上传成功: {download_url}")
        return download_url

    except Exception as e:
        logger.error(f"R2 上传失败: {e}")
        return None


async def notify_callback_simple(task_id: str, chat_id: int, download_url: Optional[str], caption: Optional[str], success: bool):
    logger.info(f"DEBUG: CALLBACK_URL = {CALLBACK_URL}")
    if not CALLBACK_URL:
        logger.warning("未配置回调地址，跳过通知")
        return

    max_retries = 5
    retry_delays = [2, 5, 10, 20, 30]

    for attempt in range(1, max_retries + 1):
        try:
            payload = {
                "task_id": task_id,
                "chat_id": chat_id,
                "download_url": download_url,
                "caption": caption,
                "success": success,
                "error": None if success else "处理失败"
            }

            headers = {}
            if AUTH_TOKEN:
                headers["X-Auth-Token"] = AUTH_TOKEN

            logger.info(f"发送回调 (尝试 {attempt}/{max_retries}) 到 {CALLBACK_URL}")
            callback_url = CALLBACK_URL
            if not callback_url.startswith('http'):
                callback_url = 'https://' + callback_url
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=30.0, read=60.0, write=30.0)) as client:
                resp = await client.post(callback_url, json=payload, headers=headers)
                resp.raise_for_status()
                logger.info(f"回调通知成功: {resp.status_code}")
                return

        except httpx.TimeoutException as e:
            logger.warning(f"回调通知超时 (尝试 {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                logger.info(f"{delay}秒后重试...")
                await asyncio.sleep(delay)
        except httpx.ConnectError as e:
            logger.warning(f"回调连接错误 (尝试 {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                logger.info(f"{delay}秒后重试...")
                await asyncio.sleep(delay)
        except httpx.HTTPStatusError as e:
            logger.warning(f"回调 HTTP 错误 (尝试 {attempt}/{max_retries}): {e.response.status_code}")
            if attempt < max_retries and e.response.status_code >= 500:
                delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                logger.info(f"{delay}秒后重试...")
                await asyncio.sleep(delay)
            else:
                logger.error(f"回调通知失败 (不可重试的错误): {e}")
                return
        except Exception as e:
            logger.error(f"回调通知失败 (尝试 {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                logger.info(f"{delay}秒后重试...")
                await asyncio.sleep(delay)
            else:
                import traceback
                traceback.print_exc()

    logger.error(f"回调通知最终失败，已重试 {max_retries} 次")


async def process_video_task(
    task_id: str,
    video_url: str,
    chat_id: int,
    caption: Optional[str] = None,
    watermark_text: Optional[str] = None,
    generate_ai_caption: bool = False,
    video_desc: Optional[str] = None,
    caption_style: Optional[str] = None,
):
    input_path = f"/tmp/{task_id}_in.mp4"
    output_path = f"/tmp/exports/{task_id}_out.mp4"
    stream_path = f"/tmp/exports/{task_id}_stream.mp4"

    os.makedirs("/tmp/exports", exist_ok=True)

    # CF Worker 已负责去重，这里不再检查数据库

    try:
        success = False
        download_url = None
        final_caption = caption or video_desc or "视频处理完成"
        if final_caption and len(final_caption) > 200:
            final_caption = final_caption[:197] + "..."

        success, fresh_video_desc = await download_video_real(video_url, input_path, task_id)
        if not success:
            await notify_callback_simple(task_id, chat_id, None, None, False)
            return

        if fresh_video_desc:
            final_caption = fresh_video_desc

        temp_watermark = "/tmp/watermark.png"

        cmd = [
            "ffmpeg", "-i", input_path,
            "-i", temp_watermark,
            "-filter_complex", "[0:v]scale=iw*0.99:ih*0.99[v_scaled];[v_scaled]pad=ceil(iw/2)*2:ceil(ih/2)*2[v_padded];[v_padded][1:v]overlay=main_w-overlay_w-10:main_h-overlay_h-10[v_watermarked]",
            "-map", "[v_watermarked]", "-map", "0:a?",
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-map_metadata", "-1",
            "-movflags", "+faststart",
            output_path, "-y"
        ]

        logger.info(f"执行 FFmpeg 水印处理...")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"FFmpeg 水印处理失败: {result.stderr}")
            shutil.copy(input_path, output_path)
        else:
            logger.info("FFmpeg 水印处理完成")

        if generate_ai_caption and video_desc:
            try:
                logger.info("开始生成 AI 文案...")
                ai_caption = await generate_ai_caption_sync(video_desc, caption_style)
                if ai_caption:
                    final_caption = ai_caption
                    logger.info(f"AI 文案生成成功: {final_caption}")
                else:
                    logger.warning("AI 文案生成失败，使用原始描述")
            except Exception as e:
                logger.error(f"AI 文案生成异常: {e}")

        stream_cmd = [
            "ffmpeg", "-y", "-i", output_path,
            "-c:v", "libx264", "-preset", "fast",
            "-movflags", "+faststart",
            stream_path
        ]
        stream_result = subprocess.run(stream_cmd, capture_output=True, text=True)
        if stream_result.returncode == 0:
            logger.info("视频流优化完成")
            final_path = stream_path
        else:
            logger.warning(f"视频流优化失败，使用原文件: {stream_result.stderr}")
            final_path = output_path

        download_url = upload_to_r2_simple(final_path, f"{task_id}_out.mp4")

        if not download_url:
            logger.error("R2 上传失败")
            await notify_callback_simple(task_id, chat_id, None, None, False)
            return

        success = True
        await notify_callback_simple(task_id, chat_id, download_url, final_caption, success)

    except Exception as e:
        logger.error(f"任务处理异常: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await notify_callback_simple(task_id, chat_id, None, None, False)

    finally:
        # CF Worker 负责任务管理，这里只清理临时文件
        for p in [input_path, output_path, stream_path]:
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass


async def generate_ai_caption_sync(text: str, style: Optional[str] = None) -> Optional[str]:
    """同步生成 AI 文案（基于 Groq API）"""
    import httpx
    import os

    if not text:
        return None

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        logger.warning("GROQ_API_KEY 未配置，跳过 AI 文案")
        return None

    model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
    is_short = style in ("humor", "short", "乐子")

    if is_short:
        system_prompt = "你是一个Telegram段子手。请根据内容写一个20字以内的简短、幽默、吸引人的文案。直接输出文案，不要解释。"
        max_tokens = 50
    else:
        system_prompt = "你是一个电报(Telegram)爆款博主。请根据口播内容写一个70字左右的文案。要求：语言犀利、大胆，吸引眼球。直接总结输出文案本身，不要解释。"
        max_tokens = 150

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"内容如下：{text}"}
        ],
        "temperature": 1,
        "max_completion_tokens": max_tokens
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers=headers,
                json=payload
            )
        if response.status_code == 200:
            result = response.json()
            caption = result["choices"][0]["message"]["content"].strip()
            caption = caption.replace("\n", "").replace('"', "'").replace("'", "'")
            if len(caption) > 75:
                caption = caption[:72] + "..."
            return caption
        else:
            logger.error(f"Groq API 错误: {response.status_code} {response.text[:200]}")
    except Exception as e:
        logger.error(f"Groq API 请求异常: {e}")

    return None


@app.get("/")
async def root():
    return {"status": "running", "service": "dy2tg-video-processor"}


@app.get("/wake")
async def wake_up():
    logger.info("收到唤醒请求")
    return {"status": "awake", "timestamp": time.time()}


@app.get("/debug")
async def debug_info():
    return {
        "status": "running",
        "R2_CONFIG": {
            "endpoint_url": R2_CONFIG["endpoint_url"][:20] + "..." if R2_CONFIG["endpoint_url"] else "",
            "has_access_key": bool(R2_CONFIG["aws_access_key_id"]),
            "has_secret_key": bool(R2_CONFIG["aws_secret_access_key"]),
        },
        "R2_BUCKET": R2_BUCKET,
        "R2_PUBLIC_URL": R2_PUBLIC_URL[:30] + "..." if R2_PUBLIC_URL else "",
        "CALLBACK_URL": CALLBACK_URL[:30] + "..." if CALLBACK_URL else "",
        "DOUYIN_COOKIE": os.getenv("DOUYIN_COOKIE", "")[:20] + "..." if os.getenv("DOUYIN_COOKIE") else "",
    }


@app.post("/enqueue")
async def enqueue_task(task: TaskRequest, background_tasks: BackgroundTasks):
    if not task.video_url:
        raise HTTPException(status_code=400, detail="video_url 不能为空")

    if not CALLBACK_URL:
        raise HTTPException(status_code=500, detail="服务端未配置回调地址")

    logger.info(f"收到任务: {task.task_id}, video_url: {task.video_url}")

    background_tasks.add_task(
        process_video_task,
        task.task_id,
        task.video_url,
        task.chat_id,
        task.caption,
        task.watermark_text,
        task.generate_ai_caption,
        task.video_desc,
        task.caption_style
    )

    return TaskResponse(
        status="processing",
        task_id=task.task_id,
        message="任务已加入处理队列"
    )


@app.get("/status/{task_id}")
async def get_task_status(task_id: str):
    if R2_PUBLIC_URL:
        download_url = f"{R2_PUBLIC_URL.rstrip('/')}/{task_id}_out.mp4"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.head(download_url)
                if resp.status_code == 200:
                    return {"task_id": task_id, "status": "completed", "download_url": download_url}
        except:
            pass
        return {"task_id": task_id, "status": "processing", "download_url": None}

    output_file = OUTPUT_DIR / f"{task_id}_out.mp4"
    if output_file.exists():
        return {"task_id": task_id, "status": "completed", "download_url": f"/outputs/{task_id}_out.mp4"}

    return {"task_id": task_id, "status": "processing"}


async def process_video_cli():
    """GitHub Actions CLI 模式"""
    video_url = os.getenv("VIDEO_URL")
    task_id = os.getenv("TASK_ID")
    chat_id = os.getenv("CHAT_ID")
    video_desc = os.getenv("VIDEO_DESC", "")

    if not video_url or not task_id:
        logger.error("缺少 VIDEO_URL 或 TASK_ID 环境变量")
        return False

    logger.info(f"开始处理视频: {video_url}, task_id: {task_id}")

    try:
        chat_id_int = int(chat_id) if chat_id else 0

        await process_video_task(
            task_id=task_id,
            video_url=video_url,
            chat_id=chat_id_int,
            caption=None,
            watermark_text=None,
            generate_ai_caption=False,
            video_desc=video_desc or None
        )

        return True
    except Exception as e:
        logger.error(f"处理异常: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    if os.getenv("GITHUB_ACTIONS") == "true" or os.getenv("VIDEO_URL"):
        result = asyncio.run(process_video_cli())
        sys.exit(0 if result else 1)
    else:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=7860)
