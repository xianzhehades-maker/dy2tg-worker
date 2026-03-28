"""AI 文案生成模块 - 支持英伟达 NIM 模型和 Groq 模型"""
import os
import sys
import io
import subprocess
import time
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import DatabaseManager
import config

from hf.utils.logger import get_logger

logger = get_logger("ai_caption")

SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FFMPEG_PATH = os.path.join(SCRIPT_DIR, "ffmpeg.exe")

if not os.path.exists(FFMPEG_PATH):
    FFMPEG_PATH = "ffmpeg"

try:
    from faster_whisper import WhisperModel
    WHISPER_AVAILABLE = True
except ImportError:
    WHISPER_AVAILABLE = False
    print("⚠️ faster-whisper 未安装，请运行: pip install faster-whisper")

try:
    from groq import Groq as GroqClient
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False
    print("⚠️ groq 未安装，请运行: pip install groq")

_whisper_model = None

def get_whisper_model():
    global _whisper_model
    if _whisper_model is None and WHISPER_AVAILABLE:
        print(f"📥 加载 Whisper 模型: small (设备: cpu, 计算类型: int8)")
        try:
            _whisper_model = WhisperModel(
                "small",
                device="cpu",
                compute_type="int8"
            )
            print(f"✅ Whisper 模型加载成功")
        except Exception as e:
            print(f"❌ Whisper 模型加载失败: {e}")
            return None
    return _whisper_model

def extract_audio(video_path, audio_path="temp_audio.mp3"):
    print(f"-> 正在从 {os.path.basename(video_path)} 提取音频...")
    
    if not os.path.exists(video_path):
        print(f"❌ 视频文件不存在: {video_path}")
        return None
    
    command = [
        FFMPEG_PATH, '-i', video_path,
        '-vn', '-acodec', 'libmp3lame',
        '-ar', '16000', '-ac', '1',
        '-y', audio_path
    ]
    
    try:
        result = subprocess.run(
            command, 
            stdout=subprocess.DEVNULL, 
            stderr=subprocess.PIPE, 
            text=True,
            timeout=300
        )
    except subprocess.TimeoutExpired:
        print(f"❌ 音频提取超时")
        return None
    
    if result.returncode != 0:
        print(f"❌ 音频提取失败 (返回码: {result.returncode})")
        if result.stderr:
            print(f"   错误信息: {result.stderr[:500]}")
        return None
    
    if os.path.exists(audio_path):
        file_size = os.path.getsize(audio_path)
        print(f"✅ 音频提取成功: {audio_path} (大小: {file_size/1024:.1f} KB)")
        return audio_path
    else:
        print(f"❌ 音频文件未生成: {audio_path}")
        return None

def get_transcript_local(audio_path):
    print("-> 正在进行本地语音转文字 (faster-whisper)...")
    
    model = get_whisper_model()
    if model is None:
        print("   ❌ Whisper 模型未加载")
        return None
    
    print(f"   [DEBUG] 音频文件: {audio_path}")
    print(f"   [DEBUG] 文件大小: {os.path.getsize(audio_path)/1024:.1f} KB")
    
    try:
        print(f"   [DEBUG] 开始转录...")
        segments, info = model.transcribe(audio_path, beam_size=5)
        
        print(f"   [DEBUG] 检测到语言: {info.language} (概率: {info.language_probability:.2f})")
        
        text_parts = []
        for segment in segments:
            text_parts.append(segment.text)
        
        full_text = " ".join(text_parts).strip()
        print(f"   ✅ 转录成功，文本长度: {len(full_text)} 字符")
        return full_text
        
    except Exception as e:
        print(f"   ❌ 转录失败: {type(e).__name__}: {str(e)[:200]}")
        import traceback
        traceback.print_exc()
        return None

def generate_tg_caption_nvidia(text, max_retries=3, retry_delay=2):
    """使用英伟达 NIM 模型生成文案"""
    print("-> 正在使用英伟达 NIM 生成精炼文案...")
    
    import httpx
    
    _ai_config = config.load_json_config('ai_config.json')
    base_url = os.environ.get("NVIDIA_NIM_URL", _ai_config.get('nvidia_nim_url', 'http://localhost:8000/v1'))
    api_key = os.environ.get("NVIDIA_API_KEY", _ai_config.get('nvidia_api_key', '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'))
    model_name = os.environ.get("NVIDIA_MODEL", _ai_config.get('nvidia_model', 'qwen/qwen3.5-397b-a17b'))
    
    print(f"   [DEBUG] NIM URL: {base_url}")
    print(f"   [DEBUG] 模型: {model_name}")
    print(f"   [DEBUG] 输入文本长度: {len(text)} 字符")
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    payload = {
        "model": model_name,
        "messages": [
            {
                "role": "system",
                "content": "你是一个电报(Telegram)爆款博主。请根据口播内容写一个70字左右的文案。要求：语言犀利、大胆，吸引眼球、直接总结输出文案本身，不要解释。"
            },
            {
                "role": "user",
                "content": f"内容如下：{text}"
            }
        ],
        "max_tokens": 1024
    }
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"   [尝试 {attempt}/{max_retries}] 调用英伟达 NIM API...")
            
            response = httpx.post(
                f"{base_url}/chat/completions",
                headers=headers,
                json=payload,
                timeout=120
            )

            if response.status_code == 200:
                result = response.json()
                caption = result["choices"][0]["message"]["content"]
                print(f"   ✅ 文案生成成功: {caption}")
                return caption
            else:
                print(f"   ❌ API 返回错误: {response.status_code}")
                print(f"   响应: {response.text[:500]}")

        except Exception as e:
            error_msg = str(e)
            print(f"   ❌ 尝试 {attempt} 失败: {type(e).__name__}: {error_msg[:200]}")

            if attempt < max_retries:
                print(f"   ⏳ {retry_delay}秒后重试...")
                time.sleep(retry_delay)
            else:
                print(f"   ⚠️ NIM API 失败，使用原始文本作为文案")
                if text and len(text) > 0:
                    return text[:200] if len(text) > 200 else text
                return None
    
    return None

def generate_tg_caption_groq(text, max_retries=3, retry_delay=2):
    """使用 Groq 模型生成文案"""
    if not GROQ_AVAILABLE:
        print("   ⚠️ Groq SDK 未安装，跳过")
        return None

    print("-> 正在使用 Groq 生成精炼文案...")

    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        _ai_config = config.load_json_config('ai_config.json')
        api_key = _ai_config.get('groq_api_key')

    if not api_key:
        print("   ⚠️ 未配置 GROQ_API_KEY，跳过")
        return None

    model_name = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
    print(f"   [DEBUG] Groq 模型: {model_name}")
    print(f"   [DEBUG] 输入文本长度: {len(text)} 字符")

    for attempt in range(1, max_retries + 1):
        try:
            print(f"   [尝试 {attempt}/{max_retries}] 调用 Groq API...")

            client = GroqClient(api_key=api_key)
            completion = client.chat.completions.create(
                model=model_name,
                messages=[
                    {
                        "role": "system",
                        "content": "你是一个电报(Telegram)爆款博主。请根据口播内容写一个70字左右的文案。要求：语言犀利、大胆，吸引眼球、直接总结输出文案本身，不要解释。"
                    },
                    {
                        "role": "user",
                        "content": f"内容如下：{text}"
                    }
                ],
                temperature=1,
                max_completion_tokens=1024
            )

            caption = completion.choices[0].message.content.strip()
            caption = caption.replace("\n", "").replace('"', '').replace("'", "")
            if len(caption) > 75:
                caption = caption[:72] + "..."
            print(f"   ✅ 文案生成成功: {caption}")
            return caption

        except Exception as e:
            print(f"   ❌ 尝试 {attempt} 失败: {type(e).__name__}: {str(e)[:200]}")
            if attempt < max_retries:
                time.sleep(retry_delay)

    print(f"   ⚠️ Groq API 失败，使用原始文本作为文案")
    if text and len(text) > 0:
        return text[:200] if len(text) > 200 else text
    return None

def generate_short_caption_nvidia(text, max_retries=3, retry_delay=2):
    """使用英伟达 NIM 模型生成简短吸引人的文案（乐子组用，20字以内）"""
    if not text:
        return None

    print("-> 正在生成简短吸引人文案...")

    import httpx

    _ai_config = config.load_json_config('ai_config.json')
    base_url = os.environ.get("NVIDIA_NIM_URL", _ai_config.get('nvidia_nim_url', 'http://localhost:8000/v1'))
    api_key = os.environ.get("NVIDIA_API_KEY", _ai_config.get('nvidia_api_key', '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'))
    model_name = os.environ.get("NVIDIA_MODEL", _ai_config.get('nvidia_model', 'qwen/qwen3.5-397b-a17b'))

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    payload = {
        "model": model_name,
        "messages": [
            {
                "role": "system",
                "content": "你是一个Telegram段子手。请根据视频标题/描述，写一个20字以内的简短、幽默、吸引人的文案。要求：押韵或巧用谐音梗，直接输出文案本身，不要解释。"
            },
            {
                "role": "user",
                "content": f"视频描述如下：{text}"
            }
        ],
        "max_tokens": 256
    }

    for attempt in range(1, max_retries + 1):
        try:
            print(f"   [尝试 {attempt}/{max_retries}] 调用英伟达 NIM API (短文案)...")

            response = httpx.post(
                f"{base_url}/chat/completions",
                headers=headers,
                json=payload,
                timeout=120
            )

            if response.status_code == 200:
                result = response.json()
                caption = result["choices"][0]["message"]["content"].strip()
                caption = caption.replace("\n", "").replace('"', '').replace("'", "")
                if len(caption) > 25:
                    caption = caption[:22] + "..."
                print(f"   ✅ 短文案生成成功: {caption}")
                return caption
            else:
                print(f"   ❌ API 返回错误: {response.status_code}")

        except Exception as e:
            print(f"   ❌ 尝试 {attempt} 失败: {type(e).__name__}: {str(e)[:200]}")
            if attempt < max_retries:
                time.sleep(retry_delay)

    print(f"   ⚠️ Groq API 失败，使用原始文本作为文案")
    if text and len(text) > 0:
        return text[:200] if len(text) > 200 else text
    return None

def generate_short_caption_groq(text, max_retries=3, retry_delay=2):
    """使用 Groq 模型生成简短文案"""
    if not GROQ_AVAILABLE:
        print("   ⚠️ Groq SDK 未安装，跳过")
        return None

    print("-> 正在使用 Groq 生成简短文案...")

    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        _ai_config = config.load_json_config('ai_config.json')
        api_key = _ai_config.get('groq_api_key')

    if not api_key:
        print("   ⚠️ 未配置 GROQ_API_KEY，跳过")
        return None

    model_name = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")

    for attempt in range(1, max_retries + 1):
        try:
            print(f"   [尝试 {attempt}/{max_retries}] 调用 Groq API (短文案)...")

            client = GroqClient(api_key=api_key)
            completion = client.chat.completions.create(
                model=model_name,
                messages=[
                    {
                        "role": "system",
                        "content": "你是一个Telegram段子手。请根据视频标题/描述，写一个20字以内的简短、幽默、吸引人的文案。要求：押韵或巧用谐音梗，直接输出文案本身，不要解释。"
                    },
                    {
                        "role": "user",
                        "content": f"视频描述如下：{text}"
                    }
                ],
                temperature=1,
                max_completion_tokens=256
            )

            caption = completion.choices[0].message.content.strip()
            caption = caption.replace("\n", "").replace('"', '').replace("'", "")
            if len(caption) > 25:
                caption = caption[:22] + "..."
            print(f"   ✅ 短文案生成成功: {caption}")
            return caption

        except Exception as e:
            print(f"   ❌ 尝试 {attempt} 失败: {type(e).__name__}: {str(e)[:200]}")
            if attempt < max_retries:
                time.sleep(retry_delay)

    print(f"   ⚠️ Groq API 失败，使用原始文本作为文案")
    if text and len(text) > 0:
        return text[:20] + "..." if len(text) > 20 else text
    return None

def generate_short_caption(text, max_retries=3, retry_delay=2):
    """从文本生成简短文案"""
    if not text:
        return None

    if GROQ_AVAILABLE and os.environ.get("GROQ_API_KEY"):
        return generate_short_caption_groq(text, max_retries, retry_delay)
    else:
        print("   ⚠️ 未配置 GROQ_API_KEY，使用原始文本")
        return text[:20] + "..." if len(text) > 20 else text

def process_video_to_caption(video_path):
    audio_path = os.path.join(SCRIPT_DIR, "temp_audio.mp3")

    try:
        audio_result = extract_audio(video_path, audio_path)
        if not audio_result:
            print("❌ 音频提取失败，跳过此任务")
            return None

        raw_text = get_transcript_local(audio_path)
        if not raw_text or not raw_text.strip():
            print("❌ 语音转文字结果为空")
            return None

        print(f"✅ 语音转文字成功，文本长度: {len(raw_text)} 字符")
        print(f"   [预览] {raw_text[:100]}...")

        if GROQ_AVAILABLE and os.environ.get("GROQ_API_KEY"):
            final_caption = generate_tg_caption_groq(raw_text, max_retries=3, retry_delay=2)
        else:
            print("   ⚠️ 未配置 GROQ_API_KEY，使用原始文本")
            final_caption = raw_text[:200] if len(raw_text) > 200 else raw_text
        return final_caption

    except Exception as e:
        print(f"❌ 处理失败: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if os.path.exists(audio_path):
            try:
                os.remove(audio_path)
            except:
                pass

def process_ai_caption_task(task_id=None, limit=10):
    """处理AI文案生成任务"""
    db_manager = DatabaseManager()
    
    print("🤖 AI 文案生成模块 - 英伟达 NIM 版本")
    print("💡 使用 Qwen 3.5 397B 模型生成爆款文案")
    
    if task_id:
        tasks = [db_manager.get_task(task_id)]
    else:
        tasks = db_manager.get_tasks(status="watermarked", limit=limit)
    
    if not tasks:
        print("📭 没有需要生成文案的任务")
        return
    
    print(f"📋 找到 {len(tasks)} 个待处理任务")
    
    processed_count = 0
    
    for task in tasks:
        if not task:
            continue
            
        print(f"\n{'='*60}")
        print(f"🔄 处理任务 {task.id}")
        print(f"{'='*60}")
        
        try:
            processed_dir = config.PROCESSED_PATH
            upload_dir = config.UPLOAD_PATH
            
            file_path = task.file_path
            if file_path and os.path.exists(file_path):
                pass
            else:
                found = False
                for directory in [processed_dir, upload_dir]:
                    for suffix in ["_watermarked.mp4", ".mp4"]:
                        candidate = os.path.join(directory, f"{task.id}{suffix}")
                        if os.path.exists(candidate):
                            file_path = candidate
                            found = True
                            break
                    if found:
                        break
                
                if not found:
                    print(f"⚠️ 任务 {task.id} 文件不存在")
                    db_manager.update_task(task.id, status="error", error_msg="文件不存在")
                    continue
            
            print(f"📁 文件路径: {file_path}")
            
            caption = process_video_to_caption(file_path)
            
            if caption:
                db_manager.update_task(
                    task.id,
                    status="ai_captioned",
                    ai_caption=caption,
                    ai_caption_time=datetime.now()
                )
                processed_count += 1
                print(f"✅ 任务 {task.id} 处理完成")
            else:
                print(f"❌ 任务 {task.id} 文案生成失败")
                db_manager.update_task(
                    task.id,
                    status="error",
                    error_msg="AI文案生成失败"
                )
            
        except Exception as e:
            print(f"❌ 任务 {task.id} 处理失败: {e}")
            db_manager.update_task(
                task.id,
                status="error",
                error_msg=f"AI文案生成失败: {str(e)}"
            )
    
    print(f"\n🏁 处理完成，共处理 {processed_count} 个任务")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-id", type=int, help="指定任务ID")
    parser.add_argument("--limit", type=int, default=10, help="处理任务数量限制")
    args = parser.parse_args()
    
    process_ai_caption_task(task_id=args.task_id, limit=args.limit)
