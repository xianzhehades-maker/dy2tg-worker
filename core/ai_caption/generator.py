"""AI文案生成模块 - 支持中英双语"""

import os
import re
import asyncio
import subprocess
from pathlib import Path
from typing import Optional

import httpx


class AICaptionGenerator:
    def __init__(
        self,
        groq_api_key: str = None,
        whisper_model: str = "base",
        language: str = "chinese",
    ):
        self.groq_api_key = groq_api_key or os.getenv("GROQ_API_KEY", "")
        self.whisper_model = whisper_model
        self.language = language

        self.groq_api_url = "https://api.groq.com/openai/v1/chat/completions"

    async def generate_caption(
        self,
        video_path: str,
        original_desc: str = "",
        style: str = "default",
        length: int = 200,
        language: str = "chinese",
    ) -> str:
        try:
            audio_path = await self._extract_audio(video_path)
            if not audio_path:
                return original_desc

            transcript = await self._transcribe_audio(audio_path)
            if not transcript:
                return original_desc

            if language == "bilingual":
                return await self._generate_bilingual(transcript, original_desc, style, length)
            elif language == "english":
                return await self._generate_english(transcript, original_desc, style, length)
            else:
                return await self._generate_chinese(transcript, original_desc, style, length)

        except Exception as e:
            return original_desc

    async def _extract_audio(self, video_path: str) -> Optional[str]:
        audio_path = video_path.replace(Path(video_path).suffix, ".mp3")

        cmd = [
            "ffmpeg", "-y", "-i", video_path,
            "-vn", "-acodec", "libmp3lame",
            "-q:a", "5", audio_path
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120
            )
            if result.returncode == 0 and os.path.exists(audio_path):
                return audio_path
        except Exception:
            pass

        return None

    async def _transcribe_audio(self, audio_path: str) -> Optional[str]:
        try:
            import faster_whisper

            model = faster_whisper.load_model(self.whisper_model)
            segments, _ = model.transcribe(audio_path)

            text_parts = []
            for segment in segments:
                text_parts.append(segment.text.strip())

            return " ".join(text_parts) if text_parts else None

        except Exception:
            return None

    async def _generate_chinese(self, transcript: str, original_desc: str, style: str, length: int) -> str:
        if style == "humor" or style == "short":
            prompt = f"""根据视频内容，生成一个有趣的短文案（{length}字以内）：

内容：{transcript}

要求：
1. 简短有趣，吸引眼球
2. {length}字以内
3. 直接返回文案，不要解释"""
        else:
            prompt = f"""根据视频内容，生成一个口播风格的文案：

内容：{transcript}

要求：
1. 自然流畅，适合口播
2. {length}字左右
3. 直接返回文案，不要解释"""

        return await self._call_groq(prompt, original_desc)

    async def _generate_english(self, transcript: str, original_desc: str, style: str, length: int) -> str:
        if style == "humor" or style == "short":
            prompt = f"""Based on the video content, generate a fun short caption (within {length} chars):

Content: {transcript}

Requirements:
1. Short and fun, eye-catching
2. Within {length} characters
3. Return only the caption, no explanation"""
        else:
            prompt = f"""Based on the video content, generate a natural spoken-style caption:

Content: {transcript}

Requirements:
1. Natural and fluent, suitable for voiceover
2. Around {length} characters
3. Return only the caption, no explanation"""

        return await self._call_groq(prompt, original_desc)

    async def _generate_bilingual(self, transcript: str, original_desc: str, style: str, length: int) -> str:
        if style == "humor" or style == "short":
            prompt = f"""根据视频内容，生成中英双语文案：

内容：{transcript}

格式要求：
中文短文案（50字内）
---
English short caption (within 50 chars)

要求：有趣吸引眼球，直接返回双语文案，不要解释"""
        else:
            prompt = f"""根据视频内容，生成中英双语文案：

内容：{transcript}

格式要求：
中文文案（自然流畅的口播风格，{length}字左右）
---
English caption (natural spoken style, around {length} chars)

要求：直接返回双语文案，不要解释"""

        return await self._call_groq(prompt, original_desc)

    async def _call_groq(self, prompt: str, fallback: str = "") -> str:
        if not self.groq_api_key:
            return fallback

        headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json",
        }

        data = {
            "model": "llama-3.1-8b-instant",
            "messages": [
                {"role": "system", "content": "You are a helpful video caption generator."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.7,
            "max_tokens": 500,
        }

        try:
            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.post(
                    self.groq_api_url,
                    headers=headers,
                    json=data
                )

                if response.status_code == 200:
                    result = response.json()
                    return result["choices"][0]["message"]["content"].strip()

        except Exception:
            pass

        return fallback if fallback else ""


async def generate_caption(
    video_path: str,
    original_desc: str = "",
    style: str = "default",
    length: int = 200,
    language: str = "chinese",
) -> str:
    generator = AICaptionGenerator()
    return await generator.generate_caption(
        video_path=video_path,
        original_desc=original_desc,
        style=style,
        length=length,
        language=language,
    )