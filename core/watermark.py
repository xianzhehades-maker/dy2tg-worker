"""视频水印处理器"""

import os
import subprocess
from pathlib import Path
from typing import Optional
from PIL import Image, ImageDraw, ImageFont


class WatermarkProcessor:
    def __init__(
        self,
        font_path: str = None,
        font_size: int = 24,
        text_color: str = "white",
        bg_color: str = "black",
        opacity: int = 128,
    ):
        self.font_path = font_path
        self.font_size = font_size
        self.text_color = text_color
        self.bg_color = bg_color
        self.opacity = opacity

    def create_text_image(self, text: str) -> Image.Image:
        """创建文字图片"""
        try:
            font = ImageFont.truetype(self.font_path, self.font_size)
        except:
            font = ImageFont.load_default()

        img = Image.new("RGBA", (800, 200), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        lines = []
        words = text.split()
        for word in words:
            if not lines:
                lines.append(word)
            else:
                last_line = lines[-1] + " " + word
                bbox = draw.textbbox((0, 0), last_line, font=font)
                if bbox[2] - bbox[0] < 750:
                    lines[-1] = last_line
                else:
                    lines.append(word)

        y = 10
        for line in lines:
            draw.text((10, y), line, font=font, fill=self.text_color)
            y += self.font_size + 5

        return img

    def add_watermark(
        self,
        video_path: str,
        text: str = None,
        output_path: str = None,
        position: str = "bottom-right",
    ) -> Optional[str]:
        """
        给视频添加文字水印

        Args:
            video_path: 视频路径
            text: 水印文字
            output_path: 输出路径
            position: 位置 (top-left, top-right, bottom-left, bottom-right)

        Returns:
            处理后的视频路径，失败返回None
        """
        if not text:
            return video_path

        if output_path is None:
            name, ext = os.path.splitext(video_path)
            output_path = f"{name}_watermarked{ext}"

        watermark_img_path = video_path + "_watermark.png"

        try:
            text_img = self.create_text_image(text)
            text_img.save(watermark_img_path)

            overlay_filter = f"overlay=10:10"
            if position == "top-right":
                overlay_filter = "overlay=main_w-overlay_w-10:10"
            elif position == "bottom-left":
                overlay_filter = "overlay=10:main_h-overlay_h-10"
            elif position == "bottom-right":
                overlay_filter = "overlay=main_w-overlay_w-10:main_h-overlay_h-10"

            cmd = [
                "ffmpeg", "-y",
                "-i", video_path,
                "-i", watermark_img_path,
                "-filter_complex", overlay_filter,
                "-codec:a", "copy",
                output_path,
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,
            )

            if os.path.exists(watermark_img_path):
                os.remove(watermark_img_path)

            if result.returncode == 0 and os.path.exists(output_path):
                return output_path

            return None

        except Exception:
            if os.path.exists(watermark_img_path):
                os.remove(watermark_img_path)
            return None


def create_image_watermark(
    text: str,
    output_path: str,
    font_path: str = None,
    font_size: int = 24,
) -> bool:
    """创建水印图片"""
    processor = WatermarkProcessor(
        font_path=font_path,
        font_size=font_size,
    )

    try:
        img = processor.create_text_image(text)
        img.save(output_path)
        return True
    except Exception:
        return False


def add_video_watermark(
    video_path: str,
    text: str,
    output_path: str = None,
    position: str = "bottom-right",
) -> Optional[str]:
    """给视频添加水印"""
    processor = WatermarkProcessor()
    return processor.add_watermark(video_path, text, output_path, position)