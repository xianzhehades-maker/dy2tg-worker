"""视频流媒体优化 - FastStart"""

import os
import subprocess
from pathlib import Path
from typing import Optional


class StreamingOptimizer:
    def __init__(self, ffmpeg_path: str = "ffmpeg"):
        self.ffmpeg_path = ffmpeg_path

    def is_optimized(self, file_path: str) -> bool:
        try:
            result = subprocess.run(
                [self.ffmpeg_path, "-i", file_path],
                capture_output=True,
                text=True,
            )
            output = result.stderr + result.stdout
            return "moov" in output.lower() and "mdat" not in output.lower()
        except:
            return False

    def optimize(self, input_path: str, output_path: str = None) -> Optional[str]:
        if output_path is None:
            input_dir = os.path.dirname(input_path)
            input_name = os.path.basename(input_path)
            name, ext = os.path.splitext(input_name)
            output_path = os.path.join(input_dir, f"{name}_stream{ext}")

        if os.path.exists(output_path):
            return output_path

        try:
            cmd = [
                self.ffmpeg_path,
                "-y",
                "-i", input_path,
                "-c", "copy",
                "-movflags", "+faststart",
                output_path,
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,
            )

            if result.returncode == 0 and os.path.exists(output_path):
                return output_path

            return None

        except Exception:
            return None

    def optimize_if_needed(self, input_path: str) -> str:
        if not input_path.endswith(".mp4"):
            return input_path

        if "_stream" in input_path:
            return input_path

        if self.is_optimized(input_path):
            return input_path

        optimized = self.optimize(input_path)
        return optimized if optimized else input_path


def fix_video_for_streaming(video_path: str) -> str:
    optimizer = StreamingOptimizer()
    return optimizer.optimize_if_needed(video_path)