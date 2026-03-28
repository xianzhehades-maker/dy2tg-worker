"""文件管理工具"""

import os
import shutil
import logging

logger = logging.getLogger(__name__)


class FileManager:
    """文件管理器 - 负责查找和清理视频文件"""

    def __init__(
        self,
        download_path: str = None,
        processed_path: str = None,
        upload_path: str = None
    ):
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        self.download_path = download_path or os.path.join(base_dir, 'data', 'downloads')
        self.processed_path = processed_path or os.path.join(base_dir, 'data', 'processed')
        self.upload_path = upload_path or os.path.join(base_dir, 'data', 'uploads')

        os.makedirs(self.download_path, exist_ok=True)
        os.makedirs(self.processed_path, exist_ok=True)
        os.makedirs(self.upload_path, exist_ok=True)

    def find_video_file(self, task_id: int) -> str:
        """
        查找任务的视频文件

        查找顺序：
        1. {upload_path}/{task_id}_watermarked.mp4
        2. {processed_path}/{task_id}_watermarked.mp4
        3. {processed_path}/{task_id}.mp4
        4. {download_path}/{task_id}.mp4
        """
        search_paths = [
            os.path.join(self.upload_path, f"{task_id}_watermarked.mp4"),
            os.path.join(self.processed_path, f"{task_id}_watermarked.mp4"),
            os.path.join(self.processed_path, f"{task_id}.mp4"),
            os.path.join(self.download_path, f"{task_id}.mp4"),
        ]

        for path in search_paths:
            if os.path.exists(path):
                logger.info(f"找到视频文件: {path}")
                return path

        logger.warning(f"未找到任务 {task_id} 的视频文件")
        return None

    def cleanup(self, task_id: int):
        """
        清理任务相关文件

        删除：
        - {task_id}.mp4
        - {task_id}_watermarked.mp4
        - {task_id}_stream.mp4
        """
        suffixes = ['', '_watermarked', '_stream']
        dirs = [self.upload_path, self.processed_path, self.download_path]

        cleaned = []
        for suffix in suffixes:
            for dir_path in dirs:
                file_path = os.path.join(dir_path, f"{task_id}{suffix}.mp4")
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        cleaned.append(file_path)
                        logger.info(f"已删除: {file_path}")
                    except Exception as e:
                        logger.error(f"删除文件失败 {file_path}: {e}")

        return cleaned

    def get_file_size(self, file_path: str) -> int:
        """获取文件大小（字节）"""
        if os.path.exists(file_path):
            return os.path.getsize(file_path)
        return 0

    def copy_to_processed(self, src_path: str, task_id: int, suffix: str = '') -> str:
        """复制文件到processed目录"""
        ext = os.path.splitext(src_path)[1]
        dest_path = os.path.join(self.processed_path, f"{task_id}{suffix}{ext}")
        shutil.copy2(src_path, dest_path)
        logger.info(f"已复制到: {dest_path}")
        return dest_path
