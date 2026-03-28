"""上传器接口定义"""

from abc import ABC, abstractmethod
from typing import Tuple


class IUploader(ABC):
    """上传器接口"""

    @abstractmethod
    async def upload(self, task_id: int, caption: str, group_id: int) -> Tuple[bool, str]:
        """
        上传任务

        Args:
            task_id: 任务ID
            caption: 文案
            group_id: 分组ID

        Returns:
            (success: bool, error_msg: str or None)
        """
        pass

    @abstractmethod
    async def upload_file(self, file_path: str, task_id: int) -> bool:
        """上传单个文件"""
        pass
