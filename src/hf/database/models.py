"""数据模型"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

@dataclass
class Customer:
    """客户数据模型"""
    id: Optional[int] = None
    name: str = ""
    user_id: str = ""
    homepage_url: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class Task:
    """任务数据模型"""
    id: Optional[int] = None
    group_id: Optional[int] = None
    customer_id: Optional[int] = None
    video_id: Optional[str] = None
    video_url: str = ""
    video_publish_time: Optional[datetime] = None
    download_url: Optional[str] = None
    status: str = "pending"
    download_time: Optional[datetime] = None
    watermark_time: Optional[datetime] = None
    ai_caption_time: Optional[datetime] = None
    upload_time: Optional[datetime] = None
    upload_bot_id: Optional[str] = None
    upload_channel_id: Optional[int] = None
    file_path: Optional[str] = None
    video_desc: Optional[str] = None
    ai_caption: Optional[str] = None
    error_msg: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class TaskPlan:
    """任务计划数据模型"""
    id: Optional[int] = None
    name: str = ""
    customer_ids: List[int] = None
    bot_config: dict = None
    execution_type: str = "once"
    target_count: Optional[int] = None
    interval_minutes: int = 30
    upload_template_id: Optional[int] = None
    enabled: bool = True
    workflow_steps: List[str] = None  # 工作流程步骤列表
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class Execution:
    """执行记录数据模型"""
    id: Optional[int] = None
    plan_id: Optional[int] = None
    mode: str = "manual"
    status: str = "stopped"
    current_step: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    cycle_count: int = 0
    cycle_duration: int = 0
    tasks_created: int = 0
    tasks_completed: int = 0
    tasks_failed: int = 0
    error_msg: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class UploadTemplate:
    """上传文字模板数据模型"""
    id: Optional[int] = None
    name: str = ""
    content: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class MonitorGroup:
    """监控分组数据模型"""
    id: Optional[int] = None
    name: str = ""
    promotion_text: Optional[str] = None
    ai_caption_style: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class GroupMonitor:
    """分组监控UP主数据模型"""
    id: Optional[int] = None
    group_id: int = 0
    up_name: str = ""
    up_url: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class GroupTarget:
    """分组目标频道数据模型"""
    id: Optional[int] = None
    group_id: int = 0
    target_channel: str = ""  # @username 或 chat_id
    chat_id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class SystemConfig:
    """系统配置数据模型"""
    id: Optional[int] = None
    config_key: str = ""
    config_value: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class DiscoveredVideo:
    """已发现的视频数据模型 - 用于记录所有发现的视频，避免重复处理"""
    id: Optional[int] = None
    customer_id: Optional[int] = None
    video_id: str = ""
    video_url: str = ""
    video_publish_time: Optional[datetime] = None
    discovered_at: Optional[datetime] = None
    is_qualified: bool = False  # 是否符合条件（不过旧、不重复等）
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
