"""refactor_v2 核心模块"""

from .config import Config, get_config
from .database import DatabaseManager, Task, Monitor, Group, get_db
from .client import VideoClient, VideoInfo, Platform, VideoClientError, extract_video_id
from .workflow import WorkflowEngine, WorkflowTrigger, TaskStatus
from .bot.handler import BotCommandHandler, CommandResult, parse_command
from .cookie_checker import CookieChecker
from .retry_handler import RetryHandler, RetryConfig, RetryStrategy
from .streaming_opt import StreamingOptimizer, fix_video_for_streaming
from .watermark import WatermarkProcessor, create_image_watermark, add_video_watermark
from .uploader import WorkflowUploader, FileManager
from .ai_caption.generator import AICaptionGenerator, generate_caption

__all__ = [
    "Config",
    "get_config",
    "DatabaseManager",
    "Task",
    "Monitor",
    "Group",
    "get_db",
    "VideoClient",
    "VideoInfo",
    "Platform",
    "VideoClientError",
    "extract_video_id",
    "WorkflowEngine",
    "WorkflowTrigger",
    "TaskStatus",
    "BotCommandHandler",
    "CommandResult",
    "parse_command",
    "CookieChecker",
    "RetryHandler",
    "RetryConfig",
    "RetryStrategy",
    "StreamingOptimizer",
    "fix_video_for_streaming",
    "WatermarkProcessor",
    "create_image_watermark",
    "add_video_watermark",
    "WorkflowUploader",
    "FileManager",
    "AICaptionGenerator",
    "generate_caption",
]