"""日志工具"""
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from datetime import datetime
from collections import deque

class ColorCodes:
    """ANSI 颜色代码"""
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    UNDERLINE = "\033[4m"
    
    # 前景色
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    
    # 背景色
    BG_BLACK = "\033[40m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"
    BG_MAGENTA = "\033[45m"
    BG_CYAN = "\033[46m"
    BG_WHITE = "\033[47m"

def colored(text: str, color: str) -> str:
    """给文本添加颜色"""
    if sys.platform == 'win32':
        # Windows 下需要启用 ANSI 颜色支持
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
        except:
            pass
    return f"{color}{text}{ColorCodes.RESET}"

def get_level_color(level: str) -> str:
    """根据日志级别获取颜色"""
    level_colors = {
        'DEBUG': ColorCodes.CYAN,
        'INFO': ColorCodes.GREEN,
        'WARNING': ColorCodes.YELLOW,
        'ERROR': ColorCodes.RED,
        'CRITICAL': ColorCodes.BOLD + ColorCodes.RED
    }
    return level_colors.get(level.upper(), ColorCodes.WHITE)

class ColoredFormatter(logging.Formatter):
    """带颜色的日志格式化器"""
    
    def format(self, record):
        level_color = get_level_color(record.levelname)
        level_name = colored(f"{record.levelname:<8}", level_color)
        time_str = colored(self.formatTime(record, '%Y-%m-%d %H:%M:%S'), ColorCodes.DIM)
        name_str = colored(record.name, ColorCodes.BLUE)
        message = record.getMessage()
        
        return f"{time_str} - {name_str} - {level_name} - {message}"

# 全局日志队列，用于存储最新的日志消息
log_queue = deque(maxlen=10000)  # 最多存储10000条日志

# 标记根日志记录器是否已初始化
_root_logger_initialized = False

def _ensure_root_logger_initialized():
    """确保根日志记录器已初始化"""
    global _root_logger_initialized
    if not _root_logger_initialized:
        get_logger("root")
        _root_logger_initialized = True

class QueueHandler(logging.Handler):
    """自定义日志处理器，将日志消息添加到队列"""
    def emit(self, record):
        try:
            # 确保记录有asctime属性
            if not hasattr(record, 'asctime'):
                # 手动添加时间戳
                import time
                record.asctime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(record.created))
            
            # 只存储消息内容，不存储格式化后的消息
            # 避免在GUI中重复显示时间戳和日志级别
            log_queue.append({
                'time': record.asctime,
                'type': record.levelname,
                'message': record.getMessage()
            })
        except Exception as e:
            # 记录错误但不中断
            print(f"QueueHandler错误: {e}", file=sys.stderr)

class StdoutRedirector:
    """重定向标准输出到日志"""
    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level
        self.old_stdout = sys.stdout
        self.buffer = []
        # 避免重复记录，检查消息是否已经是日志格式
        self.log_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} - .* - (INFO|WARNING|ERROR|DEBUG) - '
    
    def write(self, message):
        # 保留原始输出
        self.old_stdout.write(message)
        
        # 处理消息
        if message.strip():
            # 跳过已经是日志格式的消息，避免重复记录
            import re
            if not re.match(self.log_pattern, message.strip()):
                self.buffer.append(message)
                if '\n' in message:
                    # 当遇到换行符时，输出完整消息
                    full_message = ''.join(self.buffer).strip()
                    self.buffer = []
                    if full_message:  # 确保消息不为空
                        self.logger.log(self.level, full_message)
    
    def flush(self):
        # 确保缓冲区中的内容被输出
        if self.buffer:
            full_message = ''.join(self.buffer).strip()
            self.buffer = []
            if full_message:  # 确保消息不为空
                self.logger.log(self.level, full_message)
        self.old_stdout.flush()

class StderrRedirector:
    """重定向标准错误到日志"""
    def __init__(self, logger, level=logging.ERROR):
        self.logger = logger
        self.level = level
        self.old_stderr = sys.stderr
        self.buffer = []
        # 避免重复记录，检查消息是否已经是日志格式
        self.log_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} - .* - (INFO|WARNING|ERROR|DEBUG) - '
    
    def write(self, message):
        # 保留原始输出
        self.old_stderr.write(message)
        
        # 处理消息
        if message.strip():
            # 跳过已经是日志格式的消息，避免重复记录
            import re
            if not re.match(self.log_pattern, message.strip()):
                self.buffer.append(message)
                if '\n' in message:
                    # 当遇到换行符时，输出完整消息
                    full_message = ''.join(self.buffer).strip()
                    self.buffer = []
                    if full_message:  # 确保消息不为空
                        # 尝试根据消息内容判断日志级别
                        if any(keyword in full_message.lower() for keyword in ['error', 'exception', 'fail', 'traceback']):
                            self.logger.error(full_message)
                        elif any(keyword in full_message.lower() for keyword in ['warning', 'warn', 'attention']):
                            self.logger.warning(full_message)
                        else:
                            # 对于其他消息，使用INFO级别
                            self.logger.info(full_message)
    
    def flush(self):
        # 确保缓冲区中的内容被输出
        if self.buffer:
            full_message = ''.join(self.buffer).strip()
            self.buffer = []
            if full_message:  # 确保消息不为空
                # 尝试根据消息内容判断日志级别
                if any(keyword in full_message.lower() for keyword in ['error', 'exception', 'fail', 'traceback']):
                    self.logger.error(full_message)
                elif any(keyword in full_message.lower() for keyword in ['warning', 'warn', 'attention']):
                    self.logger.warning(full_message)
                else:
                    # 对于其他消息，使用INFO级别
                    self.logger.info(full_message)
        self.old_stderr.flush()

def get_logger(name: str = "app", level: str = "INFO") -> logging.Logger:
    """获取日志记录器"""
    
    # 确保根日志记录器已初始化（如果不是根日志记录器本身）
    if name != "root":
        _ensure_root_logger_initialized()
    
    logger = logging.getLogger(name)
    
    # 只为根日志记录器添加处理器
    if name == "root" and not logger.handlers:
        log_dir = os.environ.get("LOG_DIR", "logs")
        try:
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, f"{name}_{datetime.now().strftime('%Y-%m-%d')}.log")
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=10485760,
                backupCount=5,
                encoding='utf-8'
            )
            file_handler.setLevel(logging.DEBUG)
        except:
            log_dir = "/tmp/logs"
            try:
                os.makedirs(log_dir, exist_ok=True)
                log_file = os.path.join(log_dir, f"{name}_{datetime.now().strftime('%Y-%m-%d')}.log")
                file_handler = RotatingFileHandler(
                    log_file,
                    maxBytes=10485760,
                    backupCount=5,
                    encoding='utf-8'
                )
                file_handler.setLevel(logging.DEBUG)
            except:
                file_handler = None
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # 添加队列处理器，用于GUI界面获取日志
        queue_handler = QueueHandler()
        queue_handler.setLevel(logging.INFO)
        
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        console_formatter = ColoredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        if file_handler:
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        console_handler.setFormatter(console_formatter)
        queue_handler.setFormatter(file_formatter)

        if file_handler:
            logger.addHandler(console_handler)
            logger.addHandler(queue_handler)
        else:
            logger.addHandler(console_handler)
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    elif not logger.handlers:
        # 对于非根日志记录器，只设置级别，不添加处理器
        # 消息会传播到根日志记录器，由根日志记录器的处理器处理
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    return logger

def get_latest_logs(limit: int = 100) -> list:
    """获取最新的日志消息"""
    return list(log_queue)[-limit:]

def setup_logging():
    """设置日志系统，重定向标准输出和标准错误"""
    # 先保存原始的标准输出和错误
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    
    # 获取根日志记录器（此时还未重定向stdout/stderr）
    root_logger = get_logger("root")
    
    # 移除StreamHandler，避免循环日志
    for handler in list(root_logger.handlers):
        if isinstance(handler, logging.StreamHandler):
            root_logger.removeHandler(handler)
    
    # 重定向标准输出和标准错误
    sys.stdout = StdoutRedirector(root_logger, logging.INFO)
    sys.stderr = StderrRedirector(root_logger, logging.ERROR)
    
    return root_logger
