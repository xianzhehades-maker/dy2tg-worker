"""统一配置管理"""
import json
import os

def load_json_config(filename):
    """加载JSON配置文件"""
    config_path = os.path.join(os.path.dirname(__file__), filename)
    if not os.path.exists(config_path):
        return {}
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_json_config(filename, data):
    """保存JSON配置文件"""
    config_path = os.path.join(os.path.dirname(__file__), filename)
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 使用 /tmp 目录来存储运行时数据，避免权限问题
_temp_base = os.getenv('HF_RUNTIME_DIR', '/tmp/hf_runtime')
os.makedirs(_temp_base, exist_ok=True)

DOWNLOAD_PATH = os.path.join(_temp_base, "downloads")
PROCESSED_PATH = os.path.join(_temp_base, "processed")
UPLOAD_PATH = os.path.join(_temp_base, "upload")

os.makedirs(DOWNLOAD_PATH, exist_ok=True)
os.makedirs(PROCESSED_PATH, exist_ok=True)
os.makedirs(UPLOAD_PATH, exist_ok=True)

def load_network_config():
    """加载网络配置，优先从环境变量读取"""
    network_config = load_json_config('network.json')
    
    proxy_url = os.environ.get('PROXY_URL')
    if proxy_url:
        network_config['proxy_url'] = proxy_url
    
    douyin_cookies = os.environ.get('DOUYIN_COOKIES')
    if douyin_cookies:
        if 'proxy_config' not in network_config:
            network_config['proxy_config'] = {}
        network_config['proxy_config']['douyin_cookies'] = douyin_cookies
    
    return network_config

_network = load_network_config()
PROXY_CONFIG = _network.get('proxy_config', {})
PROXY_URL = _network.get('proxy_url', 'system')
PROXY_URLS = _network.get('proxy_urls', {})
DEBUG_MODE = _network.get('debug_mode', False)

def get_proxy_url_for_program(program):
    """获取指定功能的代理URL"""
    if PROXY_URLS and program in PROXY_URLS:
        return PROXY_URLS[program]
    return PROXY_URL

def load_telegram_config():
    """加载 Telegram 配置，优先从环境变量读取"""
    session_string = os.environ.get('TELEGRAM_SESSION_STRING')
    chat_id = os.environ.get('TELEGRAM_CHANNEL_ID')
    api_id = os.environ.get('TELEGRAM_API_ID')
    api_hash = os.environ.get('TELEGRAM_API_HASH')
    
    if session_string:
        config = {
            "bots": {
                "bot1": {
                    "enabled": True,
                    "session_string": session_string
                }
            }
        }
        if chat_id:
            config["bots"]["bot1"]["chat_id"] = chat_id
        if api_id:
            config["bots"]["bot1"]["api_id"] = api_id
        if api_hash:
            config["bots"]["bot1"]["api_hash"] = api_hash
        return config
    
    return load_json_config('telegram.json')

_telegram = load_telegram_config()
TELEGRAM_BOTS = _telegram.get('bots', {})

_watermark = load_json_config('watermark.json')
WATERMARK_CONFIG = _watermark

_upload = load_json_config('upload.json')
UPLOAD_TIMEOUT = _upload.get('upload_timeout', 300)
MAX_RETRIES = _upload.get('max_retries', 3)
RETRY_DELAY = _upload.get('retry_delay', 5)
AUTO_DELETE_AFTER_UPLOAD = _upload.get('auto_delete_after_upload', False)

_targets = load_json_config('targets.json')
TARGET_USER_URLS = _targets.get('target_urls', {})
UP_NAME_MAP = _targets.get('up_name_map', {})

_monitor = load_json_config('monitor.json')
MONITOR_AUTO_WORKFLOW = _monitor.get('auto_workflow', True)
MONITOR_DEFAULT_WORKFLOW_INDEX = _monitor.get('default_workflow_index', 0)

_scheduler = load_json_config('scheduler.json')
SCHEDULER_CONFIG = _scheduler

_ai = load_json_config('ai_config.json')
AI_MODEL = _ai.get('caption_model', 'gemini-2.5-flash')
AI_API_KEY = _ai.get('caption_api_key', '')
AI_BASE_URL = _ai.get('caption_base_url', 'http://localhost:3000/gemini-cli-oauth/v1')
AI_MAX_RETRIES = _ai.get('max_retries', 3)
AI_RETRY_DELAY = _ai.get('retry_delay', 2)
WHISPER_MODEL = _ai.get('whisper_model', 'small')
WHISPER_DEVICE = _ai.get('whisper_device', 'cpu')
WHISPER_COMPUTE_TYPE = _ai.get('whisper_compute_type', 'int8')

def get_ai_config():
    """获取 AI 配置字典"""
    return {
        'caption_model': AI_MODEL,
        'caption_api_key': AI_API_KEY,
        'caption_base_url': AI_BASE_URL,
        'max_retries': AI_MAX_RETRIES,
        'retry_delay': AI_RETRY_DELAY,
        'whisper_model': WHISPER_MODEL,
        'whisper_device': WHISPER_DEVICE,
        'whisper_compute_type': WHISPER_COMPUTE_TYPE
    }

def get_config():
    """获取完整配置字典（兼容适配器）"""
    config_dict = {}
    # 添加网络配置
    network = load_network_config()
    config_dict.update(network)
    # 从 proxy_config 中提取 douyin_cookies 到顶层
    proxy_config = network.get('proxy_config', {})
    if 'douyin_cookies' in proxy_config:
        config_dict['douyin_cookies'] = proxy_config['douyin_cookies']
    return config_dict
