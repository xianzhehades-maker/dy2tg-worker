"""自定义配置模块 - 加密模块依赖"""

from .config import USERAGENT

PARAMS_HEADERS = {
    "User-Agent": USERAGENT,
    "Referer": "https://www.douyin.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
}

PARAMS_HEADERS_TIKTOK = {
    "User-Agent": USERAGENT,
    "Referer": "https://www.tiktok.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
}
