"""Cookie过期检测模块"""

import re
import logging
from typing import Tuple, Optional

logger = logging.getLogger(__name__)


class CookieChecker:
    """
    Cookie过期检测器

    检测逻辑：
    1. 检查sec_user_id是否能正常提取
    2. 检查API返回是否包含认证错误关键字
    3. 提供明确的错误提示
    """

    AUTH_ERROR_KEYWORDS = [
        '未登录',
        '登录失效',
        'cookie失效',
        'cookie过期',
        'auth failed',
        'unauthorized',
        '请登录',
        'token无效',
        '登录超时',
        '验证失败',
    ]

    SEC_USER_ID_PATTERN = re.compile(r"https?://(?:www\.)?douyin\.com/user/([A-Za-z0-9_.=-]+)")

    def __init__(self):
        self.last_error: Optional[str] = None
        self.error_count = 0

    def check_url_extraction(self, url: str) -> Tuple[bool, Optional[str]]:
        """
        检查URL是否能正常提取sec_user_id

        Returns:
            (success, error_msg)
        """
        if not url:
            return False, "URL为空"

        if 'sec_user_id' in url.lower():
            return True, None

        match = self.SEC_USER_ID_PATTERN.search(url)
        if match:
            return True, None

        return False, f"无法从URL提取sec_user_id: {url}"

    def check_api_response(self, response_text: str) -> Tuple[bool, Optional[str]]:
        """
        检查API响应是否包含认证错误

        Returns:
            (is_auth_error, error_msg)
        """
        if not response_text:
            return False, None

        response_lower = response_text.lower()

        for keyword in self.AUTH_ERROR_KEYWORDS:
            if keyword.lower() in response_lower:
                error_msg = f"检测到Cookie过期: {keyword}"
                self.last_error = error_msg
                self.error_count += 1
                logger.error(error_msg)
                return True, error_msg

        return False, None

    def check_login_status(self, html_content: str) -> Tuple[bool, Optional[str]]:
        """
        检查页面内容是否显示未登录

        Returns:
            (is_not_logged_in, error_msg)
        """
        if not html_content:
            return False, None

        not_logged_in_patterns = [
            r'登录',
            r'请先登录',
            r'没有登录',
            r'login',
            r'sign in',
        ]

        for pattern in not_logged_in_patterns:
            if re.search(pattern, html_content, re.IGNORECASE):
                if self._is_login_page(html_content):
                    error_msg = "检测到未登录状态"
                    self.last_error = error_msg
                    logger.error(error_msg)
                    return True, error_msg

        return False, None

    def _is_login_page(self, content: str) -> bool:
        """判断是否为登录页面"""
        login_indicators = ['手机号', '验证码', '密码登录', 'username', 'password']
        matches = sum(1 for indicator in login_indicators if indicator in content)
        return matches >= 2

    def get_error_summary(self) -> dict:
        """获取错误摘要"""
        return {
            'last_error': self.last_error,
            'error_count': self.error_count,
            'is_likely_cookie_expired': self.error_count >= 3,
        }

    def reset(self):
        """重置错误计数"""
        self.last_error = None
        self.error_count = 0
        logger.info("Cookie检查器已重置")

    def suggest_action(self) -> str:
        """根据错误情况建议操作"""
        if self.error_count == 0:
            return "Cookie状态正常"

        if self.error_count >= 5:
            return (
                "⚠️ Cookie可能已过期，请更新Cookie\n"
                "操作步骤：\n"
                "1. 在浏览器登录抖音\n"
                "2. 打开开发者工具 → Application → Cookies\n"
                "3. 复制新的Cookie值\n"
                "4. 更新Space环境变量"
            )

        if self.last_error:
            return f"⚠️ {self.last_error}，已连续失败{self.error_count}次"

        return f"⚠️ 出现{self.error_count}次错误，请检查日志"
