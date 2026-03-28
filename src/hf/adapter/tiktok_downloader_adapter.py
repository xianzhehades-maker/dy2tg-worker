import sys
import os
import asyncio
from pathlib import Path

# 确保能导入 src/hf/ 下的模块
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# 导入 TikTokDownloader 的相关模块
try:
    from ..testers.params import Params
    from ..interface.account_tiktok import AccountTikTok
    from ..interface.detail_tiktok import DetailTikTok
    from ..interface import API
    from ..custom import USERAGENT
    TIKTOK_DOWNLOADER_AVAILABLE = True
except Exception as e:
    print(f"TikTokDownloader 导入失败: {e}")
    import traceback
    traceback.print_exc()
    TIKTOK_DOWNLOADER_AVAILABLE = False


class TikTokDownloaderAdapter:
    def __init__(self, cookie: str = "", proxy: str = ""):
        self.cookie = cookie.strip()
        self.proxy = proxy.strip() if proxy.strip() else None
        self.params = None
        
    async def __aenter__(self):
        await self._init_params()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.params:
            await self.params.client.aclose()
        
    async def _init_params(self):
        if self.params:
            return self.params
            
        # 使用 Params 类初始化（这个类是项目里现成的测试类，专门为简单使用设计的）
        self.params = Params()
        
        # 手动设置 cookie
        if self.cookie:
            self.params.cookie_str = self.cookie
            self.params.headers["Cookie"] = self.cookie
            # 同时设置 API 类的 cookie 相关参数
            API.params["msToken"] = ""
        
        # 设置代理
        if self.proxy:
            self.params.proxy = self.proxy
            # 重新创建带代理的客户端
            from src.hf.tools import create_client
            self.params.client = create_client(
                timeout=self.params.timeout,
                proxy=self.params.proxy
            )
        
        # 初始化 API 的进度对象为假进度（防止控制台输出进度条）
        API.init_progress_object(server_mode=True)
        
        return self.params
        
    async def get_user_videos(self, sec_user_id: str, count: int = 20, cursor: int = 0) -> list:
        """获取用户发布的视频列表"""
        if not TIKTOK_DOWNLOADER_AVAILABLE:
            print("❌ TikTokDownloader 模块不可用")
            return []
            
        await self._init_params()
        
        account = AccountTikTok(
            self.params,
            cookie=self.cookie,
            proxy=self.proxy,
            sec_user_id=sec_user_id,
            tab="post",
            cursor=cursor,
            count=count
        )
        
        # 只获取一页
        videos = await account.run(single_page=True)
        return videos
        
    async def get_video_detail(self, video_id: str) -> dict:
        """获取单个视频的详情（包含下载链接）"""
        if not TIKTOK_DOWNLOADER_AVAILABLE:
            print("❌ TikTokDownloader 模块不可用")
            return {}
            
        await self._init_params()
        
        detail = DetailTikTok(
            self.params,
            cookie=self.cookie,
            proxy=self.proxy,
            detail_id=video_id
        )
        
        result = await detail.run()
        return result
        
    def extract_download_url(self, video: dict) -> str:
        """从视频对象（来自列表或详情）中提取下载链接"""
        try:
            if "video" in video:
                video_data = video["video"]
                # 优先尝试 playAddr
                if "playAddr" in video_data:
                    play_addrs = video_data["playAddr"]
                    if play_addrs:
                        if isinstance(play_addrs, list) and len(play_addrs) > 0:
                            return play_addrs[0]
                        if isinstance(play_addrs, str):
                            return play_addrs
                # 再尝试 downloadAddr
                if "downloadAddr" in video_data:
                    download_addrs = video_data["downloadAddr"]
                    if download_addrs:
                        if isinstance(download_addrs, list) and len(download_addrs) > 0:
                            return download_addrs[0]
                        if isinstance(download_addrs, str):
                            return download_addrs
        except Exception:
            pass
        return ""


# 简单测试
async def main():
    adapter = TikTokDownloaderAdapter()
    print("TikTokDownloader 适配器初始化完成")


if __name__ == "__main__":
    asyncio.run(main())
