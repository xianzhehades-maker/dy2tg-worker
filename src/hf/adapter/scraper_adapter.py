"""抓取适配器 - 使用轻量级抖音 API 客户端"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from hf.database import DatabaseManager, Task, DiscoveredVideo
from hf.utils.logger import get_logger
from hf.utils.retry import async_retry
from hf.adapter.douyin_api_client import get_douyin_client
from hf.config import get_config

logger = get_logger("scraper_adapter")

# 默认只下载最近3天内的视频
DEFAULT_MAX_DAYS_OLD = 3


def is_video_too_old(video_publish_time: Optional[datetime], max_days: int = DEFAULT_MAX_DAYS_OLD) -> bool:
    """
    判断视频是否过旧

    Args:
        video_publish_time: 视频发布时间
        max_days: 最大天数

    Returns:
        True表示过旧，False表示不过旧
    """
    if not video_publish_time:
        return False

    cutoff_date = datetime.now() - timedelta(days=max_days)
    return video_publish_time < cutoff_date


class ScraperAdapter:
    """抓取适配器"""

    def __init__(self, max_days_old: int = DEFAULT_MAX_DAYS_OLD):
        self.db = DatabaseManager()
        self.max_days_old = max_days_old
        self.config = get_config()
        self.current_group_id = None

    def _get_douyin_client(self):
        """获取抖音 API 客户端"""
        cookie = self.config.get("douyin_cookies", "")
        return get_douyin_client(cookie)

    async def scrape_user_videos(self, customer_id=None):
        """
        抓取用户视频

        Args:
            customer_id: 客户ID，如果为None则抓取所有客户

        Returns:
            新增的视频数量
        """
        print(f"\n{'='*60}", flush=True)
        print(f"🚀 使用轻量级抖音 API 开始抓取", flush=True)
        print(f"{'='*60}", flush=True)

        total_new_count = 0

        if customer_id:
            customers = [self.db.get_customer(customer_id)]
        else:
            customers = self.db.get_customers()

        if not customers or not any(customers):
            print("⚠️ 没有配置客户，跳过抓取", flush=True)
            return 0

        for customer in customers:
            if not customer:
                continue

            print(f"\n🎯 开始抓取: {customer.name}", flush=True)
            print(f"   主页URL: {customer.homepage_url}", flush=True)

            try:
                new_count = await self._scrape_customer_videos(customer)
                total_new_count += new_count
                print(f"✅ {customer.name} 抓取完成，新增 {new_count} 个视频任务", flush=True)
            except Exception as e:
                print(f"❌ {customer.name} 抓取失败: {e}", flush=True)
                import traceback
                traceback.print_exc()

        print(f"\n{'='*60}", flush=True)
        print(f"🎉 抓取完成！总共新增 {total_new_count} 个任务", flush=True)
        print(f"{'='*60}", flush=True)

        return total_new_count

    async def scrape_group_videos(self, group_id: int):
        """
        抓取分组视频

        Args:
            group_id: 分组ID

        Returns:
            新增的视频数量
        """
        print(f"\n{'='*60}", flush=True)
        print(f"🚀 使用轻量级抖音 API 开始抓取分组 (ID: {group_id})", flush=True)
        print(f"{'='*60}", flush=True)

        total_new_count = 0

        group = self.db.get_monitor_group(group_id)
        if not group:
            print("⚠️ 分组不存在", flush=True)
            return 0

        self.current_group_id = group_id

        monitors = self.db.get_group_monitors(group_id)
        if not monitors:
            print("⚠️ 分组没有配置监控UP主", flush=True)
            return 0

        for monitor in monitors:
            print(f"\n🎯 开始抓取: {monitor.up_name}", flush=True)
            print(f"   主页URL: {monitor.up_url}", flush=True)

            try:
                customer = self._get_or_create_customer_for_monitor(monitor)
                if customer:
                    new_count = await self._scrape_customer_videos(customer)
                    total_new_count += new_count
                    print(f"✅ {monitor.up_name} 抓取完成，新增 {new_count} 个视频任务", flush=True)
            except Exception as e:
                print(f"❌ {monitor.up_name} 抓取失败: {e}", flush=True)
                import traceback
                traceback.print_exc()

        print(f"\n{'='*60}", flush=True)
        print(f"🎉 分组抓取完成！总共新增 {total_new_count} 个任务", flush=True)
        print(f"{'='*60}", flush=True)

        self.current_group_id = None
        return total_new_count

    def _get_or_create_customer_for_monitor(self, monitor):
        """获取或创建监控UP主对应的Customer"""
        customers = self.db.get_customers()
        for c in customers:
            if c.homepage_url == monitor.up_url:
                return c
        
        from hf.database.models import Customer
        customer = Customer(
            name=monitor.up_name,
            user_id=f"group_{monitor.group_id}_{monitor.id}",
            homepage_url=monitor.up_url
        )
        customer_id = self.db.add_customer(customer)
        if customer_id:
            customer.id = customer_id
            return customer
        return None

    async def _scrape_customer_videos(self, customer):
        """
        抓取单个客户的视频

        使用轻量级抖音 API 获取视频列表，
        通过 discovered_videos 表判断重复，
        符合条件的视频进入 tasks 表
        """
        new_task_count = 0

        try:
            client = self._get_douyin_client()

            print("🔄 正在提取用户 sec_user_id...", flush=True)
            sec_user_id = await client.extract_sec_user_id(customer.homepage_url)

            if not sec_user_id:
                print(f"❌ 无法提取 sec_user_id，请检查 URL 或 Cookie", flush=True)
                return 0

            print(f"✅ 获取到 sec_user_id: {sec_user_id}", flush=True)
            print(f"🔄 正在获取视频列表（最近 {self.max_days_old} 天）...", flush=True)

            # 获取视频列表
            videos = await client.fetch_user_videos(
                sec_user_id=sec_user_id,
                max_days_old=self.max_days_old + 7,  # 多获取几天，用于缓冲
                max_pages=5
            )

            if not videos:
                print("⚠️ 未获取到任何视频", flush=True)
                return 0

            print(f"✅ 共获取到 {len(videos)} 个视频，开始处理...", flush=True)

            # 处理每个视频
            for video in videos:
                result = await self._process_single_video(customer, video)
                if result:
                    new_task_count += 1

            print(f"🏁 {customer.name} 处理完成，新增 {new_task_count} 个任务", flush=True)

        except Exception as e:
            print(f"❌ 抓取异常: {e}", flush=True)
            import traceback
            traceback.print_exc()

        return new_task_count

    async def _process_single_video(self, customer, video: Dict[str, Any]) -> bool:
        """
        处理单个视频

        Returns:
            True表示创建了新任务，False表示跳过
        """
        video_id = video["video_id"]
        video_url = video["video_url"]
        video_publish_time = video["publish_time"]

        print(f"\n📺 处理视频: {video_id}", flush=True)
        print(f"   URL: {video_url}", flush=True)
        if video_publish_time:
            print(f"   发布时间: {video_publish_time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        # 1. 检查是否已在 discovered_videos 表中
        if self.db.discovered_video_exists(video_id):
            print(f"😴 已发现过，跳过", flush=True)
            return False

        # 2. 判断视频是否符合条件
        is_qualified = True
        skip_reason = ""

        # 检查是否过旧
        if is_video_too_old(video_publish_time, self.max_days_old):
            is_qualified = False
            skip_reason = f"视频过旧（超过{self.max_days_old}天）"

        # 检查是否已在 tasks 表中（双重保险）
        if self.db.video_id_exists(video_id) or self.db.video_exists(video_url):
            is_qualified = False
            skip_reason = "已在任务队列中"

        # 3. 记录到 discovered_videos 表（修复：移到任务创建成功后，避免失败后视频被永久跳过）
        if not is_qualified:
            print(f"⏭️  跳过: {skip_reason}", flush=True)
            return False

        # 4. 创建任务
        task = Task(
            group_id=self.current_group_id,
            customer_id=customer.id,
            video_id=video_id,
            video_url=video_url,
            video_publish_time=video_publish_time,
            download_url=video.get("download_url"),
            video_desc=video.get("desc", ""),
            status="pending"
        )
        task_id = self.db.add_task(task)

        if task_id:
            # 5. 只有任务创建成功后，才标记为已发现（修复：如果任务创建前就标记，后续失败会导致视频被永久跳过）
            discovered_video = DiscoveredVideo(
                customer_id=customer.id,
                video_id=video_id,
                video_url=video_url,
                video_publish_time=video_publish_time,
                discovered_at=datetime.now(),
                is_qualified=True
            )
            self.db.add_discovered_video(discovered_video)

            time_str = video_publish_time.strftime("%Y-%m-%d") if video_publish_time else "未知"
            print(f"✨ [新任务] ID: {video_id} -> {video_url} (发布时间: {time_str})", flush=True)
            return True
        else:
            print(f"⚠️ 创建任务失败，可能已存在", "WARN")
            return False

    async def fetch_user_videos_for_customer(self, customer_id: int) -> list:
        """
        获取用户视频列表（流水线模式用）

        Returns:
            视频信息列表，每个元素包含 video_id, video_url, video_publish_time
        """
        customer = self.db.get_customer(customer_id)
        if not customer:
            print(f"❌ 客户 {customer_id} 不存在", flush=True)
            return []

        print(f"🔍 正在获取用户视频列表: {customer.name}", flush=True)

        try:
            client = get_douyin_client(self.config.get("douyin_cookies", ""))
            sec_user_id = await client.extract_sec_user_id(customer.homepage_url)

            if not sec_user_id:
                print(f"❌ 无法获取 sec_user_id", flush=True)
                return []

            videos = await client.fetch_user_videos(
                sec_user_id=sec_user_id,
                max_days_old=self.max_days_old,
                max_pages=3
            )

            if not videos:
                print(f"⚠️ 没有获取到视频", flush=True)
                return []

            print(f"📊 获取到 {len(videos)} 个视频，开始过滤...", flush=True)

            qualified_videos = []
            for video in videos:
                video_id = video.get("video_id")
                video_url = video.get("video_url")
                video_publish_time = video.get("publish_time")

                if not video_id or not video_url:
                    continue

                if self.db.discovered_video_exists(str(video_id)):
                    print(f"⏭️  已发现过，跳过: {video_id}", flush=True)
                    continue

                qualified_videos.append({
                    "video_id": video_id,
                    "video_url": video_url,
                    "video_publish_time": video_publish_time,
                    "download_url": video.get("download_url"),
                    "video_desc": video.get("desc", ""),
                    "customer_id": customer_id
                })

                discovered = DiscoveredVideo(
                    customer_id=customer_id,
                    video_id=video_id,
                    video_url=video_url,
                    video_publish_time=video_publish_time,
                    is_qualified=True
                )
                self.db.add_discovered_video(discovered)

            print(f"✅ 符合条件的视频: {len(qualified_videos)} 个", flush=True)
            return qualified_videos

        except Exception as e:
            print(f"❌ 获取视频列表失败: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return []


def main():
    """主函数 - 测试抓取适配器"""
    import argparse

    parser = argparse.ArgumentParser(description="轻量级抖音 API 抓取适配器")
    parser.add_argument("customer_id", type=int, nargs="?", help="客户ID")
    parser.add_argument("--max-days", type=int, default=DEFAULT_MAX_DAYS_OLD,
                        help=f"只下载最近N天内的视频（默认: {DEFAULT_MAX_DAYS_OLD}天）")
    args = parser.parse_args()

    adapter = ScraperAdapter(max_days_old=args.max_days)
    asyncio.run(adapter.scrape_user_videos(args.customer_id))


if __name__ == "__main__":
    main()
