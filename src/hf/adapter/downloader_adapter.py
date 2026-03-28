"""下载适配器 - 使用轻量级抖音 API 客户端下载"""

import os
import sys
from datetime import datetime

from hf.database import DatabaseManager, Task
from hf.utils.logger import get_logger
from hf.adapter.douyin_api_client import get_douyin_client
from hf.config import get_config, DOWNLOAD_PATH

logger = get_logger("downloader_adapter")


class DownloaderAdapter:
    """下载适配器"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.config = get_config()
        # 创建必要的目录
        os.makedirs(DOWNLOAD_PATH, exist_ok=True)
    
    async def download_all_tasks(self):
        """
        下载所有待下载的任务

        Returns:
            下载成功的任务数量
        """
        print(f"\n{'='*60}", flush=True)
        print(f"📥 开始下载视频（使用抖音 API 直连）", flush=True)
        print(f"{'='*60}", flush=True)

        tasks = self.db.get_tasks(status="pending")

        print(f"📊 待下载任务数: {len(tasks)}", flush=True)

        if tasks:
            print(f"🔍 [DEBUG] 第一个任务的原始数据:", flush=True)
            first_task = tasks[0]
            print(f"   - id: {first_task.id!r} (type: {type(first_task.id)})", flush=True)
            print(f"   - video_id: {first_task.video_id!r}", flush=True)
            print(f"   - video_url: {first_task.video_url!r}", flush=True)
            print(f"   - customer_id: {first_task.customer_id!r}", flush=True)
            print(f"   - status: {first_task.status!r}", flush=True)
            print(f"   - 所有属性: {[attr for attr in dir(first_task) if not attr.startswith('_')]}", flush=True)

        if not tasks:
            print(f"✅ 没有待下载的任务", flush=True)
            return 0

        download_count = 0

        for idx, task in enumerate(tasks):
            print(f"\n🚀 开始下载任务 [{idx+1}/{len(tasks)}] task.id={task.id!r} task.video_id={task.video_id!r}", flush=True)
            print(f"🔍 [DEBUG] 任务完整信息: id={task.id}, video_id={task.video_id}, url={task.video_url}", flush=True)
            
            try:
                success = await self.download_single_task(task.id)
                if success:
                    download_count += 1
            except Exception as e:
                print(f"❌ 下载任务 {task.id} 失败: {e}", flush=True)
                import traceback
                traceback.print_exc()
        
        print(f"\n{'='*60}", flush=True)
        print(f"🎉 下载完成！成功: {download_count} 个", flush=True)
        print(f"{'='*60}", flush=True)
        
        return download_count
    
    async def download_single_task(self, task_id):
        """
        下载单个任务

        Args:
            task_id: 任务ID

        Returns:
            是否下载成功
        """
        print(f"🔍 [DEBUG] download_single_task 被调用: task_id={task_id!r} (type={type(task_id)})", flush=True)

        task = self.db.get_task(task_id)
        print(f"🔍 [DEBUG] get_task({task_id}) 返回: {task}", flush=True)

        if not task:
            print(f"❌ 任务 {task_id} 不存在", flush=True)
            return False

        print(f"🔍 [DEBUG] 获取到的任务详情: id={task.id!r}, video_id={task.video_id!r}, status={task.status!r}", flush=True)

        if task.id is None:
            print(f"❌ [严重错误] 任务的 id 字段为 None! 任务数据可能已损坏", flush=True)
            print(f"   可能原因: HF持久化恢复数据时 id 列丢失或变为 NULL", flush=True)
            return False

        if task.status == "downloaded":
            print(f"⚠️ 任务 {task_id} 已下载过，跳过", flush=True)
            return True

        save_path = os.path.abspath(os.path.join(DOWNLOAD_PATH, f"{task_id}.mp4"))

        print(f"📥 下载路径: {save_path}", flush=True)

        try:
            cookie = self.config.get("douyin_cookies", "")
            client = get_douyin_client(cookie)

            video_id = task.video_id
            if not video_id:
                import re
                video_id_match = re.search(r'/video/(\d{18,20})', task.video_url)
                if not video_id_match:
                    print(f"❌ 无法从 URL 提取 video_id", flush=True)
                    self.db.update_task(task_id, status="error", error_msg="无法提取 video_id")
                    return False
                video_id = video_id_match.group(1)

            print(f"🔍 使用 video_id 实时获取下载链接: {video_id}", flush=True)

            video_info = await client.fetch_video_detail(video_id)
            if not video_info or not video_info.get("download_url"):
                print(f"❌ 无法获取视频下载链接", flush=True)
                self.db.update_task(task_id, status="error", error_msg="无法获取下载链接")
                return False

            download_url = video_info["download_url"]
            print(f"✅ 获取到实时下载链接", flush=True)

            success = await client.download_video(download_url, save_path)

            if success:
                current_time = datetime.now()
                self.db.update_task(
                    task_id,
                    status="downloaded",
                    download_time=current_time,
                    file_path=save_path,
                    download_url=download_url
                )
                print(f"✅ 任务 [{task_id}] 下载完成: {save_path}", flush=True)
                return True
            else:
                self.db.update_task(task_id, status="error", error_msg="下载失败")
                return False

        except Exception as e:
            print(f"❌ 下载异常: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return False

    async def download_video_by_info(self, video_info: dict, group_id: int = None) -> tuple:
        """
        根据视频信息直接下载（流水线模式）

        Args:
            video_info: dict 包含 video_id, video_url, video_publish_time, video_desc
            group_id: 分组ID

        Returns:
            (success: bool, download_path: str or None, task_id: int or None)
        """
        video_id = video_info.get("video_id")
        video_url = video_info.get("video_url")
        video_publish_time = video_info.get("video_publish_time")
        video_desc = video_info.get("video_desc", "")
        customer_id = video_info.get("customer_id")

        if not video_id:
            print(f"❌ video_info 缺少 video_id", flush=True)
            return False, None, None

        try:
            cookie = self.config.get("douyin_cookies", "")
            client = get_douyin_client(cookie)

            print(f"🔍 使用 video_id 实时获取下载链接: {video_id}", flush=True)

            video_detail = await client.fetch_video_detail(str(video_id))
            if not video_detail or not video_detail.get("download_url"):
                print(f"❌ 无法获取视频下载链接", flush=True)
                return False, None, None

            download_url = video_detail["download_url"]
            print(f"✅ 获取到实时下载链接", flush=True)

            task_id = abs(hash(video_id)) % 1000000
            save_path = os.path.abspath(os.path.join(DOWNLOAD_PATH, f"{task_id}.mp4"))

            success = await client.download_video(download_url, save_path)

            if success:
                print(f"✅ 视频 {video_id} 下载完成: {save_path}", flush=True)

                # 先检查是否已存在该视频的任务
                existing_task = self.db.get_video_task_by_id(str(video_id))
                if existing_task:
                    print(f"ℹ️  任务已存在，使用现有 task_id={existing_task.id}", flush=True)
                    # 更新现有任务的文件路径和状态
                    self.db.update_task(existing_task.id, status="downloaded", file_path=save_path, download_url=download_url)
                    return True, save_path, existing_task.id

                # 创建新任务
                task = Task(
                    group_id=group_id,
                    customer_id=customer_id,
                    video_id=str(video_id),
                    video_url=video_url,
                    video_publish_time=video_publish_time,
                    download_url=download_url,
                    video_desc=video_desc,
                    status="downloaded",
                    file_path=save_path
                )
                db_task_id = self.db.add_task(task)

                if db_task_id:
                    print(f"✅ 创建数据库任务: task_id={db_task_id}", flush=True)
                    return True, save_path, db_task_id
                else:
                    print(f"⚠️ 创建数据库任务失败，再次尝试获取现有任务", flush=True)
                    existing_task = self.db.get_video_task_by_id(str(video_id))
                    if existing_task:
                        print(f"ℹ️  找到现有任务: task_id={existing_task.id}", flush=True)
                        return True, save_path, existing_task.id
                    print(f"❌ 无法获取任务ID", flush=True)
                    return False, None, None
            else:
                print(f"❌ 视频 {video_id} 下载失败", flush=True)
                if os.path.exists(save_path):
                    os.remove(save_path)
                return False, None, None

        except Exception as e:
            print(f"❌ 下载异常: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return False, None, None


def main():
    """主函数 - 测试下载适配器"""
    import asyncio
    adapter = DownloaderAdapter()
    asyncio.run(adapter.download_all_tasks())


if __name__ == "__main__":
    main()
