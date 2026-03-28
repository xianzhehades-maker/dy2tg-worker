"""分组工作流执行器 - 流水线模式：每个视频完成全流程"""
import os
import sys
import time
import asyncio
import random
import traceback
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import DatabaseManager
from adapter import ScraperAdapter, DownloaderAdapter
import config

def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")

def log_separator(title=None):
    if title:
        print(f"\n{'='*55}", flush=True)
        print(f"  🚀 {title}", flush=True)
        print(f"{'='*55}", flush=True)
    else:
        print(f"\n{'='*55}", flush=True)

def log_stage(stage_num, total_stages, stage_name):
    print(f"\n{'='*50}", flush=True)
    print(f"  [阶段 {stage_num}/{total_stages}] {stage_name}")
    print(f"{'='*50}", flush=True)

def log_summary(title, stats):
    print(f"\n📊 {title}", flush=True)
    for key, value in stats.items():
        print(f"   {key}: {value}", flush=True)


class PipelineExecutor:
    """流水线模式执行器 - 每个视频完成全流程"""

    def __init__(self):
        self.db = DatabaseManager()
        self.scraper = ScraperAdapter(max_days_old=3)
        self.downloader = DownloaderAdapter()
        self.running = True
        self.cycle_count = 0
        self.processed_count = 0

    async def process_single_video(self, video_info: dict, customer_id: int, group_id: int = None):
        """处理单个视频的完整流水线"""
        video_id = video_info.get("video_id")
        video_desc = video_info.get("video_desc", "")
        task_id = None
        ai_caption_style = None

        if group_id:
            group = self.db.get_monitor_group(group_id)
            if group:
                ai_caption_style = group.ai_caption_style

        try:
            log(f"📥 处理视频: {video_id}")
            log(f"   描述: {video_desc[:40]}..." if video_desc else "   描述: 无")

            # 步骤1: 下载
            log(f"  ⬇️  下载中...")
            success, download_path, task_id = await self.downloader.download_video_by_info(video_info, group_id)
            if not success or not download_path:
                log(f"  ❌ 下载失败", "ERROR")
                return False

            log(f"  ✅ 下载完成: {download_path}")

            # 保存视频描述到任务
            if video_desc:
                self.db.update_task(task_id, video_desc=video_desc)

            # 步骤2: 水印
            log(f"  🔖 添加水印中...")
            watermark_success = await self._add_watermark(download_path, task_id)
            if not watermark_success:
                log(f"  ⚠️ 水印添加失败，继续处理", "WARN")

            # 步骤3: 根据分组类型处理文案
            caption_result = None
            if ai_caption_style == "default" or ai_caption_style == "口播":
                log(f"  ✍️  [口播组] AI文案处理中...")
                caption_result = await self._process_ai_caption(task_id)
                if caption_result:
                    log(f"  ✅ AI文案生成成功")
                else:
                    log(f"  ⚠️ AI文案生成失败，使用原始描述", "WARN")
                    caption_result = video_desc
            elif ai_caption_style == "humor" or ai_caption_style == "short":
                log(f"  ✍️  [乐子组] 生成简短吸引人文案...")
                caption_result = await self._process_short_caption(task_id, video_desc)
                if caption_result:
                    log(f"  ✅ 简短文案生成成功: {caption_result}")
                else:
                    log(f"  ⚠️ 简短文案生成失败，使用原始描述", "WARN")
                    caption_result = video_desc
            else:
                log(f"  ⏭️  [舞蹈组] 跳过AI文案")

            # 步骤4: 上传
            log(f"  📤 上传到Telegram中...")
            upload_caption = self._build_upload_caption(video_desc, caption_result, customer_id)
            upload_success, error_msg = await self._upload_to_telegram(task_id, upload_caption, group_id)
            if not upload_success:
                log(f"  ❌ 上传失败: {error_msg}", "ERROR")
                self.db.update_task(task_id, status="upload_failed", error_msg=error_msg or "上传到Telegram失败")
                return False

            log(f"  ✅ 上传完成!")

            # 更新任务状态为已上传
            self.db.update_task(task_id, status="uploaded")

            # 步骤5: 清理
            self._cleanup_files(task_id, download_path)

            log(f"🎉 视频 {video_id} 处理完成!")

            # 每个任务完成后等待5秒再处理下一个
            await asyncio.sleep(5)
            return True

        except Exception as e:
            log(f"  ❌ 处理异常: {e}", "ERROR")
            import traceback
            traceback.print_exc()
            if task_id:
                self._cleanup_files(task_id, None)
            return False

    async def _process_pending_task(self, task, group):
        """处理之前失败的任务，继续流水线"""
        try:
            log(f"🔄 处理待处理任务 [{task.id}] status={task.status}")

            task_id = task.id
            video_desc = task.video_desc or ""

            if task.status == "downloaded":
                log(f"  🔖 添加水印中...")
                watermark_success = await self._add_watermark(task.file_path, task_id)
                if not watermark_success:
                    log(f"  ⚠️ 水印添加失败，跳过", "WARN")
                    return

            if task.status in ["downloaded", "watermarked"]:
                ai_caption_style = group.ai_caption_style if group else None
                caption_result = None

                if ai_caption_style == "default" or ai_caption_style == "口播":
                    log(f"  ✍️  [口播组] AI文案处理中...")
                    caption_result = await self._process_ai_caption(task_id)
                elif ai_caption_style == "humor" or ai_caption_style == "short":
                    log(f"  ✍️  [乐子组] 生成简短吸引人文案...")
                    caption_result = await self._process_short_caption(task_id, video_desc)

                if caption_result:
                    self.db.update_task(task_id, ai_caption=caption_result)

            log(f"  📤 上传到Telegram中...")
            upload_caption = self._build_upload_caption(video_desc, task.ai_caption, task.customer_id)
            upload_success, error_msg = await self._upload_to_telegram(task_id, upload_caption, group.id if group else None)
            if not upload_success:
                log(f"  ❌ 上传失败: {error_msg}，保持状态", "ERROR")
                return

            log(f"  ✅ 上传完成!")
            self.db.update_task(task_id, status="uploaded")
            self._cleanup_files(task_id, task.file_path)
            log(f"🎉 任务 {task_id} 处理完成!")

        except Exception as e:
            log(f"  ❌ 处理异常: {e}", "ERROR")
            import traceback
            traceback.print_exc()

    async def _add_watermark(self, video_path: str, task_id: int) -> bool:
        """添加水印"""
        try:
            from core.watermark import WatermarkProcessor
            processor = WatermarkProcessor()
            output_file = processor.add_watermark(video_path, task_id=task_id, need_watermark=True)
            return output_file is not None
        except Exception as e:
            log(f"  水印处理异常: {e}", "WARN")
            return False

    async def _process_ai_caption(self, task_id: int) -> str:
        """AI文案处理"""
        try:
            from core.ai_caption import process_video_to_caption
            task = self.db.get_task(task_id)
            if not task:
                return None

            video_file = None
            for path in [
                os.path.join(config.UPLOAD_PATH, f"{task_id}_watermarked.mp4"),
                os.path.join(config.PROCESSED_PATH, f"{task_id}_watermarked.mp4"),
                os.path.join(config.PROCESSED_PATH, f"{task_id}.mp4"),
                os.path.join(config.DOWNLOAD_PATH, f"{task_id}.mp4"),
            ]:
                if os.path.exists(path):
                    video_file = path
                    break

            if not video_file:
                log(f"  ⚠️ 视频文件不存在，跳过AI文案", "WARN")
                return None

            try:
                caption = process_video_to_caption(video_file)
                if caption:
                    self.db.update_task(task_id, ai_caption=caption)
                    return caption
            except Exception as caption_err:
                log(f"  AI生成文案失败: {caption_err}", "WARN")

            return None
        except Exception as e:
            log(f"  AI文案处理异常: {e}", "WARN")
            return None

    async def _process_short_caption(self, task_id: int, video_desc: str) -> str:
        """生成简短吸引人的文案（乐子组用）"""
        if not video_desc:
            return None

        try:
            from core.ai_caption import generate_short_caption
            short_caption = generate_short_caption(video_desc)
            if short_caption:
                self.db.update_task(task_id, ai_caption=short_caption)
            return short_caption
        except Exception as e:
            log(f"  生成简短文案异常: {e}", "WARN")
            return None

    def _build_upload_caption(self, video_desc: str, ai_caption: str, customer_id: int) -> str:
        """构建上传时的完整文案格式：原始描述 + @UP主 + AI文案"""
        parts = []

        if video_desc:
            parts.append(video_desc)

        customer = self.db.get_customer(customer_id)
        if customer and customer.name:
            parts.append(f"@{customer.name}")

        if ai_caption:
            parts.append(ai_caption)

        caption = " | ".join(parts)
        if len(caption) > 1024:
            caption = caption[:1021] + "..."

        return caption

    async def _upload_to_telegram(self, task_id: int, ai_caption: str, group_id: int = None) -> tuple:
        """上传到Telegram
        Returns:
            (success: bool, error_msg: str or None)
        """
        try:
            from core.uploader import upload_single_task
            success, error_info = await upload_single_task(task_id, ai_caption, group_id)
            if not success:
                return (False, error_info)
            return (True, None)
        except Exception as e:
            error_msg = str(e)
            log(f"  上传Telegram异常: {error_msg}", "WARN")
            return (False, error_msg)

    def _cleanup_files(self, task_id: int, download_path: str):
        """清理本地文件"""
        try:
            paths_to_delete = []

            if download_path and os.path.exists(download_path):
                paths_to_delete.append(download_path)

            for suffix in ["_watermarked", "_stream", ""]:
                for dir_path in [config.UPLOAD_PATH, config.PROCESSED_PATH, config.DOWNLOAD_PATH]:
                    file_path = os.path.join(dir_path, f"{task_id}{suffix}.mp4")
                    if os.path.exists(file_path):
                        paths_to_delete.append(file_path)

            for path in paths_to_delete:
                try:
                    os.remove(path)
                    log(f"  🗑️  删除文件: {os.path.basename(path)}")
                except Exception as e:
                    log(f"  删除文件失败 {path}: {e}", "WARN")

        except Exception as e:
            log(f"  清理文件异常: {e}", "WARN")

    async def process_customer(self, customer_id: int, group_id: int = None):
        """处理单个客户的视频 - 流水线模式"""
        try:
            log(f"开始处理客户 {customer_id} 的视频...")

            videos = await self.scraper.fetch_user_videos_for_customer(customer_id)

            if not videos:
                log(f"没有新视频")
                return 0

            log(f"获取到 {len(videos)} 个视频，开始流水线处理...")

            processed = 0
            for video_info in videos:
                if not self.running:
                    break

                video_info["customer_id"] = customer_id

                success = await self.process_single_video(video_info, customer_id, group_id)
                if success:
                    processed += 1
                    self.processed_count += 1

                await asyncio.sleep(2)

            return processed

        except Exception as e:
            log(f"处理客户 {customer_id} 失败: {e}", "ERROR")
            import traceback
            traceback.print_exc()
            return 0

    async def run_pipeline(self):
        """执行一轮流水线"""
        self.cycle_count += 1
        self.cycle_start_time = time.time()

        log_separator(f"第 {self.cycle_count} 轮流水线开始")

        stage_stats = {"抓取": {"UP主": 0, "视频": 0, "新增": 0},
                      "下载": {"成功": 0, "失败": 0},
                      "上传": {"成功": 0, "失败": 0}}

        groups = self.db.get_monitor_groups()
        if not groups:
            log("没有配置分组，退出", "ERROR")
            return

        # 阶段1: 处理待处理任务
        log_stage(1, 4, "处理待处理任务")
        pending_statuses = ["downloaded", "watermarked", "ai_captioned"]
        for group in groups:
            pending_tasks = self.db.get_tasks(status=pending_statuses[0], group_id=group.id, limit=100)
            for status in pending_statuses[1:]:
                pending_tasks.extend(self.db.get_tasks(status=status, group_id=group.id, limit=100))

            if pending_tasks:
                log(f"分组 {group.name}: {len(pending_tasks)} 个待处理任务")
                for task in pending_tasks:
                    if not self.running:
                        break
                    await self._process_pending_task(task, group)
                    stage_stats["上传"]["成功"] += 1
                    await asyncio.sleep(random.uniform(5, 10))

        # 阶段2: 抓取新视频
        log_stage(2, 4, "抓取新视频")
        for group in groups:
            if not self.running:
                break

            log(f"\n📁 分组: {group.name}")

            monitors = self.db.get_group_monitors(group.id)
            if not monitors:
                log(f"   无监控UP主")
                continue

            for monitor in monitors:
                if not self.running:
                    break

                customers = self.db.get_customers()
                customer = None
                for c in customers:
                    if c.homepage_url == monitor.up_url:
                        customer = c
                        break

                if not customer:
                    from hf.database.models import Customer
                    customer = Customer(
                        name=monitor.up_name,
                        user_id=f"group_{group.id}_{monitor.id}",
                        homepage_url=monitor.up_url
                    )
                    customer_id = self.db.add_customer(customer)
                    customer.id = customer_id

                if customer:
                    processed = await self.process_customer(customer.id, group.id)
                    log(f"   ✅ {customer.name}: {processed} 个视频")

                if self.running:
                    await asyncio.sleep(random.uniform(5, 10))

        # 阶段3: 本轮统计
        log_summary(f"第 {self.cycle_count} 轮完成", {
            "处理视频": self.processed_count,
            "耗时": f"{time.time() - (self.cycle_start_time if hasattr(self, 'cycle_start_time') else time.time()):.1f}秒"
        })
        self.print_status_summary()

    def print_status_summary(self):
        """打印状态摘要"""
        status_counts = {}
        error_tasks = []
        all_tasks = self.db.get_tasks(limit=10000)

        for task in all_tasks:
            status = task.status or "unknown"
            status_counts[status] = status_counts.get(status, 0) + 1

            if task.status in ["error", "download_failed", "upload_failed", "api_error"]:
                error_tasks.append(task)

        log("当前任务状态:")
        for status, count in sorted(status_counts.items()):
            log(f"  {status:<15} : {count}")
        log(f"  总计          : {len(all_tasks)}")

        if error_tasks:
            log_separator("⚠️ 失败任务摘要")
            for task in error_tasks[:10]:
                log(f"  [{task.id}] {task.status}: {task.error_msg[:60] if task.error_msg else 'N/A'}")
            if len(error_tasks) > 10:
                log(f"  ... 还有 {len(error_tasks) - 10} 个失败任务")
            log("使用 /list_errors 查看详情，使用 /retry all 重试")

    async def run_infinite_loop(self, interval_minutes=30):
        """无限循环运行"""
        log_separator("抖音视频爬虫系统 - 流水线模式")
        log("无限循环模式已启动")
        log(f"循环间隔: {interval_minutes} 分钟")
        log("\n流水线: 抓取 → 下载 → 水印 → AI文案 → 上传 → 清理")
        log("按 Ctrl+C 停止程序")
        log_separator()

        flag_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", ".run_now_flag")

        try:
            while self.running:
                cycle_start = time.time()

                await self.run_pipeline()

                if not self.running:
                    break

                cycle_end = time.time()
                cycle_duration = cycle_end - cycle_start

                log(f"本轮耗时 {cycle_duration:.1f} 秒")

                if os.path.exists(flag_file):
                    log("⚡ 检测到立即执行标志，跳过等待间隔")
                    try:
                        os.remove(flag_file)
                    except:
                        pass
                    continue

                next_run_time = cycle_end + interval_minutes * 60
                log(f"下次开始时间: {datetime.fromtimestamp(next_run_time).strftime('%H:%M:%S')} (间隔 {interval_minutes} 分钟)")

                while self.running:
                    remaining = next_run_time - time.time()
                    if remaining <= 0:
                        break

                    if os.path.exists(flag_file):
                        log("⚡ 检测到立即执行标志，跳过等待间隔")
                        try:
                            os.remove(flag_file)
                        except:
                            pass
                        break

                    sleep_seconds = min(remaining, 60)
                    await asyncio.sleep(sleep_seconds)

        except KeyboardInterrupt:
            log("\n收到停止信号，正在退出...", "WARN")
        finally:
            self.running = False
            log("程序已停止")

    def stop(self):
        self.running = False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="抖音视频爬虫系统 - 流水线模式")
    parser.add_argument("--interval", type=int, default=30, help="工作流循环间隔（分钟）")
    parser.add_argument("--once", action="store_true", help="仅执行一轮并退出")

    args = parser.parse_args()

    executor = PipelineExecutor()

    if args.once:
        log("执行单轮流水线模式")
        asyncio.run(executor.run_pipeline())
    else:
        asyncio.run(executor.run_infinite_loop(interval_minutes=args.interval))


if __name__ == "__main__":
    main()
