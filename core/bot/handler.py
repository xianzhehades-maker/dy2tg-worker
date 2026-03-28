"""Bot指令处理器"""

import re
import asyncio
from typing import Optional, Tuple, List
from dataclasses import dataclass

from ..database import DatabaseManager, Group, Task, Monitor, get_db
from ..client import VideoClient, Platform, extract_video_id


@dataclass
class CommandResult:
    success: bool
    message: str
    data: dict = None


class BotCommandHandler:
    VIDEO_URL_PATTERN = re.compile(r"https?://(?:www\.)?(?:douyin\.com|tiktok\.com)/video/\d+")

    def __init__(self, db: DatabaseManager = None, workflow_trigger=None):
        self.db = db or get_db()
        self.video_client = None
        self.workflow_trigger = workflow_trigger

    def set_video_client(self, client: VideoClient):
        self.video_client = client

    def set_workflow_trigger(self, trigger):
        self.workflow_trigger = trigger

    async def handle_add_url(self, group_id: int, text: str) -> CommandResult:
        try:
            urls = self._extract_urls(text)
            if not urls:
                return CommandResult(
                    success=False,
                    message="❌ 未检测到有效的视频链接\n\n支持格式:\n• 抖音: https://www.douyin.com/video/xxxxx\n• TikTok: https://www.tiktok.com/@user/video/xxxxx",
                )

            added_count = 0
            for url in urls:
                video_id, platform = extract_video_id(url)

                task = Task(
                    group_id=group_id,
                    video_id=video_id,
                    video_url=url,
                    platform=platform.value,
                    status="pending",
                )
                self.db.add_task(task)
                added_count += 1

            if self.workflow_trigger:
                self.workflow_trigger.set_immediate()

            platform_hint = "抖音/TikTok" if len(set(urls)) > 1 else (">抖音" if "douyin" in urls[0] else "TikTok")

            return CommandResult(
                success=True,
                message=f"✅ 成功添加 {added_count} 个视频任务 [{platform_hint}]\n\n任务已开始处理...",
                data={"count": added_count},
            )

        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 添加视频失败: {str(e)}",
            )

    def _extract_urls(self, text: str) -> List[str]:
        urls = []
        for match in self.VIDEO_URL_PATTERN.finditer(text):
            urls.append(match.group(0))
        return urls

    async def handle_add_group(self, name: str) -> CommandResult:
        try:
            group = Group(name=name)
            group_id = self.db.add_group(group)

            return CommandResult(
                success=True,
                message=f"✅ 分组创建成功\n\nID: {group_id}\n名称: {name}",
                data={"group_id": group_id},
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 创建分组失败: {str(e)}",
            )

    async def handle_del_group(self, group_id: int) -> CommandResult:
        try:
            group = self.db.get_group(group_id)
            if not group:
                return CommandResult(
                    success=False,
                    message=f"❌ 分组 {group_id} 不存在",
                )

            self.db.delete_group(group_id)

            return CommandResult(
                success=True,
                message=f"✅ 分组 {group_id} 已删除",
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 删除分组失败: {str(e)}",
            )

    async def handle_rename_group(self, group_id: int, new_name: str) -> CommandResult:
        try:
            group = self.db.get_group(group_id)
            if not group:
                return CommandResult(
                    success=False,
                    message=f"❌ 分组 {group_id} 不存在",
                )

            self.db.update_group(group_id, name=new_name)

            return CommandResult(
                success=True,
                message=f"✅ 分组已重命名\n\nID: {group_id}\n新名称: {new_name}",
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 重命名失败: {str(e)}",
            )

    async def handle_add_monitor(self, group_id: int, up_url: str) -> CommandResult:
        try:
            group = self.db.get_group(group_id)
            if not group:
                return CommandResult(
                    success=False,
                    message=f"❌ 分组 {group_id} 不存在",
                )

            platform = "douyin" if "douyin.com" in up_url else "tiktok"

            monitor = Monitor(
                group_id=group_id,
                up_url=up_url,
                platform=platform,
                status="active",
            )
            monitor_id = self.db.add_monitor(monitor)

            return CommandResult(
                success=True,
                message=f"✅ 监控添加成功\n\n分组: {group_id}\n平台: {platform.upper()}\n链接: {up_url}",
                data={"monitor_id": monitor_id},
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 添加监控失败: {str(e)}",
            )

    async def handle_del_monitor(self, group_id: int, up_url: str) -> CommandResult:
        try:
            monitors = self.db.get_monitors(group_id=group_id)

            for m in monitors:
                if m.up_url == up_url:
                    self.db.delete_monitor(m.id)
                    return CommandResult(
                        success=True,
                        message=f"✅ 监控已删除\n\n链接: {up_url}",
                    )

            return CommandResult(
                success=False,
                message=f"❌ 未找到监控: {up_url}",
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 删除监控失败: {str(e)}",
            )

    async def handle_mon_list(self, group_id: int = None) -> CommandResult:
        try:
            monitors = self.db.get_monitors(group_id=group_id)

            if not monitors:
                return CommandResult(
                    success=True,
                    message="📋 暂无监控",
                )

            lines = ["📋 监控列表：\n"]
            for i, m in enumerate(monitors, 1):
                lines.append(f"{i}. [{m.platform.upper()}] {m.up_url}")

            return CommandResult(
                success=True,
                message="\n".join(lines),
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 获取监控列表失败: {str(e)}",
            )

    async def handle_add_target(self, group_id: int, channel: str) -> CommandResult:
        try:
            self.db.update_group(group_id, target_channel=channel)

            return CommandResult(
                success=True,
                message=f"✅ 目标频道已设置\n\n分组: {group_id}\n频道: {channel}",
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 设置目标频道失败: {str(e)}",
            )

    async def handle_del_target(self, group_id: int) -> CommandResult:
        try:
            self.db.update_group(group_id, target_channel="")

            return CommandResult(
                success=True,
                message=f"✅ 目标频道已移除",
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 移除目标频道失败: {str(e)}",
            )

    async def handle_set_promotion(self, group_id: int, text: str) -> CommandResult:
        try:
            self.db.update_group(group_id, promotion_text=text)

            return CommandResult(
                success=True,
                message=f"✅ 推广文案已设置\n\n文案: {text[:100]}{'...' if len(text) > 100 else ''}",
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 设置推广文案失败: {str(e)}",
            )

    async def handle_set_caption_style(self, group_id: int, style: str) -> CommandResult:
        try:
            valid_styles = ["default", "口播", "humor", "short", "乐子", "none", "舞蹈", "bilingual", "双语"]

            if style.lower() not in valid_styles:
                return CommandResult(
                    success=False,
                    message=f"❌ 不支持的风格: {style}\n\n可选: {', '.join(valid_styles)}",
                )

            style_map = {
                "口播": "default",
                "humor": "humor",
                "short": "humor",
                "乐子": "humor",
                "none": "none",
                "舞蹈": "none",
                "bilingual": "bilingual",
                "双语": "bilingual",
            }

            final_style = style_map.get(style.lower(), style.lower())
            self.db.update_group(group_id, caption_style=final_style)

            return CommandResult(
                success=True,
                message=f"✅ 文案风格已设置\n\n风格: {final_style}",
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 设置文案风格失败: {str(e)}",
            )

    async def handle_set_caption_len(self, group_id: int, length: int) -> CommandResult:
        try:
            if length < 50 or length > 500:
                return CommandResult(
                    success=False,
                    message="❌ 字数必须在50-500之间",
                )

            self.db.update_group(group_id, caption_length=length)

            return CommandResult(
                success=True,
                message=f"✅ AI文案字数已设置\n\n字数: {length}",
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 设置字数失败: {str(e)}",
            )

    async def handle_status(self) -> CommandResult:
        try:
            groups = self.db.get_all_groups()

            if not groups:
                return CommandResult(
                    success=True,
                    message="📊 暂无分组",
                )

            lines = ["📊 分组状态：\n"]
            for g in groups:
                tasks = self.db.get_tasks(group_id=g.id, limit=100)
                pending = len([t for t in tasks if t.status == "pending"])
                completed = len([t for t in tasks if t.status == "uploaded"])
                failed = len([t for t in tasks if t.status == "failed"])

                lines.append(f"📁 {g.name} (ID:{g.id})")
                lines.append(f"   待处理: {pending} | 已完成: {completed} | 失败: {failed}")

            return CommandResult(
                success=True,
                message="\n".join(lines),
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 获取状态失败: {str(e)}",
            )

    async def handle_queue(self) -> CommandResult:
        try:
            tasks = self.db.get_tasks(status="pending", limit=50)

            if not tasks:
                return CommandResult(
                    success=True,
                    message="📋 队列为空",
                )

            lines = [f"📋 待处理任务 ({len(tasks)}个)：\n"]
            for i, t in enumerate(tasks[:10], 1):
                lines.append(f"{i}. {t.video_url} [{t.platform}]")

            if len(tasks) > 10:
                lines.append(f"... 还有 {len(tasks) - 10} 个")

            return CommandResult(
                success=True,
                message="\n".join(lines),
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 获取队列失败: {str(e)}",
            )

    async def handle_sync(self) -> CommandResult:
        return CommandResult(
            success=True,
            message="✅ 配置已同步",
        )

    async def handle_clear_cache(self) -> CommandResult:
        try:
            import shutil
            from pathlib import Path

            cache_dir = Path(__file__).parent.parent.parent / "temp_hf_cache"
            if cache_dir.exists():
                shutil.rmtree(cache_dir)

            return CommandResult(
                success=True,
                message="✅ 缓存已清除",
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 清除缓存失败: {str(e)}",
            )

    async def handle_time(self, group_id: int = None, interval: int = None) -> CommandResult:
        try:
            if interval is not None:
                if interval < 60 or interval > 3600:
                    return CommandResult(
                        success=False,
                        message="❌ 间隔时间必须在60-3600秒之间",
                    )

                if group_id:
                    self.db.update_group(group_id, check_interval=interval)
                    return CommandResult(
                        success=True,
                        message=f"✅ 分组 {group_id} 检查间隔已设置为 {interval} 秒",
                    )
                else:
                    return CommandResult(
                        success=False,
                        message="❌ 请指定分组ID",
                    )

            if group_id:
                group = self.db.get_group(group_id)
                if group:
                    return CommandResult(
                        success=True,
                        message=f"📊 分组 {group_id} 检查间隔: {group.check_interval} 秒",
                    )

            return CommandResult(
                success=True,
                message="📊 用法: /time <group_id> <秒数>",
            )
        except Exception as e:
            return CommandResult(
                success=False,
                message=f"❌ 设置失败: {str(e)}",
            )

    async def handle_help(self) -> CommandResult:
        help_text = """
🤖 Bot 命令帮助

【分组管理】
/add_group <名称> - 添加分组
/del_group <id> - 删除分组
/rename_group <id> <新名称> - 重命名分组

【监控管理】
/addup <group_id> <url> - 添加UP主监控
/del_monitor <group_id> <url> - 删除监控
/mon_list - 查看所有监控

【目标管理】
/add_target <group_id> @channel - 添加目标频道
/del_target <group_id> - 移除目标频道

【设置】
/set_promotion <group_id> <文案> - 设置推广文案
/set_caption_style <group_id> <风格> - 设置文案风格
  风格: default(口播), humor(乐子), none(无AI), bilingual(双语)
/set_caption_len <group_id> <字数> - 设置AI文案字数(50-500)

【视频】
/add_url <url1>
<url2>... - 添加视频任务(立即执行)

【系统】
/status - 查看所有分组状态
/queue - 查看待处理队列
/sync - 同步配置
/clear_cache - 清除缓存
/time <group_id> [秒数] - 查看/设置检查间隔
/help - 显示此帮助
"""
        return CommandResult(
            success=True,
            message=help_text.strip(),
        )


def parse_command(text: str) -> Tuple[str, List[str]]:
    text = text.strip()

    if text.startswith("/"):
        parts = text.split(None, 1)
        cmd = parts[0].lower()
        args = parts[1].split(None) if len(parts) > 1 else []
        return cmd, args

    return "", []