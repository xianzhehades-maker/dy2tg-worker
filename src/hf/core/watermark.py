import os
import sys
import io
import subprocess
import shutil
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import DatabaseManager, Task
from hf.utils.logger import get_logger
from config import DOWNLOAD_PATH, PROCESSED_PATH, UPLOAD_PATH

logger = get_logger("watermark")

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_FILE = os.path.join(parent_dir, "config", "watermark.json")

def find_chinese_font():
    """查找系统中可用的中文字体"""
    font_paths = []

    # 1. 先检查捆绑的中文字体（优先）
    bundled_font = os.path.join(parent_dir, "assets", "fonts", "chinese.otf")
    if os.path.exists(bundled_font):
        logger.info(f"使用捆绑的中文字体: {bundled_font}")
        return bundled_font

    if sys.platform == "win32":
        font_dir = r"C:\Windows\Fonts"
        if os.path.exists(font_dir):
            font_paths.append(os.path.join(font_dir, "simhei.ttf"))
            font_paths.append(os.path.join(font_dir, "msyh.ttc"))
            font_paths.append(os.path.join(font_dir, "simsun.ttc"))
            font_paths.append(os.path.join(font_dir, "simkai.ttf"))
            font_paths.append(os.path.join(font_dir, "STXIHEI.ttf"))
            font_paths.append(os.path.join(font_dir, "STSONG.ttf"))
    else:
        font_dirs = [
            "/usr/share/fonts",
            "/usr/share/fonts/truetype",
            "/usr/share/fonts/opentype",
            "/usr/local/share/fonts",
            "/usr/local/share/fonts/truetype",
            "/opt/conda/share/fonts",
            "/opt/conda/share/fonts/truetype",
            "/home/user/.fonts",
            "/root/.fonts",
            "/app/.fonts",
            "/app/fonts",
            os.path.expanduser("~/.fonts"),
            os.path.expanduser("~/.local/share/fonts"),
            "/app/data/fonts",
        ]
        for font_dir in font_dirs:
            if os.path.exists(font_dir):
                for root, dirs, files in os.walk(font_dir):
                    for f in files:
                        if f.endswith(('.ttf', '.TTC', '.TTF', '.otf', '.OTF')):
                            if any(kw in f.lower() for kw in ['chinese', 'cjk', 'hei', 'song', 'kai', 'ming', 'noto', 'zh', 'cn', 'sc']):
                                font_paths.append(os.path.join(root, f))

    for path in font_paths:
        if os.path.exists(path):
            logger.info(f"找到中文字体: {path}")
            return path

    logger.warn("未找到中文字体，使用默认字体")
    return None

CHINESE_FONT = find_chinese_font()

WATERMARK_IMAGE_PATH = os.path.join(parent_dir, "data", "watermark.png")

def load_config():
    """加载配置文件"""
    default_config = {
        "enabled": True,
        "text": "来财@yunyunyuyu",
        "font_size": 40,
        "color": "#FF0000",
        "opacity": 0.2,
        "position": "bottom-right",
        "margin_x": 10,
        "margin_y": 10,
        "use_image_watermark": True,
        "dynamic_enabled": False,
        "dynamic_formula_x": "(w-text_w)*(0.5+0.5*sin(0.2*t))",
        "dynamic_formula_y": "(h-text_h)*(0.5+0.5*cos(0.2*t))"
    }

    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
                for key, value in default_config.items():
                    if key not in config:
                        config[key] = value
                config["use_image_watermark"] = True
                config["dynamic_enabled"] = False
                return config
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            return default_config
    else:
        return default_config

class WatermarkProcessor:
    """水印处理器"""
    
    def __init__(self):
        # 每次初始化时重新加载配置，确保使用最新的设置
        self.config = load_config()
        # 创建必要的目录
        os.makedirs(DOWNLOAD_PATH, exist_ok=True)
        os.makedirs(PROCESSED_PATH, exist_ok=True)
        os.makedirs(UPLOAD_PATH, exist_ok=True)
        os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
    
    def add_watermark(self, input_file, output_file=None, task_id=None, need_watermark=True):
        """给视频添加水印"""
        if not need_watermark or not self.config.get("enabled", False):
            logger.info("不需要添加水印，直接复制文件")
            return self._copy_file(input_file, output_file, task_id=task_id)
        
        # 从输入文件名中提取 task_id，如果没有提供task_id参数
        filename = os.path.basename(input_file)
        if task_id is None:
            task_id = os.path.splitext(filename)[0]
        else:
            # 如果提供了task_id，确保是字符串类型
            task_id = str(task_id)
        
        # 重新定义输出路径到 processed 文件夹
        if output_file is None:
            output_file = os.path.join(PROCESSED_PATH, f"{task_id}_watermarked.mp4")
        
        # 校验：如果文件不存在，尝试在DOWNLOAD_PATH里找
        if not os.path.exists(input_file):
            potential_path = os.path.join(DOWNLOAD_PATH, os.path.basename(input_file))
            if os.path.exists(potential_path):
                input_file = potential_path
        
        try:
            logger.info(f"开始处理视频: {input_file}")

            use_image_watermark = self.config.get("use_image_watermark", True)

            if use_image_watermark and os.path.exists(WATERMARK_IMAGE_PATH):
                logger.info("使用图片水印模式")
                position = self.config.get("position", "bottom-right")
                margin_x = self.config.get("margin_x", 10)
                margin_y = self.config.get("margin_y", 10)

                cmd = self._build_image_watermark_command(
                    input_file,
                    output_file,
                    position,
                    margin_x,
                    margin_y
                )
            else:
                logger.info("使用文字水印模式")
                text = self.config.get("text", "来财@yunyunyuyu")
                font_size = self.config.get("font_size", 40)
                color = self.config.get("color", "#FF0000")
                opacity = self.config.get("opacity", 0.2)
                position = self.config.get("position", "bottom-right")
                margin_x = self.config.get("margin_x", 10)
                margin_y = self.config.get("margin_y", 10)
                dynamic_enabled = self.config.get("dynamic_enabled", False)
                dynamic_formula_x = self.config.get("dynamic_formula_x", "(w-tw)*(0.5+0.5*sin(0.2*t))")
                dynamic_formula_y = self.config.get("dynamic_formula_y", "(h-th)*(0.5+0.5*cos(0.2*t))")

                x, y = self._calculate_position(position, margin_x, margin_y)

                if dynamic_enabled and dynamic_formula_x and dynamic_formula_y:
                    x = dynamic_formula_x
                    y = dynamic_formula_y
                    logger.info(f"使用动态水印: x={x}, y={y}")

                cmd = self._build_ffmpeg_command(
                    input_file,
                    output_file,
                    text,
                    font_size,
                    color,
                    opacity,
                    x,
                    y
                )

            logger.info(f"执行FFmpeg命令: {' '.join(cmd)}")

            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                    timeout=300  # 5分钟超时
                )
            except subprocess.TimeoutExpired:
                logger.error("水印添加超时（5分钟），跳过此视频")
                return None
            
            if result.returncode == 0:
                logger.info(f"水印添加成功: {output_file}")
                
                # 复制到待上传目录
                upload_file = os.path.join(UPLOAD_PATH, os.path.basename(output_file))
                shutil.copy2(output_file, upload_file)
                logger.info(f"已复制到待上传目录: {upload_file}")
                
                # 成功后：删原片
                if os.path.exists(input_file):
                    os.remove(input_file)
                    logger.info(f"已删除原片: {input_file}")
                
                return upload_file
            else:
                logger.error(f"水印添加失败: {result.stderr}")
                return None
                
        except Exception as e:
            logger.error(f"水印处理异常: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _calculate_position(self, position, margin_x, margin_y):
        """计算水印位置"""
        if position == "top-left":
            return margin_x, margin_y
        elif position == "top-right":
            return f"(w-text_w-{margin_x})", margin_y
        elif position == "bottom-left":
            return margin_x, f"(h-text_h-{margin_y})"
        elif position == "bottom-right":
            return f"(w-text_w-{margin_x})", f"(h-text_h-{margin_y})"
        else:
            return 10, 10
    
    def _build_ffmpeg_command(self, input_file, output_file, text, font_size, color, opacity, x, y):
        """构建FFmpeg命令（文字水印）"""

        if os.path.exists(os.path.join(parent_dir, "ffmpeg.exe")):
            ffmpeg_path = os.path.join(parent_dir, "ffmpeg.exe")
        else:
            ffmpeg_path = "ffmpeg"

        if CHINESE_FONT:
            drawtext_params = f"drawtext=text='{text}':fontfile='{CHINESE_FONT}':fontcolor=white:fontsize={font_size}:x={x}:y={y}:alpha={opacity}:enable='gt(t,0)'"
        else:
            drawtext_params = f"drawtext=text='{text}':fontcolor=white:fontsize={font_size}:x={x}:y={y}:alpha={opacity}:enable='gt(t,0)'"

        cmd = [
            ffmpeg_path,
            "-i", input_file,
            "-vf", drawtext_params,
            "-c:a", "copy",
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-y",
            output_file
        ]

        return cmd

    def _build_image_watermark_command(self, input_file, output_file, position, margin_x, margin_y):
        """构建图片水印FFmpeg命令（使用overlay滤镜，CPU友好）"""

        if os.path.exists(os.path.join(parent_dir, "ffmpeg.exe")):
            ffmpeg_path = os.path.join(parent_dir, "ffmpeg.exe")
        else:
            ffmpeg_path = "ffmpeg"

        if position == "top-left":
            x = margin_x
            y = margin_y
        elif position == "top-right":
            x = f"(W-w-{margin_x})"
            y = margin_y
        elif position == "bottom-left":
            x = margin_x
            y = f"(H-h-{margin_y})"
        elif position == "bottom-right":
            x = f"(W-w-{margin_x})"
            y = f"(H-h-{margin_y})"
        else:
            x = f"(W-w-{margin_x})"
            y = f"(H-h-{margin_y})"

        overlay_params = f"overlay={x}:{y}"

        cmd = [
            ffmpeg_path,
            "-i", input_file,
            "-i", WATERMARK_IMAGE_PATH,
            "-filter_complex", overlay_params,
            "-c:a", "copy",
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-y",
            output_file
        ]

        return cmd
    
    def _hex_to_rgb(self, hex_color):
        """转换十六进制颜色为RGB"""
        hex_color = hex_color.lstrip('#')
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    
    def _copy_file(self, input_file, output_file=None, task_id=None):
        """直接复制文件（不添加水印）"""
        # 从输入文件名中提取 task_id，如果没有提供task_id参数
        filename = os.path.basename(input_file)
        if task_id is None:
            task_id = os.path.splitext(filename)[0]
        else:
            # 如果提供了task_id，确保是字符串类型
            task_id = str(task_id)
        
        # 不加水印的文件直接复制到 processed 文件夹，然后再复制到 upload 文件夹
        if output_file is None:
            output_file = os.path.join(PROCESSED_PATH, f"{task_id}.mp4")
        
        shutil.copy2(input_file, output_file)
        logger.info(f"文件已复制到processed: {output_file}")
        
        # 复制到待上传目录
        upload_file = os.path.join(UPLOAD_PATH, os.path.basename(output_file))
        shutil.copy2(output_file, upload_file)
        logger.info(f"已复制到待上传目录: {upload_file}")
        
        return upload_file

def process_single_task(db, task_id):
    """处理单个任务"""
    task = db.get_task(task_id)
    if not task:
        logger.error(f"任务 {task_id} 不存在")
        return
    
    file_path = task.file_path
    status = task.status
    
    # 状态为 downloaded 说明已经成功下载，直接打水印
    need_watermark = True
    
    logger.info(f"开始处理任务 [{task_id}]: {file_path}, 状态: {status}, 需要水印: {need_watermark}")
    
    if not file_path or not os.path.exists(file_path):
        logger.error(f"文件不存在: {file_path}")
        db.update_task(task_id, status="error", error_msg="文件不存在")
        return
    
    processor = WatermarkProcessor()
    
    # 使用传入的task_id，而不是从文件名提取
    output_file = processor.add_watermark(file_path, task_id=task_id, need_watermark=need_watermark)
    
    if output_file:
        # 更新任务状态为 watermarked，并更新文件路径
        db.update_task(task_id, status="watermarked", watermark_time=datetime.now(), file_path=output_file)
        logger.info(f"任务 [{task_id}] 处理完成: {output_file}")
        print(f"✅ 任务 [{task_id}] 处理完成")
    else:
        # 更新任务状态为错误
        db.update_task(task_id, status="error", error_msg="水印处理失败")
        logger.error(f"任务 [{task_id}] 处理失败")
        print(f"❌ 任务 [{task_id}] 处理失败")

def process_watermark_task():
    """处理水印任务"""
    db = DatabaseManager()
    logger.info("水印处理器已启动")
    
    processor = WatermarkProcessor()
    
    # 获取所有已下载的任务和error状态但有文件的任务
    all_downloaded_tasks = db.get_tasks(status="downloaded")
    error_tasks_with_file = [task for task in db.get_tasks(status="error") if task.file_path and os.path.exists(task.file_path)]
    tasks = all_downloaded_tasks + error_tasks_with_file
    
    if error_tasks_with_file:
        logger.info(f"发现 {len(error_tasks_with_file)} 个error状态但有文件的任务，将重新处理")
        print(f"🔧 发现 {len(error_tasks_with_file)} 个error状态但有文件的任务，将重新处理")
    
    if not tasks:
        logger.info(f"没有更多待处理的任务，退出")
        return
    
    task_count = 0
    for task in tasks:
        task_id = task.id
        file_path = task.file_path
        status = task.status
        
        # 状态为 downloaded 说明已经成功下载，直接打水印
        need_watermark = True
        
        logger.info(f"开始处理任务 [{task_id}]: {file_path}, 状态: {status}, 需要水印: {need_watermark}")
        print(f"🔍 开始处理任务 [{task_id}]: {file_path}, 状态: {status}, 需要水印: {need_watermark}")
        
        if not file_path or not os.path.exists(file_path):
            logger.error(f"文件不存在: {file_path}")
            db.update_task(task_id, status="error", error_msg="文件不存在")
            task_count += 1
            continue
        
        output_file = processor.add_watermark(file_path, task_id=task_id, need_watermark=need_watermark)
        
        if output_file:
            # 更新任务状态为 watermarked，并更新文件路径
            # 如果任务当前状态是error，说明之前下载有问题但文件已经存在，现在成功处理水印后应该更新状态
            db.update_task(task_id, status="watermarked", watermark_time=datetime.now(), file_path=output_file)
            logger.info(f"任务 [{task_id}] 处理完成: {output_file}")
            print(f"✅ 任务 [{task_id}] 处理完成")
            task_count += 1
        else:
            # 只有当任务不是error状态时才更新为error，避免覆盖原有的错误信息
            if status != "error":
                db.update_task(task_id, status="error", error_msg="水印处理失败")
                logger.error(f"任务 [{task_id}] 处理失败")
                print(f"❌ 任务 [{task_id}] 处理失败")
            else:
                # 任务已经是error状态，保持原有错误信息
                logger.error(f"任务 [{task_id}] 水印处理失败（保持原有error状态）")
                print(f"❌ 任务 [{task_id}] 水印处理失败（保持原有error状态）")
            task_count += 1
    
    logger.info(f"水印任务处理完成，共处理 {task_count} 个任务")

if __name__ == "__main__":
    import argparse
    import time
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-id", type=int, help="指定任务ID")
    args = parser.parse_args()
    
    if args.task_id:
        # 处理指定任务
        db = DatabaseManager()
        process_single_task(db, args.task_id)
    else:
        # 处理所有已下载的任务
        process_watermark_task()
