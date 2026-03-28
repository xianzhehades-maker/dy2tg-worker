"""下载模块 - 使用抖音 API 直连下载"""

import os
import sys
import io
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from adapter import DownloaderAdapter


def main():
    """主函数"""
    from hf.utils.module_initializer import initialize_module
    initialize_module(__file__)
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-id", type=int, help="指定任务ID")
    args = parser.parse_args()
    
    adapter = DownloaderAdapter()
    
    import asyncio
    if args.task_id:
        asyncio.run(adapter.download_single_task(args.task_id))
    else:
        asyncio.run(adapter.download_all_tasks())


if __name__ == "__main__":
    main()
