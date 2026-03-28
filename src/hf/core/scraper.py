"""抓取模块 - 使用抖音 API 直连抓取"""

import os
import sys
import io
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from adapter import ScraperAdapter


def main():
    """主函数"""
    from hf.utils.module_initializer import initialize_module
    initialize_module(__file__)
    
    parser = argparse.ArgumentParser()
    parser.add_argument("customer_id", type=int, nargs="?", help="客户ID")
    parser.add_argument("--group-id", type=int, help="分组ID（分组模式）")
    parser.add_argument("--max-days", type=int, default=3, 
                        help="只下载最近N天内的视频（默认: 3天）")
    args = parser.parse_args()
    
    adapter = ScraperAdapter(max_days_old=args.max_days)
    
    import asyncio
    if args.group_id:
        asyncio.run(adapter.scrape_group_videos(args.group_id))
    else:
        asyncio.run(adapter.scrape_user_videos(args.customer_id))


if __name__ == "__main__":
    main()
