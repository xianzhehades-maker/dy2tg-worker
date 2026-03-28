import os
import sys


def initialize_module(caller_file: str = None):
    """统一模块初始化

    Args:
        caller_file: 调用者文件路径(__file__)，如果为 None 则自动获取
    """
    # 添加项目根目录到 Python 路径
    if caller_file is None:
        import inspect
        frame = inspect.stack()[1]
        caller_file = frame.filename

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(caller_file)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
