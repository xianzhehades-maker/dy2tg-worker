"""Bot指令模块"""

from .handler import BotCommandHandler, CommandResult, parse_command

__all__ = [
    "BotCommandHandler",
    "CommandResult",
    "parse_command",
]