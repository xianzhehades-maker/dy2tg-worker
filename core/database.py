"""数据库模块 - SQLite封装"""

import sqlite3
import json
import threading
from pathlib import Path
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Task:
    id: int = 0
    group_id: int = 0
    video_id: str = ""
    video_url: str = ""
    platform: str = "douyin"
    status: str = "pending"
    retry_count: int = 0
    error_msg: str = ""
    created_at: str = ""
    updated_at: str = ""


@dataclass
class Monitor:
    id: int = 0
    group_id: int = 0
    up_url: str = ""
    platform: str = "douyin"
    status: str = "active"
    created_at: str = ""


@dataclass
class Group:
    id: int = 0
    name: str = ""
    target_channel: str = ""
    promotion_text: str = ""
    caption_style: str = "default"
    caption_length: int = 200
    caption_language: str = "chinese"
    check_interval: int = 300
    created_at: str = ""


class DatabaseManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.db_path = Path(__file__).parent.parent.parent / "data" / "bot.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = None
        self._cursor = None
        self._connect()
        self._init_tables()
        self._initialized = True

    def _connect(self):
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._cursor = self._conn.cursor()

    def _init_tables(self):
        self._cursor.executescript("""
            CREATE TABLE IF NOT EXISTS groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                target_channel TEXT DEFAULT '',
                promotion_text TEXT DEFAULT '',
                caption_style TEXT DEFAULT 'default',
                caption_length INTEGER DEFAULT 200,
                caption_language TEXT DEFAULT 'chinese',
                check_interval INTEGER DEFAULT 300,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                video_id TEXT NOT NULL,
                video_url TEXT NOT NULL,
                platform TEXT DEFAULT 'douyin',
                status TEXT DEFAULT 'pending',
                retry_count INTEGER DEFAULT 0,
                error_msg TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (group_id) REFERENCES groups (id)
            );

            CREATE TABLE IF NOT EXISTS monitors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                up_url TEXT NOT NULL,
                platform TEXT DEFAULT 'douyin',
                status TEXT DEFAULT 'active',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (group_id) REFERENCES groups (id)
            );

            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)

    def add_task(self, task: Task) -> int:
        now = datetime.now().isoformat()
        self._cursor.execute(
            """INSERT INTO tasks (group_id, video_id, video_url, platform, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (task.group_id, task.video_id, task.video_url, task.platform, task.status, now, now)
        )
        self._conn.commit()
        return self._cursor.lastrowid

    def get_task(self, task_id: int) -> Optional[Task]:
        self._cursor.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
        row = self._cursor.fetchone()
        if row:
            return Task(**dict(row))
        return None

    def get_tasks(self, group_id: int = None, status: str = None, limit: int = 100) -> List[Task]:
        query = "SELECT * FROM tasks WHERE 1=1"
        params = []

        if group_id:
            query += " AND group_id = ?"
            params.append(group_id)

        if status:
            query += " AND status = ?"
            params.append(status)

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        self._cursor.execute(query, params)
        return [Task(**dict(row)) for row in self._cursor.fetchall()]

    def update_task_status(self, task_id: int, status: str, error_msg: str = ""):
        now = datetime.now().isoformat()
        self._cursor.execute(
            "UPDATE tasks SET status = ?, error_msg = ?, updated_at = ? WHERE id = ?",
            (status, error_msg, now, task_id)
        )
        self._conn.commit()

    def add_group(self, group: Group) -> int:
        now = datetime.now().isoformat()
        self._cursor.execute(
            """INSERT INTO groups (name, target_channel, promotion_text, caption_style,
               caption_length, caption_language, check_interval, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (group.name, group.target_channel, group.promotion_text, group.caption_style,
             group.caption_length, group.caption_language, group.check_interval, now)
        )
        self._conn.commit()
        return self._cursor.lastrowid

    def get_group(self, group_id: int) -> Optional[Group]:
        self._cursor.execute("SELECT * FROM groups WHERE id = ?", (group_id,))
        row = self._cursor.fetchone()
        if row:
            return Group(**dict(row))
        return None

    def get_group_targets(self, group_id: int) -> List[str]:
        """获取分组的目标频道列表"""
        group = self.get_group(group_id)
        if not group or not group.target_channel:
            return []
        return [ch.strip() for ch in group.target_channel.split(",") if ch.strip()]

    def get_all_groups(self) -> List[Group]:
        self._cursor.execute("SELECT * FROM groups ORDER BY created_at DESC")
        return [Group(**dict(row)) for row in self._cursor.fetchall()]

    def update_group(self, group_id: int, **kwargs):
        for key, value in kwargs.items():
            self._cursor.execute(f"UPDATE groups SET {key} = ? WHERE id = ?", (value, group_id))
        self._conn.commit()

    def delete_group(self, group_id: int):
        self._cursor.execute("DELETE FROM tasks WHERE group_id = ?", (group_id,))
        self._cursor.execute("DELETE FROM monitors WHERE group_id = ?", (group_id,))
        self._cursor.execute("DELETE FROM groups WHERE id = ?", (group_id,))
        self._conn.commit()

    def add_monitor(self, monitor: Monitor) -> int:
        now = datetime.now().isoformat()
        self._cursor.execute(
            "INSERT INTO monitors (group_id, up_url, platform, status, created_at) VALUES (?, ?, ?, ?, ?)",
            (monitor.group_id, monitor.up_url, monitor.platform, monitor.status, now)
        )
        self._conn.commit()
        return self._cursor.lastrowid

    def get_monitors(self, group_id: int = None, status: str = None) -> List[Monitor]:
        query = "SELECT * FROM monitors WHERE 1=1"
        params = []

        if group_id:
            query += " AND group_id = ?"
            params.append(group_id)

        if status:
            query += " AND status = ?"
            params.append(status)

        self._cursor.execute(query, params)
        return [Monitor(**dict(row)) for row in self._cursor.fetchall()]

    def delete_monitor(self, monitor_id: int):
        self._cursor.execute("DELETE FROM monitors WHERE id = ?", (monitor_id,))
        self._conn.commit()

    def get_config(self, key: str, default=None) -> str:
        self._cursor.execute("SELECT value FROM config WHERE key = ?", (key,))
        row = self._cursor.fetchone()
        return row["value"] if row else default

    def set_config(self, key: str, value: str):
        self._cursor.execute(
            "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
            (key, value)
        )
        self._conn.commit()

    def close(self):
        if self._conn:
            self._conn.close()


_db_instance = None


def get_db() -> DatabaseManager:
    global _db_instance
    if _db_instance is None:
        _db_instance = DatabaseManager()
    return _db_instance