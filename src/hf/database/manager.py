"""数据库管理模块"""
import sqlite3
import json
import os
from datetime import datetime
from typing import List, Optional
from .models import Customer, Task, TaskPlan, Execution, UploadTemplate, MonitorGroup, GroupMonitor, GroupTarget, SystemConfig, DiscoveredVideo

class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self, db_path=None):
        if db_path is None:
            parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            db_path = os.path.join(parent_dir, "data", "tasks.db")
        self.db_path = os.path.abspath(db_path)
        self.init_db()
    
    def get_conn(self):
        """获取数据库连接 - 使用 row_factory 以字典方式访问数据"""
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_db(self):
        """初始化数据库"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        conn = self.get_conn()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                user_id TEXT NOT NULL,
                homepage_url TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER,
                video_id TEXT,
                video_url TEXT NOT NULL,
                video_publish_time TIMESTAMP,
                download_url TEXT,
                status TEXT DEFAULT 'pending',
                download_time TIMESTAMP,
                watermark_time TIMESTAMP,
                ai_caption_time TIMESTAMP,
                upload_time TIMESTAMP,
                upload_bot_id TEXT,
                upload_channel_id INTEGER,
                file_path TEXT,
                video_desc TEXT,
                ai_caption TEXT,
                error_msg TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(id),
                UNIQUE(video_id),
                UNIQUE(video_url)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_plans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                customer_ids TEXT NOT NULL,
                bot_config TEXT NOT NULL,
                execution_type TEXT DEFAULT 'once',
                target_count INTEGER,
                interval_minutes INTEGER DEFAULT 30,
                upload_template_id INTEGER,
                enabled INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plan_id INTEGER,
                mode TEXT DEFAULT 'manual',
                status TEXT DEFAULT 'stopped',
                current_step TEXT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                cycle_count INTEGER DEFAULT 0,
                cycle_duration INTEGER DEFAULT 0,
                tasks_created INTEGER DEFAULT 0,
                tasks_completed INTEGER DEFAULT 0,
                tasks_failed INTEGER DEFAULT 0,
                error_msg TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (plan_id) REFERENCES task_plans(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS upload_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS monitor_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                promotion_text TEXT,
                ai_caption_style TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS group_monitors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                up_name TEXT NOT NULL,
                up_url TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (group_id) REFERENCES monitor_groups(id) ON DELETE CASCADE,
                UNIQUE(group_id, up_url)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS group_targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                target_channel TEXT NOT NULL,
                chat_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (group_id) REFERENCES monitor_groups(id) ON DELETE CASCADE,
                UNIQUE(group_id, target_channel)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_key TEXT NOT NULL UNIQUE,
                config_value TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discovered_videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER,
                video_id TEXT NOT NULL,
                video_url TEXT NOT NULL,
                video_publish_time TIMESTAMP,
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_qualified INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(id),
                UNIQUE(video_id),
                UNIQUE(customer_id, video_id)
            )
        ''')
        
        # 添加 tasks 表的索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_customer ON tasks(customer_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_video_id ON tasks(video_id)')

        # 添加 discovered_videos 表的索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_discovered_customer ON discovered_videos(customer_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_discovered_video_id ON discovered_videos(video_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_discovered_qualified ON discovered_videos(is_qualified)')

        conn.commit()
        conn.close()

        # 运行数据库迁移
        self.migrate_db()

        # 迁移后添加可能缺失的索引
        self._create_missing_indexes()
    
    def migrate_db(self):
        """数据库迁移 - 添加新字段"""
        conn = self.get_conn()
        cursor = conn.cursor()
        
        try:
            # 检查task_plans表是否有upload_template_id字段
            cursor.execute("PRAGMA table_info(task_plans)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'upload_template_id' not in columns:
                cursor.execute('ALTER TABLE task_plans ADD COLUMN upload_template_id INTEGER')
                conn.commit()
                print("[迁移] 已为task_plans表添加upload_template_id字段")
            
            # 检查task_plans表是否有workflow_steps字段
            if 'workflow_steps' not in columns:
                cursor.execute('ALTER TABLE task_plans ADD COLUMN workflow_steps TEXT')
                conn.commit()
                print("[迁移] 已为task_plans表添加workflow_steps字段")
            
            # 检查tasks表是否有ai_caption和ai_caption_time字段
            cursor.execute("PRAGMA table_info(tasks)")
            task_columns = [column[1] for column in cursor.fetchall()]
            
            if 'ai_caption' not in task_columns:
                cursor.execute('ALTER TABLE tasks ADD COLUMN ai_caption TEXT')
                conn.commit()
                print("[迁移] 已为tasks表添加ai_caption字段")
            
            if 'ai_caption_time' not in task_columns:
                cursor.execute('ALTER TABLE tasks ADD COLUMN ai_caption_time TIMESTAMP')
                conn.commit()
                print("[迁移] 已为tasks表添加ai_caption_time字段")
            
            if 'video_publish_time' not in task_columns:
                cursor.execute('ALTER TABLE tasks ADD COLUMN video_publish_time TIMESTAMP')
                conn.commit()
                print("[迁移] 已为tasks表添加video_publish_time字段")
            
            if 'video_id' not in task_columns:
                cursor.execute('ALTER TABLE tasks ADD COLUMN video_id TEXT')
                conn.commit()
                print("[迁移] 已为tasks表添加video_id字段")
            
            if 'download_url' not in task_columns:
                cursor.execute('ALTER TABLE tasks ADD COLUMN download_url TEXT')
                conn.commit()
                print("[迁移] 已为tasks表添加download_url字段")

            if 'video_desc' not in task_columns:
                cursor.execute('ALTER TABLE tasks ADD COLUMN video_desc TEXT')
                conn.commit()
                print("[迁移] 已为tasks表添加video_desc字段")

            if 'group_id' not in task_columns:
                cursor.execute('ALTER TABLE tasks ADD COLUMN group_id INTEGER')
                conn.commit()
                print("[迁移] 已为tasks表添加group_id字段")

            # 检查monitor_groups表是否有ai_caption_style字段
            cursor.execute("PRAGMA table_info(monitor_groups)")
            group_columns = [column[1] for column in cursor.fetchall()]
            if 'ai_caption_style' not in group_columns:
                cursor.execute('ALTER TABLE monitor_groups ADD COLUMN ai_caption_style TEXT')
                conn.commit()
                print("[迁移] 已为monitor_groups表添加ai_caption_style字段")

            # 检查discovered_videos表是否存在，如果不存在则创建（兼容旧版本）
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='discovered_videos'")
            if not cursor.fetchone():
                cursor.execute('''
                    CREATE TABLE discovered_videos (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        customer_id INTEGER,
                        video_id TEXT NOT NULL,
                        video_url TEXT NOT NULL,
                        video_publish_time TIMESTAMP,
                        discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_qualified INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (customer_id) REFERENCES customers(id),
                        UNIQUE(video_id),
                        UNIQUE(customer_id, video_id)
                    )
                ''')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_discovered_customer ON discovered_videos(customer_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_discovered_video_id ON discovered_videos(video_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_discovered_qualified ON discovered_videos(is_qualified)')
                conn.commit()
                print("[迁移] 已创建discovered_videos表")
        except Exception as e:
            print(f"[迁移] 数据库迁移出错: {e}")
        finally:
            conn.close()

    def _create_missing_indexes(self):
        """创建可能缺失的索引（在迁移添加列之后调用）"""
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("PRAGMA index_list(tasks)")
            existing_indexes = [row[1] for row in cursor.fetchall()]

            if 'idx_tasks_group' not in existing_indexes:
                cursor.execute("PRAGMA table_info(tasks)")
                columns = [col[1] for col in cursor.fetchall()]
                if 'group_id' in columns:
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_group ON tasks(group_id)')
                    conn.commit()
                    print("[索引] 已创建 idx_tasks_group 索引")
        except Exception as e:
            print(f"[索引] 创建索引出错: {e}")
        finally:
            conn.close()
    
    def add_customer(self, customer: Customer) -> int:
        """添加客户"""
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO customers (name, user_id, homepage_url)
                VALUES (?, ?, ?)
            ''', (customer.name, customer.user_id, customer.homepage_url))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None
        finally:
            conn.close()
    
    def get_customers(self) -> List[Customer]:
        """获取所有客户"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM customers ORDER BY created_at DESC')
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_customer(row) for row in rows]
    
    def get_customer(self, customer_id: int) -> Optional[Customer]:
        """获取客户"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM customers WHERE id = ?', (customer_id,))
        row = cursor.fetchone()
        conn.close()
        return self._row_to_customer(row) if row else None
    
    def update_customer(self, customer_id: int, **kwargs):
        conn = self.get_conn()
        cursor = conn.cursor()
        fields = []
        values = []
        for key, value in kwargs.items():
            if value is not None:
                fields.append(f'{key} = ?')
                values.append(value)
        if fields:
            cursor.execute(f'UPDATE customers SET {", ".join(fields)}, updated_at = ? WHERE id = ?', values + [datetime.now(), customer_id])
            conn.commit()
        conn.close()
    
    def delete_customer(self, customer_id: int):
        """删除客户"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM customers WHERE id = ?', (customer_id,))
        conn.commit()
        conn.close()
    
    def add_task(self, task: Task) -> int:
        """添加任务"""
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO tasks (group_id, customer_id, video_id, video_url, video_publish_time, download_url, status, file_path, video_desc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (task.group_id, task.customer_id, task.video_id, task.video_url,
                  task.video_publish_time.isoformat() if task.video_publish_time else None,
                  task.download_url,
                  task.status,
                  task.file_path,
                  task.video_desc))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None
        finally:
            conn.close()
    
    def get_tasks(self, status: str = None, customer_id: int = None, group_id: int = None, limit: int = 1000) -> List[Task]:
        """获取任务列表"""
        conn = self.get_conn()
        cursor = conn.cursor()
        query = 'SELECT * FROM tasks WHERE 1=1'
        params = []

        if status:
            query += ' AND status = ?'
            params.append(status)

        if customer_id:
            query += ' AND customer_id = ?'
            params.append(customer_id)

        if group_id:
            query += ' AND group_id = ?'
            params.append(group_id)

        query += ' ORDER BY created_at DESC LIMIT ?'
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        conn.close()
        return [self._row_to_task(row) for row in rows]
    
    def get_task(self, task_id: int) -> Optional[Task]:
        """获取任务"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM tasks WHERE id = ?', (task_id,))
        row = cursor.fetchone()
        conn.close()
        return self._row_to_task(row) if row else None
    
    def update_task(self, task_id: int, **kwargs):
        conn = self.get_conn()
        cursor = conn.cursor()
        fields = []
        values = []
        for key, value in kwargs.items():
            if value is not None:
                fields.append(f'{key} = ?')
                values.append(value)
        if fields:
            cursor.execute(f'UPDATE tasks SET {", ".join(fields)}, updated_at = ? WHERE id = ?', values + [datetime.now(), task_id])
            conn.commit()
        conn.close()
    
    def delete_task(self, task_id: int):
        """删除任务"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM tasks WHERE id = ?', (task_id,))
        conn.commit()
        conn.close()
    
    def video_exists(self, video_url: str) -> bool:
        """
        高效检查视频URL是否已存在
        
        Args:
            video_url: 视频URL
            
        Returns:
            True表示已存在，False表示不存在
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM tasks WHERE video_url = ? LIMIT 1', (video_url,))
        exists = cursor.fetchone() is not None
        conn.close()
        return exists
    
    def video_id_exists(self, video_id: str) -> bool:
        """
        高效检查视频ID是否已存在（推荐使用）
        
        Args:
            video_id: 视频ID
            
        Returns:
            True表示已存在，False表示不存在
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM tasks WHERE video_id = ? LIMIT 1', (video_id,))
        exists = cursor.fetchone() is not None
        conn.close()
        return exists
    
    def get_video_task(self, video_url: str) -> Optional[Task]:
        """
        根据视频URL获取任务
        
        Args:
            video_url: 视频URL
            
        Returns:
            Task对象或None
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM tasks WHERE video_url = ? LIMIT 1', (video_url,))
        row = cursor.fetchone()
        conn.close()
        return self._row_to_task(row) if row else None
    
    def get_video_task_by_id(self, video_id: str) -> Optional[Task]:
        """
        根据视频ID获取任务（推荐使用）
        
        Args:
            video_id: 视频ID
            
        Returns:
            Task对象或None
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM tasks WHERE video_id = ? LIMIT 1', (video_id,))
        row = cursor.fetchone()
        conn.close()
        return self._row_to_task(row) if row else None
    
    def get_task_queue_stats(self) -> dict:
        """
        获取任务队列统计信息
        
        Returns:
            包含各状态任务数量的字典
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT status, COUNT(*) as count
            FROM tasks
            GROUP BY status
        ''')
        rows = cursor.fetchall()
        conn.close()
        return {row[0]: row[1] for row in rows}
    
    def add_task_plan(self, plan: TaskPlan) -> int:
        """添加任务计划"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('''
                INSERT INTO task_plans (name, customer_ids, bot_config, execution_type, target_count, interval_minutes, upload_template_id, enabled, workflow_steps)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (plan.name, json.dumps(plan.customer_ids or []), json.dumps(plan.bot_config or {}),
                   plan.execution_type, plan.target_count, plan.interval_minutes, plan.upload_template_id, 
                   1 if plan.enabled else 0, json.dumps(plan.workflow_steps or [])))
        conn.commit()
        plan_id = cursor.lastrowid
        conn.close()
        return plan_id
    
    def get_task_plans(self) -> List[TaskPlan]:
        """获取所有任务计划"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM task_plans ORDER BY created_at DESC')
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_task_plan(row) for row in rows]
    
    def get_task_plan(self, plan_id: int) -> Optional[TaskPlan]:
        """获取任务计划"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM task_plans WHERE id = ?', (plan_id,))
        row = cursor.fetchone()
        conn.close()
        return self._row_to_task_plan(row) if row else None
    
    def update_task_plan(self, plan_id: int, **kwargs):
        conn = self.get_conn()
        cursor = conn.cursor()
        fields = []
        values = []
        for key, value in kwargs.items():
            if value is not None:
                if key == 'customer_ids':
                    value = json.dumps(value)
                elif key == 'bot_config':
                    value = json.dumps(value)
                elif key == 'workflow_steps':
                    value = json.dumps(value)
                elif key == 'enabled':
                    value = 1 if value else 0
                fields.append(f'{key} = ?')
                values.append(value)
        if fields:
            cursor.execute(f'UPDATE task_plans SET {", ".join(fields)}, updated_at = ? WHERE id = ?', values + [datetime.now(), plan_id])
            conn.commit()
        conn.close()
    
    def delete_task_plan(self, plan_id: int):
        """删除任务计划"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM task_plans WHERE id = ?', (plan_id,))
        conn.commit()
        conn.close()
    
    def add_execution(self, execution: Execution) -> int:
        """添加执行记录"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('''
                INSERT INTO executions (plan_id, mode, status, start_time)
                VALUES (?, ?, ?, ?)
            ''', (execution.plan_id, execution.mode, execution.status, datetime.now()))
        conn.commit()
        execution_id = cursor.lastrowid
        conn.close()
        return execution_id
    
    def get_executions(self, plan_id: int = None, limit: int = 50) -> List[Execution]:
        """获取执行记录"""
        conn = self.get_conn()
        cursor = conn.cursor()
        if plan_id:
            cursor.execute('SELECT * FROM executions WHERE plan_id = ? ORDER BY start_time DESC LIMIT ?', (plan_id, limit))
        else:
            cursor.execute('SELECT * FROM executions ORDER BY start_time DESC LIMIT ?', (limit,))
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_execution(row) for row in rows]
    
    def get_execution(self, execution_id: int) -> Optional[Execution]:
        """获取单个执行记录"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM executions WHERE id = ?', (execution_id,))
        row = cursor.fetchone()
        conn.close()
        return self._row_to_execution(row) if row else None
    
    def update_execution(self, execution_id: int, **kwargs):
        conn = self.get_conn()
        cursor = conn.cursor()
        fields = []
        values = []
        for key, value in kwargs.items():
            if value is not None:
                fields.append(f'{key} = ?')
                values.append(value)
        if fields:
            cursor.execute(f'UPDATE executions SET {", ".join(fields)}, updated_at = ? WHERE id = ?', values + [datetime.now(), execution_id])
            conn.commit()
        conn.close()
    
    def add_upload_template(self, template: UploadTemplate) -> int:
        """添加上传模板"""
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO upload_templates (name, content)
                VALUES (?, ?)
            ''', (template.name, template.content))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None
        finally:
            conn.close()
    
    def get_upload_templates(self) -> List[UploadTemplate]:
        """获取所有上传模板"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM upload_templates ORDER BY created_at DESC')
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_upload_template(row) for row in rows]
    
    def get_upload_template(self, template_id: int) -> Optional[UploadTemplate]:
        """获取上传模板"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM upload_templates WHERE id = ?', (template_id,))
        row = cursor.fetchone()
        conn.close()
        return self._row_to_upload_template(row) if row else None
    
    def update_upload_template(self, template_id: int, **kwargs):
        conn = self.get_conn()
        cursor = conn.cursor()
        fields = []
        values = []
        for key, value in kwargs.items():
            if value is not None:
                fields.append(f'{key} = ?')
                values.append(value)
        if fields:
            cursor.execute(f'UPDATE upload_templates SET {", ".join(fields)}, updated_at = ? WHERE id = ?', values + [datetime.now(), template_id])
            conn.commit()
        conn.close()
    
    def delete_upload_template(self, template_id: int):
        """删除上传模板"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM upload_templates WHERE id = ?', (template_id,))
        conn.commit()
        conn.close()
    
    def get_task_stats(self) -> dict:
        """获取任务统计"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT status, COUNT(*) as count
            FROM tasks
            GROUP BY status
        ''')
        rows = cursor.fetchall()
        conn.close()
        return {row[0]: row[1] for row in rows}
    
    def _row_to_customer(self, row) -> Customer:
        """将数据库行转换为Customer对象 - 使用sqlite3.Row直接访问"""
        from datetime import datetime
        
        def to_datetime(value):
            if value:
                try:
                    return datetime.fromisoformat(value)
                except:
                    return None
            return None
        
        return Customer(
            id=row['id'],
            name=row['name'] if 'name' in row.keys() else '',
            user_id=row['user_id'] if 'user_id' in row.keys() else '',
            homepage_url=row['homepage_url'] if 'homepage_url' in row.keys() else '',
            created_at=to_datetime(row['created_at'] if 'created_at' in row.keys() else None),
            updated_at=to_datetime(row['updated_at'] if 'updated_at' in row.keys() else None)
        )
    
    def _row_to_task(self, row) -> Task:
        """将数据库行转换为Task对象 - 使用sqlite3.Row直接访问"""
        from datetime import datetime

        def to_datetime(value):
            if value:
                try:
                    return datetime.fromisoformat(value)
                except:
                    return None
            return None

        task = Task(
            id=row['id'],
            group_id=row['group_id'] if 'group_id' in row.keys() else None,
            customer_id=row['customer_id'] if 'customer_id' in row.keys() else None,
            video_id=row['video_id'] if 'video_id' in row.keys() else None,
            video_url=row['video_url'] if 'video_url' in row.keys() else '',
            video_publish_time=to_datetime(row['video_publish_time'] if 'video_publish_time' in row.keys() else None),
            download_url=row['download_url'] if 'download_url' in row.keys() else None,
            status=row['status'] if 'status' in row.keys() else 'pending',
            download_time=to_datetime(row['download_time'] if 'download_time' in row.keys() else None),
            watermark_time=to_datetime(row['watermark_time'] if 'watermark_time' in row.keys() else None),
            ai_caption_time=to_datetime(row['ai_caption_time'] if 'ai_caption_time' in row.keys() else None),
            upload_time=to_datetime(row['upload_time'] if 'upload_time' in row.keys() else None),
            upload_bot_id=row['upload_bot_id'] if 'upload_bot_id' in row.keys() else None,
            upload_channel_id=row['upload_channel_id'] if 'upload_channel_id' in row.keys() else None,
            file_path=row['file_path'] if 'file_path' in row.keys() else None,
            video_desc=row['video_desc'] if 'video_desc' in row.keys() else None,
            ai_caption=row['ai_caption'] if 'ai_caption' in row.keys() else None,
            error_msg=row['error_msg'] if 'error_msg' in row.keys() else None,
            created_at=to_datetime(row['created_at'] if 'created_at' in row.keys() else None),
            updated_at=to_datetime(row['updated_at'] if 'updated_at' in row.keys() else None)
        )

        return task
    
    def _row_to_task_plan(self, row) -> TaskPlan:
        """将数据库行转换为TaskPlan对象 - 使用sqlite3.Row直接访问"""
        from datetime import datetime
        
        def to_datetime(value):
            if value:
                try:
                    return datetime.fromisoformat(value)
                except:
                    return None
            return None
        
        try:
            customer_ids = json.loads(row['customer_ids'] if 'customer_ids' in row.keys() else '[]') if row['customer_ids'] else []
        except:
            customer_ids = []
        
        try:
            bot_config = json.loads(row['bot_config'] if 'bot_config' in row.keys() else '{}') if row['bot_config'] else {}
        except:
            bot_config = {}
        
        try:
            workflow_steps = json.loads(row['workflow_steps'] if 'workflow_steps' in row.keys() else '[]') if row['workflow_steps'] else []
        except:
            workflow_steps = []
        
        return TaskPlan(
            id=row['id'],
            name=row['name'] if 'name' in row.keys() else '',
            customer_ids=customer_ids,
            bot_config=bot_config,
            execution_type=row['execution_type'] if 'execution_type' in row.keys() else 'once',
            target_count=row['target_count'] if 'target_count' in row.keys() else None,
            interval_minutes=row['interval_minutes'] if 'interval_minutes' in row.keys() else 30,
            upload_template_id=row['upload_template_id'] if 'upload_template_id' in row.keys() else None,
            enabled=bool(row['enabled'] if 'enabled' in row.keys() else True),
            workflow_steps=workflow_steps,
            created_at=to_datetime(row['created_at'] if 'created_at' in row.keys() else None),
            updated_at=to_datetime(row['updated_at'] if 'updated_at' in row.keys() else None)
        )
    
    def _row_to_execution(self, row) -> Execution:
        """将数据库行转换为Execution对象 - 使用sqlite3.Row直接访问"""
        from datetime import datetime
        
        def to_datetime(value):
            if value:
                try:
                    return datetime.fromisoformat(value)
                except:
                    return None
            return None
        
        return Execution(
            id=row['id'],
            plan_id=row['plan_id'] if 'plan_id' in row.keys() else None,
            mode=row['mode'] if 'mode' in row.keys() else 'manual',
            status=row['status'] if 'status' in row.keys() else 'stopped',
            current_step=row['current_step'] if 'current_step' in row.keys() else None,
            start_time=to_datetime(row['start_time'] if 'start_time' in row.keys() else None),
            end_time=to_datetime(row['end_time'] if 'end_time' in row.keys() else None),
            cycle_count=row['cycle_count'] if 'cycle_count' in row.keys() else 0,
            cycle_duration=row['cycle_duration'] if 'cycle_duration' in row.keys() else 0,
            tasks_created=row['tasks_created'] if 'tasks_created' in row.keys() else 0,
            tasks_completed=row['tasks_completed'] if 'tasks_completed' in row.keys() else 0,
            tasks_failed=row['tasks_failed'] if 'tasks_failed' in row.keys() else 0,
            error_msg=row['error_msg'] if 'error_msg' in row.keys() else None,
            created_at=to_datetime(row['created_at'] if 'created_at' in row.keys() else None),
            updated_at=to_datetime(row['updated_at'] if 'updated_at' in row.keys() else None)
        )
    
    def _row_to_upload_template(self, row) -> UploadTemplate:
        """将数据库行转换为UploadTemplate对象 - 使用sqlite3.Row直接访问"""
        from datetime import datetime
        
        def to_datetime(value):
            if value:
                try:
                    return datetime.fromisoformat(value)
                except:
                    return None
            return None
        
        return UploadTemplate(
            id=row['id'],
            name=row['name'] if 'name' in row.keys() else '',
            content=row['content'] if 'content' in row.keys() else '',
            created_at=to_datetime(row['created_at'] if 'created_at' in row.keys() else None),
            updated_at=to_datetime(row['updated_at'] if 'updated_at' in row.keys() else None)
        )
    
    def add_monitor_group(self, group: MonitorGroup) -> int:
        """添加监控分组"""
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO monitor_groups (name, promotion_text, ai_caption_style)
                VALUES (?, ?, ?)
            ''', (group.name, group.promotion_text, group.ai_caption_style))
            conn.commit()
            new_id = cursor.lastrowid
            if new_id and group.id is None:
                group.id = new_id
            return new_id
        except sqlite3.IntegrityError:
            return None
        finally:
            conn.close()
    
    def get_monitor_groups(self) -> List[MonitorGroup]:
        """获取所有监控分组"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM monitor_groups ORDER BY id ASC')
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_monitor_group(row) for row in rows]
    
    def get_monitor_group(self, group_id: int) -> Optional[MonitorGroup]:
        """获取单个监控分组"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM monitor_groups WHERE id = ?', (group_id,))
        row = cursor.fetchone()
        conn.close()
        return self._row_to_monitor_group(row) if row else None
    
    def update_monitor_group(self, group_id: int, **kwargs):
        """更新监控分组"""
        conn = self.get_conn()
        cursor = conn.cursor()
        fields = []
        values = []
        for key, value in kwargs.items():
            if value is not None:
                fields.append(f'{key} = ?')
                values.append(value)
        if fields:
            cursor.execute(f'UPDATE monitor_groups SET {", ".join(fields)}, updated_at = ? WHERE id = ?', values + [datetime.now(), group_id])
            conn.commit()
        conn.close()
    
    def delete_monitor_group(self, group_id: int):
        """删除监控分组"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM monitor_groups WHERE id = ?', (group_id,))
        conn.commit()
        conn.close()
    
    def add_group_monitor(self, monitor: GroupMonitor) -> int:
        """添加分组监控UP主"""
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO group_monitors (group_id, up_name, up_url)
                VALUES (?, ?, ?)
            ''', (monitor.group_id, monitor.up_name, monitor.up_url))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None
        finally:
            conn.close()
    
    def get_group_monitors(self, group_id: int = None) -> List[GroupMonitor]:
        """获取分组监控UP主列表"""
        conn = self.get_conn()
        cursor = conn.cursor()
        if group_id:
            cursor.execute('SELECT * FROM group_monitors WHERE group_id = ? ORDER BY id ASC', (group_id,))
        else:
            cursor.execute('SELECT * FROM group_monitors ORDER BY id ASC')
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_group_monitor(row) for row in rows]
    
    def delete_group_monitor(self, monitor_id: int):
        """删除分组监控UP主"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM group_monitors WHERE id = ?', (monitor_id,))
        conn.commit()
        conn.close()
    
    def delete_group_monitor_by_url(self, group_id: int, up_url: str):
        """通过URL删除分组监控UP主"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM group_monitors WHERE group_id = ? AND up_url = ?', (group_id, up_url))
        conn.commit()
        conn.close()
    
    def add_group_target(self, target: GroupTarget) -> int:
        """添加分组目标频道"""
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO group_targets (group_id, target_channel, chat_id)
                VALUES (?, ?, ?)
            ''', (target.group_id, target.target_channel, target.chat_id))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None
        finally:
            conn.close()
    
    def get_group_targets(self, group_id: int = None) -> List[GroupTarget]:
        """获取分组目标频道列表"""
        conn = self.get_conn()
        cursor = conn.cursor()
        if group_id:
            cursor.execute('SELECT * FROM group_targets WHERE group_id = ? ORDER BY id ASC', (group_id,))
        else:
            cursor.execute('SELECT * FROM group_targets ORDER BY id ASC')
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_group_target(row) for row in rows]
    
    def delete_group_target(self, target_id: int):
        """删除分组目标频道"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM group_targets WHERE id = ?', (target_id,))
        conn.commit()
        conn.close()
    
    def delete_group_target_by_channel(self, group_id: int, target_channel: str):
        """通过频道名删除分组目标"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM group_targets WHERE group_id = ? AND target_channel = ?', (group_id, target_channel))
        conn.commit()
        conn.close()
    
    def update_group_target_chat_id(self, target_id: int, chat_id: int):
        """更新目标频道的chat_id"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('UPDATE group_targets SET chat_id = ?, updated_at = ? WHERE id = ?', (chat_id, datetime.now(), target_id))
        conn.commit()
        conn.close()
    
    def set_system_config(self, key: str, value: str):
        """设置系统配置"""
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO system_configs (config_key, config_value)
                VALUES (?, ?)
            ''', (key, value))
            conn.commit()
        finally:
            conn.close()
    
    def get_system_config(self, key: str, default: str = None) -> Optional[str]:
        """获取系统配置"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT config_value FROM system_configs WHERE config_key = ?', (key,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else default
    
    def _row_to_monitor_group(self, row) -> MonitorGroup:
        """将数据库行转换为MonitorGroup对象 - 使用sqlite3.Row直接访问"""
        from datetime import datetime
        
        def to_datetime(value):
            if value:
                try:
                    return datetime.fromisoformat(value)
                except:
                    return None
            return None
        
        return MonitorGroup(
            id=row['id'],
            name=row['name'] if 'name' in row.keys() else '',
            promotion_text=row['promotion_text'] if 'promotion_text' in row.keys() else None,
            ai_caption_style=row['ai_caption_style'] if 'ai_caption_style' in row.keys() else None,
            created_at=to_datetime(row['created_at'] if 'created_at' in row.keys() else None),
            updated_at=to_datetime(row['updated_at'] if 'updated_at' in row.keys() else None)
        )
    
    def _row_to_group_monitor(self, row) -> GroupMonitor:
        """将数据库行转换为GroupMonitor对象 - 使用sqlite3.Row直接访问"""
        from datetime import datetime
        
        def to_datetime(value):
            if value:
                try:
                    return datetime.fromisoformat(value)
                except:
                    return None
            return None
        
        return GroupMonitor(
            id=row['id'],
            group_id=row['group_id'] if 'group_id' in row.keys() else None,
            up_name=row['up_name'] if 'up_name' in row.keys() else '',
            up_url=row['up_url'] if 'up_url' in row.keys() else '',
            created_at=to_datetime(row['created_at'] if 'created_at' in row.keys() else None),
            updated_at=to_datetime(row['updated_at'] if 'updated_at' in row.keys() else None)
        )
    
    def _row_to_group_target(self, row) -> GroupTarget:
        """将数据库行转换为GroupTarget对象 - 使用sqlite3.Row直接访问"""
        from datetime import datetime
        
        def to_datetime(value):
            if value:
                try:
                    return datetime.fromisoformat(value)
                except:
                    return None
            return None
        
        return GroupTarget(
            id=row['id'],
            group_id=row['group_id'] if 'group_id' in row.keys() else None,
            target_channel=row['target_channel'] if 'target_channel' in row.keys() else '',
            chat_id=row['chat_id'] if 'chat_id' in row.keys() else None,
            created_at=to_datetime(row['created_at'] if 'created_at' in row.keys() else None),
            updated_at=to_datetime(row['updated_at'] if 'updated_at' in row.keys() else None)
        )
    
    def add_discovered_video(self, video: DiscoveredVideo) -> int:
        """添加已发现的视频"""
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO discovered_videos 
                (customer_id, video_id, video_url, video_publish_time, is_qualified)
                VALUES (?, ?, ?, ?, ?)
            ''', (video.customer_id, video.video_id, video.video_url, 
                  video.video_publish_time.isoformat() if video.video_publish_time else None,
                  1 if video.is_qualified else 0))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None
        finally:
            conn.close()
    
    def discovered_video_exists(self, video_id: str) -> bool:
        """检查视频是否已被发现过"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM discovered_videos WHERE video_id = ? LIMIT 1', (video_id,))
        exists = cursor.fetchone() is not None
        conn.close()
        return exists
    
    def get_discovered_videos(self, customer_id: int = None, is_qualified: bool = None, limit: int = 1000) -> List[DiscoveredVideo]:
        """获取已发现的视频列表"""
        conn = self.get_conn()
        cursor = conn.cursor()
        
        query = 'SELECT * FROM discovered_videos WHERE 1=1'
        params = []
        
        if customer_id is not None:
            query += ' AND customer_id = ?'
            params.append(customer_id)
        
        if is_qualified is not None:
            query += ' AND is_qualified = ?'
            params.append(1 if is_qualified else 0)
        
        query += ' ORDER BY discovered_at DESC LIMIT ?'
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_discovered_video(row) for row in rows]
    
    def update_discovered_video(self, video_id: str, **kwargs):
        """更新已发现的视频"""
        conn = self.get_conn()
        cursor = conn.cursor()
        fields = []
        values = []
        for key, value in kwargs.items():
            if key == 'is_qualified' and value is not None:
                fields.append(f'{key} = ?')
                values.append(1 if value else 0)
            elif value is not None:
                fields.append(f'{key} = ?')
                values.append(value)
        if fields:
            cursor.execute(f'UPDATE discovered_videos SET {", ".join(fields)}, updated_at = ? WHERE video_id = ?', 
                          values + [datetime.now(), video_id])
            conn.commit()
        conn.close()
    
    def _row_to_discovered_video(self, row) -> DiscoveredVideo:
        """将数据库行转换为DiscoveredVideo对象 - 使用sqlite3.Row直接访问"""
        from datetime import datetime
        
        def to_datetime(value):
            if value:
                try:
                    return datetime.fromisoformat(value)
                except:
                    return None
            return None
        
        return DiscoveredVideo(
            id=row['id'],
            customer_id=row['customer_id'] if 'customer_id' in row.keys() else None,
            video_id=row['video_id'] if 'video_id' in row.keys() else '',
            video_url=row['video_url'] if 'video_url' in row.keys() else '',
            video_publish_time=to_datetime(row['video_publish_time'] if 'video_publish_time' in row.keys() else None),
            discovered_at=to_datetime(row['discovered_at'] if 'discovered_at' in row.keys() else None),
            is_qualified=bool(row['is_qualified'] if 'is_qualified' in row.keys() else False),
            created_at=to_datetime(row['created_at'] if 'created_at' in row.keys() else None),
            updated_at=to_datetime(row['updated_at'] if 'updated_at' in row.keys() else None)
        )
