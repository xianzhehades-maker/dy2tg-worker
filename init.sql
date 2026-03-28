-- dy2tg D1 数据库初始化 SQL
-- 运行方式: wrangler d1 execute dy2tg_db --file=init.sql

-- 创建任务历史表
CREATE TABLE IF NOT EXISTS task_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL UNIQUE,
    source_url TEXT NOT NULL,
    chat_id INTEGER,
    group_id INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    r2_url TEXT,
    error_msg TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at DATETIME
);

-- 创建索引加速查重查询
CREATE UNIQUE INDEX IF NOT EXISTS idx_video_id ON task_history(video_id);
CREATE INDEX IF NOT EXISTS idx_status ON task_history(status);
CREATE INDEX IF NOT EXISTS idx_chat_id ON task_history(chat_id);
CREATE INDEX IF NOT EXISTS idx_group_id ON task_history(group_id);

-- 任务状态说明:
-- pending:    等待处理
-- processing: 处理中
-- completed:  已完成
-- failed:     失败
-- send_failed: 发送失败

-- 创建全局配置表
CREATE TABLE IF NOT EXISTS global_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL UNIQUE,
    value TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 创建监控分组表
CREATE TABLE IF NOT EXISTS monitor_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    chat_id INTEGER,
    check_interval INTEGER DEFAULT 3600,
    ai_caption_style TEXT DEFAULT 'default',
    ai_caption_language TEXT DEFAULT 'chinese',
    ai_caption_length INTEGER DEFAULT 200,
    target_channels TEXT,
    promotion_text TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 可选：UP主监控表
CREATE TABLE IF NOT EXISTS up_monitors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER NOT NULL,
    up_name TEXT NOT NULL,
    up_url TEXT NOT NULL,
    platform TEXT DEFAULT 'douyin',
    status TEXT DEFAULT 'active',
    last_checked_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES monitor_groups(id)
);

-- 可选：发现的视频记录
CREATE TABLE IF NOT EXISTS discovered_videos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT NOT NULL,
    video_id TEXT NOT NULL UNIQUE,
    discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
