-- 安全的数据库迁移脚本
-- 只有在列不存在时才添加

-- 为 task_history 表添加 group_id 字段（如果不存在）
-- 由于 SQLite 不支持 ALTER TABLE IF NOT EXISTS，我们需要用不同的方式处理
-- 这里我们直接尝试添加，如果已存在会忽略错误

-- 尝试添加 group_id 列（忽略错误）
ALTER TABLE task_history ADD COLUMN group_id INTEGER;

-- 尝试添加 group_id 索引（忽略错误）
CREATE INDEX IF NOT EXISTS idx_group_id ON task_history(group_id);

-- 尝试添加 promotion_text 列（忽略错误）
ALTER TABLE monitor_groups ADD COLUMN promotion_text TEXT;
