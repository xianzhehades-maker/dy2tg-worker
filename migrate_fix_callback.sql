-- 修复回调功能的数据库迁移脚本
-- 为 task_history 表添加 group_id 字段
-- 为 monitor_groups 表添加 promotion_text 字段

-- 为 task_history 表添加 group_id 字段
PRAGMA table_info(task_history);
-- 如果 group_id 列不存在，添加它
ALTER TABLE task_history ADD COLUMN group_id INTEGER;

-- 为 task_history 表添加 group_id 索引
CREATE INDEX IF NOT EXISTS idx_group_id ON task_history(group_id);

-- 为 monitor_groups 表添加 promotion_text 字段
PRAGMA table_info(monitor_groups);
-- 如果 promotion_text 列不存在，添加它
ALTER TABLE monitor_groups ADD COLUMN promotion_text TEXT;
