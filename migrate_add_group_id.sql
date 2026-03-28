-- 为 task_history 表添加 group_id 字段
-- 用于记录视频任务属于哪个分组

ALTER TABLE task_history ADD COLUMN group_id INTEGER;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_group_id ON task_history(group_id);
