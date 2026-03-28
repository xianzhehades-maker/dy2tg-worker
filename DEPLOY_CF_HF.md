# dy2tg 部署指南 (Cloudflare + Hugging Face)

## 架构概览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Cloudflare Workers (指挥中心)                      │
│                                                                          │
│   1. 接收 Telegram Webhook                                              │
│   2. D1 查重 (video_id 是否存在)                                        │
│   3. 插入 pending 记录 (占位防重复)                                       │
│   4. 转发任务给 HF                                                      │
│   5. 接收 HF 回调 (R2 URL + caption)                                    │
│   6. 调用 Telegram API 发送视频                                          │
│   7. 更新 D1 状态为 completed                                            │
└─────────────────────────────────────────────────────────────────────────┘
                    ↑ 任务下发 (video_url, chat_id, task_id)
                    │                           ↓
                    │            ┌───────────────────────────────────────┐
                    │            │     Hugging Face FastAPI              │
                    │            │                                       │
                    │            │  1. 下载视频到 /tmp/                   │
                    │            │  2. FFmpeg 加工 (水印 + FastStart)  │
                    │            │  3. AI 文案生成 (Groq API)            │
                    │            │  4. boto3 上传到 R2                   │
                    │            │  5. 删除本地文件 (释放空间)             │
                    │            │  6. 回调 CF: R2 URL + caption         │
                    │            └───────────────────────────────────────┘
                    │                           ↓
                    │            ┌───────────────────────────────────────┐
                    │            │     Cloudflare R2                     │
                    │            │     (24小时后自动删除)                  │
                    │            └───────────────────────────────────────┘
                    │                           ↓ Telegram 拉取
                    └─────────────────────────────────────────────────────────┘
```

---

## 第一部分：Cloudflare 配置

### 1.1 创建 D1 数据库

```bash
# 安装 Wrangler CLI
npm install -g wrangler

# 登录 Cloudflare
wrangler login

# 创建 D1 数据库
wrangler d1 create dy2tg_db

# 初始化数据库 (会返回 database_id)
wrangler d1 execute dy2tg_db --file=init.sql
```

### 1.2 创建 R2 存储桶

1. 进入 Cloudflare Dashboard → R2
2. 创建桶，命名为 `video-assets`
3. 设置生命周期规则：Delete objects after 1 day
4. 设置公开访问：
   - 在 Settings → Access → 启用 "Allow public access"
   - 或设置 Custom Domain 如 `https://pub-your-id.r2.dev`

### 1.3 创建 R2 API Token

1. 进入 Cloudflare Dashboard → R2 → Manage R2 API Tokens
2. 创建 Token，权限选择 "Edit"
3. 记录 Access Key ID 和 Secret Access Key

### 1.4 获取 R2 Endpoint URL

R2 Endpoint URL 格式: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`
在 R2 Settings 里可以看到你的 Account ID。

### 1.5 部署 Workers

```bash
# 编辑 wrangler.toml，填入:
# - D1 database_id
# - R2 bucket info

# 部署
wrangler deploy
```

### 1.6 设置环境变量

在 Cloudflare Workers Dashboard → Settings → Variables 中设置:

| 变量 | 说明 | 示例 |
|------|------|------|
| `BOT_TOKEN` | Telegram Bot Token | `123456:ABC-...` |
| `HF_AUTH_TOKEN` | HF 认证令牌 | `hf_xxxx` |
| `CALLBACK_SECRET` | 回调鉴权密钥 | `your-secret-token` |
| `HF_API_URL` | HF Space URL | `https://your-space.hf.space` |

### 1.7 设置 Telegram Webhook

```bash
# 将 YOUR_DOMAIN 替换为你的 Workers 域名
curl -X POST "https://api.telegram.org/bot<BOT_TOKEN>/setWebhook" \
  -d "url=https://your-worker.your-username.workers.dev/webhook"
```

---

## 第二部分：Hugging Face 配置

### 2.1 创建 HF Space

1. 进入 https://huggingface.co/new-space
2. 选择 SDK: "Docker"
3. 创建一个新的 Space

### 2.2 配置 HF 环境变量

在 Space Settings → Variables 中设置:

| 变量 | 说明 | 示例 |
|------|------|------|
| `CALLBACK_URL` | CF Workers 回调地址 | `https://your-worker.workers.dev/callback` |
| `AUTH_TOKEN` | 与 CF 通信的认证令牌 | `your-secret-token` (同 CALLBACK_SECRET) |
| `R2_ENDPOINT_URL` | R2 Endpoint | `https://xxx.r2.cloudflarestorage.com` |
| `R2_ACCESS_KEY_ID` | R2 Access Key | `xxxx` |
| `R2_SECRET_ACCESS_KEY` | R2 Secret | `xxxx` |
| `R2_BUCKET_NAME` | R2 桶名 | `video-assets` |
| `R2_PUBLIC_URL` | R2 公开URL | `https://pub-xxx.r2.dev` |
| `GROQ_API_KEY` | AI 文案生成 | `gsk_xxxx` |

### 2.3 上传 HF Worker 代码

上传 `hf_worker.py` 和 `requirements_hf.txt` 到 HF Space。

---

## 第三部分：文件说明

| 文件 | 位置 | 说明 |
|------|------|------|
| `worker.js` | Cloudflare Workers | 指挥中心 |
| `wrangler.toml` | Cloudflare | Workers 配置 |
| `init.sql` | Cloudflare D1 | 数据库初始化 |
| `hf_worker.py` | Hugging Face | 计算引擎 |
| `requirements_hf.txt` | Hugging Face | Python 依赖 |

---

## 第四部分：测试流程

### 4.1 手动测试

```bash
# 1. 唤醒 HF Space
curl https://your-space.hf.space/wake

# 2. 发送测试任务
curl -X POST https://your-space.hf.space/enqueue \
  -H "Content-Type: application/json" \
  -H "X-Auth-Token: your-secret-token" \
  -d '{
    "video_url": "https://www.douyin.com/video/xxx",
    "chat_id": 123456,
    "task_id": "test-123",
    "generate_ai_caption": true
  }'
```

### 4.2 Telegram 测试

1. 向 Bot 发送 `/start`
2. 发送一个抖音视频链接
3. 等待处理完成，查看是否收到视频

---

## 第五部分：故障排查

| 问题 | 可能原因 | 解决方案 |
|------|---------|---------|
| Workers 收不到 Telegram 消息 | Webhook 未设置 | 检查 Telegram BotFather setWebhook |
| HF 收不到任务 | CALLBACK_URL 错误 | 检查 HF 环境变量 |
| R2 上传失败 | API Token 错误 | 检查 R2 配置 |
| Telegram 发送失败 | Bot Token 错误 | 检查 CF 环境变量 BOT_TOKEN |
| 视频无法播放 | R2 未设置公开访问 | 在 R2 Settings 启用 public access |

---

## 更新日志

| 日期 | 版本 | 说明 |
|------|------|------|
| 2026-03-23 | v1.0 | 初始版本 (CF + HF + R2 架构) |
