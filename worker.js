async function sendToTelegram(botToken, chatId, text, parseMode = null) {
  const url = `https://api.telegram.org/bot${botToken}/sendMessage`;
  const payload = { chat_id: chatId, text };
  if (parseMode) {
    payload.parse_mode = parseMode;
  }
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const result = await response.json();
  console.log(`[Telegram sendMessage] chat_id=${chatId}, message_id=${result.result?.message_id}, ok=${result.ok}`);
  return result;
}

async function sendVideoToTelegram(botToken, chatId, videoUrl, caption = null) {
  const delay = Math.random() * 2000 + 500;
  await new Promise(r => setTimeout(r, delay));

  const url = `https://api.telegram.org/bot${botToken}/sendVideo`;
  const payload = {
    chat_id: chatId,
    video: videoUrl,
  };
  if (caption) {
    payload.caption = caption;
  }
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const result = await response.json();
  const messageId = result.result?.message_id;
  console.log(`[Telegram sendVideo] chat_id=${chatId}, message_id=${messageId}, ok=${result.ok}, error=${result.description || ''}`);
  return { ok: result.ok, messageId, error: result.description };
}

function extractVideoId(url) {
  const patterns = [
    /\/video\/([a-zA-Z0-9]+)/,
    /\/v\/([a-zA-Z0-9]+)/,
    /note\/([a-zA-Z0-9]+)/,
    /modal_id=(\d+)/,
  ];
  for (const pattern of patterns) {
    const match = url.match(pattern);
    if (match) return match[1];
  }
  return null;
}

function extractSecUserId(url) {
  const match = url.match(/\/user\/([A-Za-z0-9_-]+)/);
  if (match) return match[1];
  return null;
}

function isDouyinUrl(url) {
  return /douyin\.com|iesdouyin\.com/.test(url);
}

function parseCommand(text) {
  text = text.trim();
  if (text.startsWith('/')) {
    const parts = text.split(/\s+/);
    const cmd = parts[0].toLowerCase();
    const args = parts.slice(1);
    return { cmd, args };
  }
  return { cmd: null, args: [] };
}

function parseAddupCommand(text) {
  const match = text.match(/^\/addup\s+(\d+)\s+(.+?)\s*[:：]\s*(.+)$/);
  if (match) {
    return {
      groupId: parseInt(match[1]),
      upName: match[2].trim(),
      upUrl: match[3].trim()
    };
  }
  return null;
}

function parseTargetChannels(channelsStr) {
  if (!channelsStr) return [];
  try {
    return JSON.parse(channelsStr);
  } catch {
    return channelsStr.split(',').map(s => s.trim()).filter(s => s);
  }
}

function stringifyTargetChannels(channels) {
  return JSON.stringify(channels);
}

const HELP_TEXT = `🤖 Bot 命令帮助

【分组管理】
/add_group <名称> - 添加分组
/del_group <id> - 删除分组
/rename_group <id> <新名称> - 重命名分组

【监控管理】
/addup <group_id> 用户名: <url> - 添加UP主监控
  示例: /addup 2 小寒想吃草莓: https://www.douyin.com/user/xxx
/del_monitor <group_id> <url> - 按URL删除监控
/del_monitor <group_id> <index> - 按序号删除(见/status)
/mon_list - 查看所有监控

【目标管理】
/add_target <group_id> @channel - 添加目标频道
/del_target <group_id> <channel序号> - 删除目标频道
  示例: /del_target 2 2 (删除第二组的第二个频道)

【设置】
/set_promotion <group_id> <文案> - 设置推广文案
/set_caption_style <group_id> <风格> - 设置文案风格
  风格: default(口播), humor(乐子), none(无AI)
/set_caption_lan <group_id> <语言> - 设置文案语言
  语言: chinese(中文), bilingual(双语)
/set_caption_len <group_id> <字数> - 设置AI文案字数(0=禁用,50-500)
/generate_ai_caption <group_id> <True/False> - 开启/关闭AI文案生成

【视频】
直接发送抖音链接 - 添加视频任务(发送给当前对话)
/add_url <group_id> <url> - 添加视频任务到指定分组
  示例: /add_url 1 https://www.douyin.com/video/xxx
/add_url <url> - 添加视频任务(默认分组1)

【系统】
/setup - 初始化数据库(首次使用)
/status - 查看所有分组状态
/queue - 查看待处理队列
/list_errors - 查看失败任务
/retry_failed - 重试所有失败任务
/run_now - 立即执行流水线
/reset_task <video_id> - 重置单个任务
/reset_task all - 重置所有处理中的任务
/clear_tasks - 清空所有任务记录
/clean_r2 - 清理R2存储中的文件
   用法: /clean_r2 [list|purge|all|failed|<video_id>]
/test_target <group_id> - 测试目标频道发送
/sync_ids - 同步频道chat_id
/sync - 同步配置
/clear_cache - 清除缓存
/time <秒数> - 设置全局检查间隔
  示例: /time 3600 (3600秒后自动执行)
/help - 显示此帮助

【快速使用】
直接发送抖音视频链接，机器人会自动处理！

【批量指令】
支持一次发送多行命令，每行一个命令`;

async function getGlobalConfig(env, key) {
  const result = await env.BOT_DB.prepare(
    'SELECT value FROM global_config WHERE key = ?'
  ).bind(key).first();
  return result ? result.value : null;
}

async function setGlobalConfig(env, key, value) {
  await env.BOT_DB.prepare(
    'INSERT OR REPLACE INTO global_config (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)'
  ).bind(key, value).run();
}

async function handleSingleCommand(env, chatId, cmd, args, fullText, ctx = null) {
  if (cmd === '/setup') {
    try {
      await env.BOT_DB.exec(
        'CREATE TABLE IF NOT EXISTS monitor_groups (' +
        'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
        'name TEXT NOT NULL,' +
        'promotion_text TEXT,' +
        "ai_caption_style TEXT DEFAULT 'default'," +
        "ai_caption_language TEXT DEFAULT 'chinese'," +
        'ai_caption_length INTEGER DEFAULT 200,' +
        'target_channels TEXT,' +
        'chat_id INTEGER,' +
        'created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,' +
        'updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'
      );

      await env.BOT_DB.exec(
        'CREATE TABLE IF NOT EXISTS up_monitors (' +
        'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
        'group_id INTEGER NOT NULL,' +
        'up_name TEXT NOT NULL,' +
        'up_url TEXT NOT NULL,' +
        "platform TEXT DEFAULT 'douyin'," +
        "status TEXT DEFAULT 'active'," +
        'created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,' +
        'updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,' +
        'FOREIGN KEY (group_id) REFERENCES monitor_groups(id) ON DELETE CASCADE)'
      );

      await env.BOT_DB.exec(
        'CREATE TABLE IF NOT EXISTS task_history (' +
        'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
        'video_id TEXT NOT NULL,' +
        'source_url TEXT,' +
        'chat_id INTEGER,' +
        'group_id INTEGER,' +
        "status TEXT DEFAULT 'pending'," +
        'r2_url TEXT,' +
        'error_msg TEXT,' +
        'video_desc TEXT,' +
        'created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,' +
        'updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,' +
        'FOREIGN KEY (group_id) REFERENCES monitor_groups(id) ON DELETE SET NULL)'
      );

      await env.BOT_DB.exec(
        'CREATE TABLE IF NOT EXISTS global_config (' +
        'id INTEGER PRIMARY KEY AUTOINCREMENT,' +
        'key TEXT NOT NULL UNIQUE,' +
        'value TEXT,' +
        'updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'
      );

      await sendToTelegram(env.BOT_TOKEN, chatId, '✅ 数据库初始化完成');
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 初始化失败: ' + e.message);
    }
    return true;
  }

  if (cmd === '/start' || cmd === '/help') {
    await sendToTelegram(env.BOT_TOKEN, chatId, HELP_TEXT);
    return true;
  }

  if (cmd === '/add_group') {
    if (args.length === 0) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法: /add_group <名称>');
      return true;
    }
    const name = args.join(' ');
    try {
      const result = await env.BOT_DB.prepare(
        'INSERT INTO monitor_groups (name, chat_id, target_channels) VALUES (?, ?, ?)'
      ).bind(name, chatId, '[]').run();
      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 分组创建成功\n\nID: ${result.meta.last_row_id}\n名称: ${name}`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 创建分组失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/del_group') {
    if (args.length === 0) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法: /del_group <id>');
      return true;
    }
    const groupId = parseInt(args[0]);
    if (isNaN(groupId)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 分组ID必须是数字');
      return true;
    }
    try {
      await env.BOT_DB.prepare('DELETE FROM monitor_groups WHERE id = ?').bind(groupId).run();
      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 分组 ${groupId} 已删除`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 删除分组失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/rename_group') {
    if (args.length < 2) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法: /rename_group <id> <新名称>');
      return true;
    }
    const groupId = parseInt(args[0]);
    const newName = args.slice(1).join(' ');
    if (isNaN(groupId)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 分组ID必须是数字');
      return true;
    }
    try {
      const group = await env.BOT_DB.prepare('SELECT id FROM monitor_groups WHERE id = ?').bind(groupId).first();
      if (!group) {
        await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 分组 ${groupId} 不存在`);
        return true;
      }
      await env.BOT_DB.prepare(
        'UPDATE monitor_groups SET name = ? WHERE id = ?'
      ).bind(newName, groupId).run();
      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 分组已重命名\n\nID: ${groupId}\n新名称: ${newName}`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 重命名分组失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/addup') {
    const parsed = parseAddupCommand(fullText);
    if (!parsed || isNaN(parsed.groupId)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法: /addup <group_id> 用户名: <url>\n示例: /addup 2 小寒想吃草莓: https://www.douyin.com/user/xxx');
      return true;
    }
    const { groupId, upName, upUrl } = parsed;
    const platform = upUrl.includes('douyin.com') ? 'douyin' : 'tiktok';
    try {
      const group = await env.BOT_DB.prepare('SELECT id FROM monitor_groups WHERE id = ?').bind(groupId).first();
      if (!group) {
        await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 分组 ${groupId} 不存在`);
        return true;
      }
      const result = await env.BOT_DB.prepare(
        'INSERT INTO up_monitors (group_id, up_name, up_url, platform, status) VALUES (?, ?, ?, ?, ?)'
      ).bind(groupId, upName, upUrl, platform, 'active').run();
      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 监控添加成功\n\n分组: ${groupId}\nUP主: ${upName}\n平台: ${platform.toUpperCase()}\n链接: ${upUrl}`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 添加监控失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/mon_list') {
    try {
      const { results } = await env.BOT_DB.prepare('SELECT * FROM up_monitors').all();
      if (results.length === 0) {
        await sendToTelegram(env.BOT_TOKEN, chatId, '📋 暂无监控');
        return true;
      }
      const lines = ['📋 监控列表：\n'];
      results.forEach((m, i) => {
        lines.push(`${i + 1}. [${m.platform.toUpperCase()}] ${m.up_name} - ${m.up_url}`);
      });
      await sendToTelegram(env.BOT_TOKEN, chatId, lines.join('\n'));
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 获取监控列表失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/del_monitor') {
    if (args.length < 2) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法: /del_monitor <group_id> <url>');
      return true;
    }
    const groupId = parseInt(args[0]);
    const upUrl = args.slice(1).join(' ');
    if (isNaN(groupId)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 分组ID必须是数字');
      return true;
    }
    try {
      const { results } = await env.BOT_DB.prepare('SELECT * FROM up_monitors WHERE group_id = ?').bind(groupId).all();

      for (const m of results) {
        if (m.up_url === upUrl) {
          await env.BOT_DB.prepare('DELETE FROM up_monitors WHERE id = ?').bind(m.id).run();
          await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 监控已删除\n\n链接: ${upUrl}`);
          return true;
        }
      }

      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 未找到监控: ${upUrl}`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 删除监控失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/set_promotion') {
    if (args.length < 2) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法: /set_promotion <group_id> <文案>');
      return true;
    }
    const groupId = parseInt(args[0]);
    const text = args.slice(1).join(' ');
    if (isNaN(groupId)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 分组ID必须是数字');
      return true;
    }
    try {
      const group = await env.BOT_DB.prepare('SELECT id FROM monitor_groups WHERE id = ?').bind(groupId).first();
      if (!group) {
        await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 分组 ${groupId} 不存在`);
        return true;
      }
      await env.BOT_DB.prepare(
        'UPDATE monitor_groups SET promotion_text = ? WHERE id = ?'
      ).bind(text, groupId).run();
      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 推广文案已设置\n\n文案: ${text}`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 设置推广文案失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/add_target') {
    if (args.length < 2) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法: /add_target <group_id> @channel');
      return true;
    }
    const groupId = parseInt(args[0]);
    const channel = args.slice(1).join(' ');
    if (isNaN(groupId)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 分组ID必须是数字');
      return true;
    }
    try {
      const group = await env.BOT_DB.prepare('SELECT target_channels FROM monitor_groups WHERE id = ?').bind(groupId).first();
      if (!group) {
        await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 分组 ${groupId} 不存在`);
        return true;
      }
      const channels = parseTargetChannels(group.target_channels);
      channels.push(channel);
      await env.BOT_DB.prepare(
        'UPDATE monitor_groups SET target_channels = ? WHERE id = ?'
      ).bind(stringifyTargetChannels(channels), groupId).run();
      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 目标频道已添加\n\n分组: ${groupId}\n频道: ${channel}`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 添加目标频道失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/del_target') {
    if (args.length < 2) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法: /del_target <group_id> <channel序号>\n示例: /del_target 2 2 (删除第二组的第二个频道)');
      return true;
    }
    const groupId = parseInt(args[0]);
    const channelIndex = parseInt(args[1]) - 1;
    if (isNaN(groupId) || isNaN(channelIndex)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 分组ID和频道序号必须是数字');
      return true;
    }
    try {
      const group = await env.BOT_DB.prepare('SELECT target_channels FROM monitor_groups WHERE id = ?').bind(groupId).first();
      if (!group) {
        await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 分组 ${groupId} 不存在`);
        return true;
      }
      const channels = parseTargetChannels(group.target_channels);
      if (channelIndex < 0 || channelIndex >= channels.length) {
        await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 频道序号无效，当前有 ${channels.length} 个频道`);
        return true;
      }
      const removedChannel = channels.splice(channelIndex, 1)[0];
      await env.BOT_DB.prepare(
        'UPDATE monitor_groups SET target_channels = ? WHERE id = ?'
      ).bind(stringifyTargetChannels(channels), groupId).run();
      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 目标频道已删除\n\n分组: ${groupId}\n已删除: ${removedChannel}`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 删除目标频道失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/set_caption_style') {
    if (args.length < 2) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法: /set_caption_style <group_id> <风格>\n风格: default(口播), humor(乐子), none(无AI)');
      return true;
    }
    const groupId = parseInt(args[0]);
    const style = args[1].toLowerCase();
    if (isNaN(groupId)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 分组ID必须是数字');
      return true;
    }
    const validStyles = ['default', 'humor', 'none', '口播', '乐子', '无ai'];
    if (!validStyles.includes(style)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 不支持的风格\n可选: default(口播), humor(乐子), none(无AI)');
      return true;
    }
    const styleMap = { '口播': 'default', '乐子': 'humor', '无ai': 'none' };
    const finalStyle = styleMap[style] || style;
    try {
      await env.BOT_DB.prepare(
        'UPDATE monitor_groups SET ai_caption_style = ? WHERE id = ?'
      ).bind(finalStyle, groupId).run();
      if (finalStyle === 'none') {
        await env.BOT_DB.prepare(
          'UPDATE monitor_groups SET ai_caption_length = 0 WHERE id = ?'
        ).bind(groupId).run();
      }
      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 文案风格已设置\n\n分组: ${groupId}\n风格: ${finalStyle}`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 设置文案风格失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/set_caption_lan') {
    if (args.length < 2) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法: /set_caption_lan <group_id> <语言>\n语言: chinese(中文), bilingual(双语)');
      return true;
    }
    const groupId = parseInt(args[0]);
    const language = args[1].toLowerCase();
    if (isNaN(groupId)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 分组ID必须是数字');
      return true;
    }
    const validLanguages = ['chinese', 'bilingual', '中文', '双语'];
    if (!validLanguages.includes(language)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 不支持的语言\n可选: chinese(中文), bilingual(双语)');
      return true;
    }
    const languageMap = { '中文': 'chinese', '双语': 'bilingual' };
    const finalLanguage = languageMap[language] || language;
    try {
      await env.BOT_DB.prepare(
        'UPDATE monitor_groups SET ai_caption_language = ? WHERE id = ?'
      ).bind(finalLanguage, groupId).run();
      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 文案语言已设置\n\n分组: ${groupId}\n语言: ${finalLanguage}`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 设置文案语言失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/set_caption_len') {
    if (args.length < 2) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法: /set_caption_len <group_id> <字数(0=禁用,50-500)>');
      return true;
    }
    const groupId = parseInt(args[0]);
    const length = parseInt(args[1]);
    if (isNaN(groupId) || isNaN(length)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 分组ID和字数必须是数字');
      return true;
    }
    if (length !== 0 && (length < 50 || length > 500)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 字数必须是0(禁用)或50-500之间');
      return true;
    }
    try {
      await env.BOT_DB.prepare(
        'UPDATE monitor_groups SET ai_caption_length = ? WHERE id = ?'
      ).bind(length, groupId).run();
      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ AI文案字数已设置\n\n分组: ${groupId}\n字数: ${length}`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 设置字数失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/generate_ai_caption') {
    if (args.length < 2) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法: /generate_ai_caption <group_id> <True/False>');
      return true;
    }
    const groupId = parseInt(args[0]);
    const enabled = args[1].toLowerCase() === 'true';
    if (isNaN(groupId)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 分组ID必须是数字');
      return true;
    }
    try {
      await env.BOT_DB.prepare(
        'UPDATE monitor_groups SET generate_ai_caption = ? WHERE id = ?'
      ).bind(enabled ? 'true' : 'false', groupId).run();
      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ AI文案生成已${enabled ? '开启' : '关闭'}\n\n分组: ${groupId}`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 设置失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/time') {
    if (args.length === 0) {
      const currentInterval = await getGlobalConfig(env, 'check_interval');
      await sendToTelegram(env.BOT_TOKEN, chatId, `📊 当前全局检查间隔: ${currentInterval || '3600'} 秒\n\n用法: /time <秒数> - 设置全局检查间隔`);
      return true;
    }
    const interval = parseInt(args[0]);
    if (isNaN(interval) || interval < 60) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 秒数必须是数字且不小于60\n示例: /time 3600');
      return true;
    }
    try {
      await setGlobalConfig(env, 'check_interval', interval.toString());
      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 全局检查间隔已设置为 ${interval} 秒`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 设置检查间隔失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/status') {
    try {
      const { count: totalPending } = await env.BOT_DB.prepare("SELECT COUNT(*) as count FROM task_history WHERE status = 'pending'").first() || { count: 0 };
      const { count: totalCompleted } = await env.BOT_DB.prepare("SELECT COUNT(*) as count FROM task_history WHERE status IN ('completed', 'uploaded')").first() || { count: 0 };
      const { count: totalFailed } = await env.BOT_DB.prepare("SELECT COUNT(*) as count FROM task_history WHERE status IN ('failed', 'send_failed')").first() || { count: 0 };
      const { count: totalTasks } = await env.BOT_DB.prepare("SELECT COUNT(*) as count FROM task_history").first() || { count: 0 };
      const { count: totalGroups } = await env.BOT_DB.prepare("SELECT COUNT(*) as count FROM monitor_groups").first() || { count: 0 };
      const { count: totalMonitors } = await env.BOT_DB.prepare("SELECT COUNT(*) as count FROM up_monitors").first() || { count: 0 };

      const globalInterval = await getGlobalConfig(env, 'check_interval');

      const lines = [
        '📊 系统状态\n',
        '【全局统计】',
        `📦 总分组数: ${totalGroups}`,
        `👁️  总监控UP主: ${totalMonitors}`,
        `🎬  总任务: ${totalTasks}`,
        `   ⏳ 待处理: ${totalPending}`,
        `   ✅ 已完成: ${totalCompleted}`,
        `   ❌ 失败: ${totalFailed}`,
        `⏱️  全局检查间隔: ${globalInterval || '3600'} 秒`,
        '',
        '查看详细: /group_list'
      ];

      await sendToTelegram(env.BOT_TOKEN, chatId, lines.join('\n'));
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 获取状态失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/queue') {
    try {
      const { results: tasks } = await env.BOT_DB.prepare('SELECT * FROM task_history WHERE status = ? LIMIT 50').bind('pending').all();
      if (tasks.length === 0) {
        await sendToTelegram(env.BOT_TOKEN, chatId, '📋 队列为空');
        return true;
      }
      const lines = [`📋 待处理任务 (${tasks.length}个)：\n`];
      tasks.slice(0, 10).forEach((t, i) => {
        lines.push(`${i + 1}. ${t.source_url}`);
      });
      if (tasks.length > 10) {
        lines.push(`... 还有 ${tasks.length - 10} 个`);
      }
      await sendToTelegram(env.BOT_TOKEN, chatId, lines.join('\n'));
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 获取队列失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/run_now') {
    console.log('/run_now: 开始执行，触发 GitHub Actions 轮询');
    const notifPromise = (async () => {
      try {
        if (!env.GH_REPO || !env.GH_PAT) {
          await sendToTelegram(env.BOT_TOKEN, chatId, '❌ GH_REPO 或 GH_PAT 未配置');
          return;
        }

        const [owner, repo] = env.GH_REPO.split('/');
        const ghResponse = await fetch(
          `https://api.github.com/repos/${owner}/${repo}/dispatches`,
          {
            method: 'POST',
            headers: {
              'Authorization': `Bearer ${env.GH_PAT}`,
              'Accept': 'application/vnd.github+json',
              'X-GitHub-Api-Version': '2022-11-28',
              'Content-Type': 'application/json',
              'User-Agent': 'dy2tg-bot',
            },
            body: JSON.stringify({
              event_type: 'poll-and-process',
              client_payload: {},
            }),
          }
        );

        if (ghResponse.status === 204 || ghResponse.status === 200) {
          console.log('/run_now: GitHub Actions 轮询触发成功');
          await sendToTelegram(env.BOT_TOKEN, chatId, '✅ 已触发 GitHub Actions 轮询UP...\n完成后会自动处理并推送视频');
        } else {
          console.error('/run_now: GitHub API 失败:', ghResponse.status);
          await sendToTelegram(env.BOT_TOKEN, chatId, `❌ GitHub API 失败: ${ghResponse.status}`);
        }
      } catch (e) {
        console.error('/run_now 执行失败:', e);
        await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 触发失败: ' + e.message);
      }
    })();

    if (ctx) {
      ctx.waitUntil(notifPromise);
    }
    return true;
  }

  if (cmd === '/sync') {
    await sendToTelegram(env.BOT_TOKEN, chatId, '✅ 配置已同步');
    return true;
  }

  if (cmd === '/clear_cache') {
    await sendToTelegram(env.BOT_TOKEN, chatId, '✅ 缓存已清除');
    return true;
  }

  if (cmd === '/list_errors') {
    try {
      const { results: failedTasks } = await env.BOT_DB.prepare(
        "SELECT video_id, source_url, error_msg, created_at FROM task_history " +
        "WHERE status = ? OR status = ? OR status = ? ORDER BY created_at DESC LIMIT 20"
      ).bind('failed', 'send_failed', 'error').all();

      if (!failedTasks || failedTasks.length === 0) {
        await sendToTelegram(env.BOT_TOKEN, chatId, '✅ 没有失败的任务');
        return true;
      }

      let msg = `❌ 失败任务 (${failedTasks.length}个):\n\n`;
      for (let i = 0; i < failedTasks.length; i++) {
        const t = failedTasks[i];
        msg += `${i + 1}. ${t.video_id}\n   链接: ${(t.source_url || '').substring(0, 60)}\n`;
        if (t.error_msg) msg += `   错误: ${t.error_msg.substring(0, 50)}\n`;
        msg += '\n';
      }
      msg += '使用 /retry_failed 重试所有失败任务';

      await sendToTelegram(env.BOT_TOKEN, chatId, msg);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 查询失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/clear_tasks') {
    try {
      const { results: tasks } = await env.BOT_DB.prepare(
        'SELECT COUNT(*) as cnt FROM task_history'
      ).all();

      if (tasks.length === 0 || tasks[0].cnt === 0) {
        await sendToTelegram(env.BOT_TOKEN, chatId, '📋 任务表已经是空的');
        return true;
      }

      await env.BOT_DB.prepare('DELETE FROM task_history').run();
      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 已删除所有任务记录`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 删除失败: ' + e.message);
    }
    return true;
  }

  if (cmd === '/reset_task') {
    if (args.length === 0) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法:\n/reset_task <video_id> - 重置单个任务\n/reset_task all - 重置所有处理中的任务');
      return true;
    }

    try {
      if (args[0] === 'all') {
        const result = await env.BOT_DB.prepare(
          'UPDATE task_history SET status = ? WHERE status = ?'
        ).bind('pending', 'processing').run();

        await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 已重置 ${result.meta.changes} 个处理中的任务`);
      } else {
        const videoId = args[0];
        const result = await env.BOT_DB.prepare(
          'UPDATE task_history SET status = ? WHERE video_id = ?'
        ).bind('pending', videoId).run();

        if (result.meta.changes > 0) {
          await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 任务已重置: ${videoId}`);
        } else {
          await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 未找到任务: ${videoId}`);
        }
      }
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 重置任务失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/clean_r2') {
    try {
      const mode = args[0] || 'failed';

      if (!['all', 'failed', 'list', 'purge'].includes(mode) && !mode.match(/^\d+$/)) {
        await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法:\n/clean_r2 list - 列出R2中的所有文件\n/clean_r2 purge - 清空整个R2存储桶\n/clean_r2 failed - 清理失败任务的R2文件\n/clean_r2 all - 清理所有任务的R2文件\n/clean_r2 <video_id> - 清理指定视频的R2文件');
        return true;
      }

      if (!env.VIDEO_BUCKET) {
        await sendToTelegram(env.BOT_TOKEN, chatId, '❌ R2 存储桶未绑定，请检查 wrangler.toml 配置');
        return true;
      }

      if (mode === 'list') {
        try {
          const listed = await env.VIDEO_BUCKET.list();
          let fileList = '📁 R2 存储桶中的文件:\n\n';
          if (listed.objects && listed.objects.length > 0) {
            listed.objects.forEach((obj, i) => {
              fileList += `${i + 1}. ${obj.key} (${Math.round(obj.size / 1024)} KB)\n`;
            });
            fileList += `\n总计: ${listed.objects.length} 个文件`;
          } else {
            fileList += '存储桶为空';
          }
          await sendToTelegram(env.BOT_TOKEN, chatId, fileList);
          return true;
        } catch (listError) {
          console.error('列出 R2 文件失败:', listError);
          await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 列出文件失败: ${listError.message}`);
          return true;
        }
      }

      if (mode === 'purge') {
        try {
          await sendToTelegram(env.BOT_TOKEN, chatId, '⚠️ 正在清空整个 R2 存储桶...');

          const listed = await env.VIDEO_BUCKET.list();
          if (!listed.objects || listed.objects.length === 0) {
            await sendToTelegram(env.BOT_TOKEN, chatId, '📋 存储桶已经是空的了');
            return true;
          }

          let deletedCount = 0;
          let failedCount = 0;

          for (const obj of listed.objects) {
            try {
              await env.VIDEO_BUCKET.delete(obj.key);
              console.log(`已删除: ${obj.key}`);
              deletedCount++;
            } catch (delError) {
              console.error(`删除失败 ${obj.key}:`, delError);
              failedCount++;
            }
          }

          let message = `✅ 清空完成\n\n`;
          message += `已删除: ${deletedCount} 个文件\n`;
          if (failedCount > 0) {
            message += `失败: ${failedCount} 个`;
          }

          await sendToTelegram(env.BOT_TOKEN, chatId, message);
          return true;
        } catch (purgeError) {
          console.error('清空 R2 存储桶失败:', purgeError);
          await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 清空失败: ${purgeError.message}`);
          return true;
        }
      }

      let tasksToClean = [];
      let query;

      if (mode === 'all') {
        query = 'SELECT video_id, r2_url FROM task_history WHERE r2_url IS NOT NULL';
      } else if (mode === 'failed') {
        query = 'SELECT video_id, r2_url FROM task_history WHERE r2_url IS NOT NULL AND (status = ? OR status = ?)';
      } else {
        query = 'SELECT video_id, r2_url FROM task_history WHERE r2_url IS NOT NULL AND video_id = ?';
      }

      let stmt;
      if (mode === 'all') {
        stmt = env.BOT_DB.prepare(query);
      } else if (mode === 'failed') {
        stmt = env.BOT_DB.prepare(query).bind('failed', 'error');
      } else {
        stmt = env.BOT_DB.prepare(query).bind(mode);
      }

      const { results } = await stmt.all();
      tasksToClean = results || [];

      if (tasksToClean.length === 0) {
        await sendToTelegram(env.BOT_TOKEN, chatId, '📋 没有需要清理的R2文件（数据库中没有记录）\n使用 /clean_r2 list 查看R2存储桶中的实际文件');
        return true;
      }

      await sendToTelegram(env.BOT_TOKEN, chatId, `🔄 准备清理 ${tasksToClean.length} 个任务的R2文件...`);

      let deletedCount = 0;
      let failedCount = 0;
      let skippedCount = 0;

      for (const task of tasksToClean) {
        if (!task.r2_url) continue;

        try {
          let objectKey = `${task.video_id}_out.mp4`;

          try {
            const url = new URL(task.r2_url);
            const pathParts = url.pathname.split('/').filter(p => p);
            if (pathParts.length > 0) {
              objectKey = pathParts[pathParts.length - 1];
            }
          } catch (e) {
          }

          let fileExists = false;
          try {
            const head = await env.VIDEO_BUCKET.head(objectKey);
            if (head) {
              fileExists = true;
            }
          } catch (headError) {
          }

          if (fileExists) {
            try {
              await env.VIDEO_BUCKET.delete(objectKey);
              console.log(`已删除 R2 文件: ${objectKey}`);
              deletedCount++;
            } catch (r2Error) {
              console.error(`删除 R2 文件失败 ${objectKey}:`, r2Error);
              failedCount++;
            }
          } else {
            console.log(`R2 文件不存在: ${objectKey}`);
            skippedCount++;
          }

          await env.BOT_DB.prepare(
            'UPDATE task_history SET r2_url = NULL WHERE video_id = ?'
          ).bind(task.video_id).run();
        } catch (e) {
          console.error(`清理失败 ${task.video_id}:`, e);
          failedCount++;
        }
      }

      let message = `✅ 清理完成\n\n`;
      message += `已删除: ${deletedCount} 个文件\n`;
      message += `跳过(不存在): ${skippedCount} 个\n`;
      if (failedCount > 0) {
        message += `失败: ${failedCount} 个`;
      }

      await sendToTelegram(env.BOT_TOKEN, chatId, message);
      return true;
    } catch (e) {
      console.error('清理R2文件失败:', e);
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 清理失败: ${e.message}`);
      return true;
    }
  }

  if (cmd === '/retry_failed') {
    try {
      const { results: failedTasks } = await env.BOT_DB.prepare(
        'SELECT * FROM task_history WHERE status = ? OR status = ? LIMIT 50'
      ).bind('failed', 'send_failed').all();

      if (failedTasks.length === 0) {
        await sendToTelegram(env.BOT_TOKEN, chatId, '📋 没有失败的任务');
        return true;
      }

      await sendToTelegram(env.BOT_TOKEN, chatId, `🔄 正在重试 ${failedTasks.length} 个失败任务...`);

      let retried = 0;
      for (const task of failedTasks) {
        try {
          const existing = await env.BOT_DB.prepare(
            'SELECT status, r2_url FROM task_history WHERE video_id = ?'
          ).bind(task.video_id).first();

          if (existing && existing.status === 'completed' && existing.r2_url) {
            continue;
          }

          await env.BOT_DB.prepare(
            'UPDATE task_history SET status = ?, error_msg = NULL WHERE id = ?'
          ).bind('pending', task.id).run();

          let success = false;

          if (env.GH_REPO && env.GH_PAT) {
            const [owner, repo] = env.GH_REPO.split('/');
            const ghDispatch = await fetch(
              `https://api.github.com/repos/${owner}/${repo}/dispatches`,
              {
                method: 'POST',
                headers: {
                  'Accept': 'application/vnd.github+json',
                  'Authorization': `Bearer ${env.GH_PAT}`,
                  'X-GitHub-Api-Version': '2022-11-28',
                  'Content-Type': 'application/json',
                  'User-Agent': 'dy2tg-bot',
                },
                body: JSON.stringify({
                  event_type: 'video-process',
                  client_payload: {
                    video_url: task.source_url,
                    task_id: task.video_id,
                    chat_id: task.chat_id,
                    video_desc: task.video_desc || '',
                  }
                }),
              }
            );
            if (ghDispatch.status === 204 || ghDispatch.status === 200) {
              success = true;
            }
          }

          if (success) {
            await env.BOT_DB.prepare(
              'UPDATE task_history SET status = ? WHERE id = ?'
            ).bind('processing', task.id).run();
            retried++;
          }
        } catch (err) {
          console.error('重试任务失败:', err);
        }
      }

      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 已重试 ${retried}/${failedTasks.length} 个任务`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 重试失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/sync_ids') {
    try {
      const { results: groups } = await env.BOT_DB.prepare('SELECT * FROM monitor_groups').all();
      let updated = 0;

      for (const group of groups) {
        const channels = parseTargetChannels(group.target_channels);
        if (channels.length === 0) continue;

        const resolvedChannels = [];
        for (const ch of channels) {
          const channelUsername = ch.replace('@', '');
          try {
            const botInfo = await fetch(
              `https://api.telegram.org/bot${env.BOT_TOKEN}/getChat/@${channelUsername}`
            );
            if (botInfo.ok) {
              const chatInfo = await botInfo.json();
              if (chatInfo.ok) {
                resolvedChannels.push(`@${chatInfo.result.username || channelUsername}`);
                updated++;
              } else {
                resolvedChannels.push(ch);
              }
            } else {
              resolvedChannels.push(ch);
            }
          } catch {
            resolvedChannels.push(ch);
          }
        }

        await env.BOT_DB.prepare(
          'UPDATE monitor_groups SET target_channels = ? WHERE id = ?'
        ).bind(stringifyTargetChannels(resolvedChannels), group.id).run();
      }

      await sendToTelegram(env.BOT_TOKEN, chatId, `✅ 同步完成，更新了 ${updated} 个频道信息`);
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 同步失败: ${e.message}`);
    }
    return true;
  }

  if (cmd === '/test_target') {
    if (args.length === 0) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法: /test_target <group_id>');
      return true;
    }

    const groupId = parseInt(args[0]);
    if (isNaN(groupId)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 分组ID必须是数字');
      return true;
    }

    try {
      const group = await env.BOT_DB.prepare(
        'SELECT target_channels FROM monitor_groups WHERE id = ?'
      ).bind(groupId).first();

      if (!group) {
        await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 分组 ${groupId} 不存在`);
        return true;
      }

      const channels = parseTargetChannels(group.target_channels);
      if (channels.length === 0) {
        await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 分组 ${groupId} 未配置目标频道`);
        return true;
      }

      await sendToTelegram(env.BOT_TOKEN, chatId, `🧪 正在测试 ${channels.length} 个目标频道...`);

      const testChannels = async () => {
        let successCount = 0;
        console.log(`开始测试 ${channels.length} 个频道`);
        for (const channel of channels) {
          const channelId = channel.replace('@', '');
          console.log(`正在发送测试消息到频道: ${channelId}`);
          try {
            const testMsg = await sendToTelegram(
              env.BOT_TOKEN,
              channelId,
              '✅ 测试消息：机器人可以向此频道发送消息'
            );
            console.log(`频道 ${channelId} 发送结果:`, testMsg ? '成功' : '失败');
            if (testMsg && testMsg.ok) {
              successCount++;
            }
          } catch (e) {
            console.error(`测试频道 ${channelId} 失败:`, e);
          }
        }
        await sendToTelegram(
          env.BOT_TOKEN,
          chatId,
          `📊 测试完成：${successCount}/${channels.length} 个频道可发送消息`
        );
      };

      ctx.waitUntil(testChannels());
      return true;
    } catch (e) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 测试失败: ${e.message}`);
      return true;
    }
  }

  if (cmd === '/add_url') {
    if (args.length === 0) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法:\n/add_url <group_id> <url> - 添加到指定分组\n/add_url <url> - 添加到默认分组1\n\n示例:\n/add_url 1 https://www.douyin.com/video/xxx\n/add_url https://www.douyin.com/video/xxx');
      return true;
    }

    let groupId = 1;
    let videoUrl = args[0];

    if (!isNaN(parseInt(args[0]))) {
      if (args.length < 2) {
        await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 用法: /add_url <group_id> <url>');
        return true;
      }
      groupId = parseInt(args[0]);
      videoUrl = args.slice(1).join(' ');
    } else {
      videoUrl = args.join(' ');
    }

    const group = await env.BOT_DB.prepare('SELECT id, target_channels FROM monitor_groups WHERE id = ?').bind(groupId).first();
    if (!group) {
      await sendToTelegram(env.BOT_TOKEN, chatId, `❌ 分组 ${groupId} 不存在`);
      return true;
    }

    if (!isDouyinUrl(videoUrl)) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 请提供有效的抖音视频链接');
      return true;
    }

    const videoId = extractVideoId(videoUrl);
    if (!videoId) {
      await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 无法提取视频ID，请检查链接格式');
      return true;
    }

    try {
      const existing = await env.BOT_DB.prepare(
        'SELECT status, r2_url FROM task_history WHERE video_id = ?'
      ).bind(videoId).first();

      if (existing) {
        if (existing.status === 'completed' && existing.r2_url) {
          await sendToTelegram(env.BOT_TOKEN, chatId, '♻️ 这个视频之前处理过，正在发送...');
          const channels = parseTargetChannels(group.target_channels);
          if (channels.length > 0) {
            for (const channel of channels) {
              const channelId = channel.replace('@', '');
              await sendVideoToTelegram(env.BOT_TOKEN, channelId, existing.r2_url, '来自历史记录');
            }
          } else {
            await sendVideoToTelegram(env.BOT_TOKEN, chatId, existing.r2_url, '来自历史记录');
          }
          return true;
        } else {
          await env.BOT_DB.prepare(
            'DELETE FROM task_history WHERE video_id = ?'
          ).bind(videoId).run();
          await sendToTelegram(env.BOT_TOKEN, chatId, '🔄 删除旧记录，重新处理视频...');
        }
      }

      await sendToTelegram(env.BOT_TOKEN, chatId, `🎬 收到视频链接，开始处理...\n分组: ${groupId}`);

      await env.BOT_DB.prepare(
        'INSERT INTO task_history (video_id, source_url, chat_id, group_id, status) VALUES (?, ?, ?, ?, ?)'
      ).bind(videoId, videoUrl, chatId, groupId, 'pending').run();

      let success = false;

      if (env.GH_REPO && env.GH_PAT) {
        const [owner, repo] = env.GH_REPO.split('/');
        const ghDispatch = await fetch(
          `https://api.github.com/repos/${owner}/${repo}/dispatches`,
          {
            method: 'POST',
            headers: {
              'Authorization': `Bearer ${env.GH_PAT}`,
              'Accept': 'application/vnd.github+json',
              'X-GitHub-Api-Version': '2022-11-28',
              'Content-Type': 'application/json',
              'User-Agent': 'dy2tg-bot',
            },
            body: JSON.stringify({
              event_type: 'video-process',
              client_payload: {
                video_url: videoUrl,
                task_id: videoId,
                chat_id: chatId,
                video_desc: '',
              },
            }),
          }
        );

        if (ghDispatch.status === 204 || ghDispatch.status === 200) {
          console.log('GitHub Actions dispatch 成功');
          success = true;
        } else {
          console.error('GitHub dispatch 失败:', ghDispatch.status);
        }
      }

      if (success) {
        await env.BOT_DB.prepare(
          'UPDATE task_history SET status = ? WHERE video_id = ?'
        ).bind('processing', videoId).run();
        await sendToTelegram(env.BOT_TOKEN, chatId, '✅ 任务已提交，正在处理中...\n预计需要1-5分钟，请稍候');
      } else {
        await sendToTelegram(env.BOT_TOKEN, chatId, '⚠️ 处理出错: GitHub Actions 不可用\n请检查配置');
      }

    } catch (error) {
      console.error('处理失败:', error);
      await sendToTelegram(env.BOT_TOKEN, chatId, `⚠️ 处理出错: ${error.message}\n请稍后重试`);
    }
    return true;
  }

  if (cmd) {
    await sendToTelegram(env.BOT_TOKEN, chatId, `❓ 未知命令: ${cmd}\n发送 /help 查看可用命令`);
    return true;
  }

  return false;
}

async function verifyApiToken(request, env) {
  const authHeader = request.headers.get('X-Auth-Token');
  return authHeader === env.CALLBACK_SECRET;
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === '/health') {
      return Response.json({
        status: 'ok',
        version: '2026-03-28-1',
        has_bot_token: !!env.BOT_TOKEN,
        has_gh_repo: !!env.GH_REPO,
        has_callback_secret: !!env.CALLBACK_SECRET,
        has_db: !!env.BOT_DB,
      });
    }

    if (url.pathname === '/debug/path') {
      return Response.json({ path: url.pathname, method: request.method });
    }

    if (url.pathname === '/api/monitors' && request.method === 'GET') {
      const { results: monitors } = await env.BOT_DB.prepare('SELECT * FROM up_monitors').all();
      const { results: groups } = await env.BOT_DB.prepare('SELECT * FROM monitor_groups').all();
      return Response.json({ success: true, monitors, groups });
    }

    if (url.pathname === '/api/task_history' && request.method === 'GET') {
      try {
        const { results: tasks } = await env.BOT_DB.prepare(
          'SELECT video_id, status FROM task_history ORDER BY created_at DESC'
        ).all();
        return Response.json({ success: true, tasks });
      } catch (e) {
        return Response.json({ success: false, error: e.message }, { status: 500 });
      }
    }

    if (url.pathname === '/api/task_history' && request.method === 'POST') {
      try {
        const data = await request.json();
        const { video_id, source_url, chat_id, group_id } = data;

        if (!video_id) {
          return Response.json({ success: false, error: 'Missing video_id' }, { status: 400 });
        }

        const existing = await env.BOT_DB.prepare(
          'SELECT id, status FROM task_history WHERE video_id = ?'
        ).bind(video_id).first();

        if (existing) {
          return Response.json({ success: true, message: 'Video already exists', status: existing.status });
        }

        await env.BOT_DB.prepare(
          'INSERT INTO task_history (video_id, source_url, chat_id, group_id, status) VALUES (?, ?, ?, ?, ?)'
        ).bind(video_id, source_url || '', chat_id || 0, group_id || 1, 'pending').run();

        return Response.json({ success: true, video_id });
      } catch (e) {
        return Response.json({ success: false, error: e.message }, { status: 500 });
      }
    }

    if (url.pathname === '/webhook' && request.method === 'POST') {
      try {
        const data = await request.json();
        console.log('收到消息:', JSON.stringify(data));

        if (data.channel_post) {
          console.log('忽略频道回显消息');
          return new Response('OK', { status: 200 });
        }

        if (data.message && data.message.text) {
          const chatId = data.message.chat.id;
          const fullText = data.message.text.trim();

          const lines = fullText.split('\n').map(line => line.trim()).filter(line => line);

          let hasCommand = false;

          for (const lineText of lines) {
            const { cmd, args } = parseCommand(lineText);

            if (cmd) {
              hasCommand = true;
              const handled = await handleSingleCommand(env, chatId, cmd, args, lineText, ctx);
              if (handled) {
                continue;
              }
            } else if (isDouyinUrl(lineText)) {
              hasCommand = true;
              const videoId = extractVideoId(lineText);
              const secUserId = extractSecUserId(lineText);

              if (!videoId && !secUserId) {
                await sendToTelegram(env.BOT_TOKEN, chatId, '❌ 无法提取视频ID或用户ID，请检查链接格式');
                continue;
              }

              const taskId = videoId || secUserId;
              console.log('处理抖音链接, videoId:', videoId, 'secUserId:', secUserId, 'taskId:', taskId);

              const defaultGroupId = 1;
              const group = await env.BOT_DB.prepare(
                'SELECT id, target_channels FROM monitor_groups WHERE id = ?'
              ).bind(defaultGroupId).first();

              try {
                const existing = await env.BOT_DB.prepare(
                  'SELECT status, r2_url FROM task_history WHERE video_id = ?'
                ).bind(taskId).first();

                if (existing) {
                  if (existing.status === 'completed' && existing.r2_url) {
                    await sendToTelegram(env.BOT_TOKEN, chatId, '♻️ 这个视频之前处理过，正在发送...');
                    if (group) {
                      const channels = parseTargetChannels(group.target_channels);
                      if (channels.length > 0) {
                        for (const channel of channels) {
                          const channelId = channel.replace('@', '');
                          await sendVideoToTelegram(env.BOT_TOKEN, channelId, existing.r2_url, '来自历史记录');
                        }
                      } else {
                        await sendVideoToTelegram(env.BOT_TOKEN, chatId, existing.r2_url, '来自历史记录');
                      }
                    } else {
                      await sendVideoToTelegram(env.BOT_TOKEN, chatId, existing.r2_url, '来自历史记录');
                    }
                    continue;
                  } else {
                    await env.BOT_DB.prepare(
                      'DELETE FROM task_history WHERE video_id = ?'
                    ).bind(taskId).run();
                    await sendToTelegram(env.BOT_TOKEN, chatId, '🔄 删除旧记录，重新处理视频...');
                  }
                }

                await sendToTelegram(env.BOT_TOKEN, chatId, `🎬 收到视频链接，开始处理...\n分组: ${defaultGroupId}`);

                await env.BOT_DB.prepare(
                  'INSERT INTO task_history (video_id, source_url, chat_id, group_id, status) VALUES (?, ?, ?, ?, ?)'
                ).bind(taskId, lineText, chatId, defaultGroupId, 'pending').run();

                let success = false;

                if (env.GH_REPO && env.GH_PAT) {
                  const [owner, repo] = env.GH_REPO.split('/');
                  const ghDispatch = await fetch(
                    `https://api.github.com/repos/${owner}/${repo}/dispatches`,
                    {
                      method: 'POST',
                      headers: {
                        'Authorization': `Bearer ${env.GH_PAT}`,
                        'Accept': 'application/vnd.github+json',
                        'X-GitHub-Api-Version': '2022-11-28',
                        'Content-Type': 'application/json',
                      },
                      body: JSON.stringify({
                        event_type: 'video-process',
                        client_payload: {
                          video_url: lineText,
                          task_id: taskId,
                          chat_id: chatId,
                          video_desc: '',
                        }
                      }),
                    }
                  );

                  if (ghDispatch.status === 204 || ghDispatch.status === 200) {
                    console.log('GitHub Actions dispatch 成功');
                    success = true;
                  } else {
                    console.error('GitHub dispatch 失败:', ghDispatch.status);
                  }
                }

                if (success) {
                  await env.BOT_DB.prepare(
                    'UPDATE task_history SET status = ? WHERE video_id = ?'
                  ).bind('processing', taskId).run();
                  await sendToTelegram(env.BOT_TOKEN, chatId, '✅ 任务已提交，正在处理中...\n预计需要1-5分钟，请稍候');
                } else {
                  throw new Error('处理服务不可用');
                }

              } catch (error) {
                console.error('处理失败:', error);
                await sendToTelegram(env.BOT_TOKEN, chatId, `⚠️ 处理出错: ${error.message}\n请稍后重试`);
              }
              continue;
            }
          }

          if (!hasCommand) {
            await sendToTelegram(env.BOT_TOKEN, chatId, '❓ 请发送抖音视频链接，或使用 /help 查看帮助');
          }
        }

        return new Response('OK', { status: 200 });
      } catch (e) {
        console.error('Webhook 处理错误:', e);
        return new Response('Error: ' + e.message, { status: 500 });
      }
    }

    if (url.pathname === '/callback' && request.method === 'POST') {
      try {
        const authHeader = request.headers.get('X-Auth-Token');
        if (env.CALLBACK_SECRET && authHeader !== env.CALLBACK_SECRET) {
          return new Response('Unauthorized', { status: 401 });
        }

        const data = await request.json();
        console.log('收到回调:', JSON.stringify(data));

        const { task_id, chat_id, download_url, caption, success, error, video_desc, group_id, source_url } = data;

        const processCallback = async () => {
          if (success && download_url) {
            try {
              let finalCaption = video_desc || '视频处理完成';
              if (!finalCaption || /^\d+$/.test(finalCaption.trim())) {
                finalCaption = '#热辣舞蹈';
              } else if (caption && caption.trim().length > 0 && caption.trim() !== video_desc.trim()) {
                finalCaption = `${finalCaption}\n\n---\n\n${caption}`;
              }

              let firstMessageId = null;
              let targetChannels = [];

              if (group_id) {
                const group = await env.BOT_DB.prepare(
                  'SELECT target_channels FROM monitor_groups WHERE id = ?'
                ).bind(group_id).first();
                if (group) {
                  targetChannels = parseTargetChannels(group.target_channels);
                }
              }

              if (targetChannels.length > 0) {
                for (const channel of targetChannels) {
                  const channelId = channel.replace('@', '');
                  const result = await sendVideoToTelegram(env.BOT_TOKEN, channelId, download_url, finalCaption);
                  if (firstMessageId === null && result.messageId) {
                    firstMessageId = result.messageId;
                  }
                }
              } else {
                const result = await sendVideoToTelegram(env.BOT_TOKEN, chat_id, download_url, finalCaption);
                firstMessageId = result.messageId;
              }

              await env.BOT_DB.prepare(
                'INSERT OR REPLACE INTO task_history (video_id, source_url, r2_url, telegram_message_id, status, completed_at) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)'
              ).bind(task_id, source_url || '', download_url, firstMessageId, 'completed').run();

            } catch (sendError) {
              console.error('发送视频失败，不写入记录:', sendError.message);
            }
          }
        };

        ctx.waitUntil(processCallback());
        return new Response('OK', { status: 200 });
      } catch (e) {
        console.error('回调处理错误:', e);
        return new Response('Error: ' + e.message, { status: 500 });
      }
    }

    return new Response('Not found', { status: 404 });
  },

  async scheduled(controller, env) {
    console.log('Cron triggered, triggering GitHub Actions poll-and-process...');

    if (!env.GH_REPO || !env.GH_PAT) {
      console.error('GH_REPO 或 GH_PAT 未配置，跳过');
      return;
    }

    try {
      const [owner, repo] = env.GH_REPO.split('/');
      const ghResponse = await fetch(
        `https://api.github.com/repos/${owner}/${repo}/dispatches`,
        {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${env.GH_PAT}`,
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28',
            'Content-Type': 'application/json',
            'User-Agent': 'dy2tg-bot',
          },
          body: JSON.stringify({
            event_type: 'poll-and-process',
            client_payload: {},
          }),
        }
      );

      if (ghResponse.status === 204 || ghResponse.status === 200) {
        console.log('GitHub Actions poll-and-process 触发成功');
      } else {
        console.error('GitHub API 失败:', ghResponse.status);
      }
    } catch (e) {
      console.error('Cron job error:', e);
    }
  },
};
