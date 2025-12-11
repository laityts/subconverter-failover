// 工具函数库

// 默认常量（可通过环境变量覆盖）
export const DEFAULT_CACHE_TTL = 60 * 1000; // 健康状态缓存1分钟
export const DEFAULT_HEALTH_CHECK_TIMEOUT = 2000; // 健康检查超时2秒
export const DEFAULT_CONCURRENT_HEALTH_CHECKS = 5; // 并发健康检查数量
export const DEFAULT_FAST_CHECK_TIMEOUT = 800; // 快速检查超时800ms
export const DEFAULT_FAST_CHECK_CACHE_TTL = 2000; // 快速检查缓存2秒
export const DEFAULT_KV_WRITE_COOLDOWN = 30 * 1000; // KV写入冷却时间30秒
export const DEFAULT_HEALTHY_WEIGHT_INCREMENT = 10; // 健康状态权重增量
export const DEFAULT_FAILURE_WEIGHT_DECREMENT = 20; // 故障权重减量
export const DEFAULT_MAX_WEIGHT = 100; // 最大权重
export const DEFAULT_MIN_WEIGHT = 10; // 最小权重
export const DEFAULT_WEIGHT_RECOVERY_RATE = 2; // 每分钟权重恢复值
export const DEFAULT_BACKEND_STALE_THRESHOLD = 30 * 1000; // 后端信息过期阈值30秒
export const DEFAULT_LB_ALGORITHM = 'weighted_round_robin'; // 负载均衡算法
export const DEFAULT_ENABLE_STREAMING_PROXY = true; // 启用流式代理
export const DEFAULT_STREAMING_CHUNK_SIZE = 8192; // 流式分块大小
export const DEFAULT_WEIGHT_ADJUSTMENT_FACTOR = 0.3; // 权重调整平滑因子
export const DEFAULT_RESPONSE_TIME_WINDOW = 10; // 响应时间滑动窗口大小
export const DEFAULT_HEALTH_THRESHOLD = 0.7; // 健康阈值
export const DEFAULT_FAILURE_PENALTY = 15; // 失败惩罚
export const DEFAULT_SUCCESS_BOOST = 8; // 成功奖励
export const DEFAULT_BASE_WEIGHT = 50; // 初始权重

// Telegram通知相关常量
export const TG_API_URL = "https://api.telegram.org/bot";
export const DEFAULT_NOTIFY_ON_REQUEST = true;
export const DEFAULT_NOTIFY_ON_HEALTH_CHANGE = true;
export const DEFAULT_NOTIFY_ON_ERROR = true;

// 默认后端列表
export const DEFAULT_BACKENDS = [];

// ==================== 工具函数 ====================

// 生成唯一请求ID用于日志追踪
export function generateRequestId() {
  return Array.from(crypto.getRandomValues(new Uint8Array(4)))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}

// 统一配置读取函数（支持类型转换和验证）
export function getConfig(env, key, defaultValue) {
  if (!(key in env)) {
    return defaultValue;
  }
  
  const value = env[key];
  
  if (value === '') {
    return defaultValue;
  }
  
  if (typeof defaultValue === 'number') {
    const num = parseFloat(value);
    return isNaN(num) ? defaultValue : num;
  }
  
  if (typeof defaultValue === 'boolean') {
    return value === 'true' || value === '1' || value === 'yes';
  }
  
  if (typeof defaultValue === 'object') {
    try {
      return JSON.parse(value);
    } catch (error) {
      console.warn(`解析JSON配置${key}失败，使用默认值:`, error);
      return defaultValue;
    }
  }
  
  if (typeof defaultValue === 'string') {
    if (key === 'LB_ALGORITHM') {
      return value === 'weighted_round_robin' ? value : DEFAULT_LB_ALGORITHM;
    }
  }
  
  return value;
}

// 验证配置值的有效性
export function validateConfig(env, requestId) {
  const configs = [
    { key: 'CACHE_TTL', min: 1000, max: 300000, defaultValue: DEFAULT_CACHE_TTL },
    { key: 'HEALTH_CHECK_TIMEOUT', min: 100, max: 10000, defaultValue: DEFAULT_HEALTH_CHECK_TIMEOUT },
    { key: 'CONCURRENT_HEALTH_CHECKS', min: 1, max: 20, defaultValue: DEFAULT_CONCURRENT_HEALTH_CHECKS },
    { key: 'FAST_CHECK_TIMEOUT', min: 100, max: 5000, defaultValue: DEFAULT_FAST_CHECK_TIMEOUT },
    { key: 'FAST_CHECK_CACHE_TTL', min: 500, max: 30000, defaultValue: DEFAULT_FAST_CHECK_CACHE_TTL },
    { key: 'MAX_WEIGHT', min: 10, max: 1000, defaultValue: DEFAULT_MAX_WEIGHT },
    { key: 'MIN_WEIGHT', min: 1, max: 100, defaultValue: DEFAULT_MIN_WEIGHT },
    { key: 'WEIGHT_RECOVERY_RATE', min: 0, max: 100, defaultValue: DEFAULT_WEIGHT_RECOVERY_RATE },
    { key: 'FAILURE_WEIGHT_DECREMENT', min: 1, max: 100, defaultValue: DEFAULT_FAILURE_WEIGHT_DECREMENT },
    { key: 'BACKEND_STALE_THRESHOLD', min: 1000, max: 300000, defaultValue: DEFAULT_BACKEND_STALE_THRESHOLD },
    { key: 'STREAMING_CHUNK_SIZE', min: 1024, max: 65536, defaultValue: DEFAULT_STREAMING_CHUNK_SIZE },
    { key: 'WEIGHT_ADJUSTMENT_FACTOR', min: 0.01, max: 1, defaultValue: DEFAULT_WEIGHT_ADJUSTMENT_FACTOR },
    { key: 'RESPONSE_TIME_WINDOW', min: 3, max: 100, defaultValue: DEFAULT_RESPONSE_TIME_WINDOW },
    { key: 'HEALTH_THRESHOLD', min: 0.1, max: 1, defaultValue: DEFAULT_HEALTH_THRESHOLD },
    { key: 'FAILURE_PENALTY', min: 1, max: 100, defaultValue: DEFAULT_FAILURE_PENALTY },
    { key: 'SUCCESS_BOOST', min: 1, max: 100, defaultValue: DEFAULT_SUCCESS_BOOST },
    { key: 'BASE_WEIGHT', min: 1, max: 1000, defaultValue: DEFAULT_BASE_WEIGHT }
  ];
  
  const errors = [];
  
  for (const config of configs) {
    const value = getConfig(env, config.key, config.defaultValue);
    
    if (value < config.min || value > config.max) {
      errors.push({
        key: config.key,
        value: value,
        message: `值 ${value} 超出范围 (${config.min}-${config.max})`
      });
    }
  }
  
  const lbAlgorithm = getConfig(env, 'LB_ALGORITHM', DEFAULT_LB_ALGORITHM);
  if (lbAlgorithm !== 'weighted_round_robin') {
    errors.push({
      key: 'LB_ALGORITHM',
      value: lbAlgorithm,
      message: `无效的负载均衡算法: ${lbAlgorithm}，仅支持 weighted_round_robin`
    });
  }
  
  if (errors.length > 0 && requestId) {
    console.warn(`[${requestId}] 配置验证警告:`, errors);
  }
  
  return errors;
}

// 获取环境变量中的后端列表
export function getBackendsFromEnv(env) {
  try {
    if (env.BACKEND_URLS) {
      const backends = JSON.parse(env.BACKEND_URLS);
      
      if (Array.isArray(backends)) {
        return backends.filter(url => {
          try {
            new URL(url);
            return true;
          } catch {
            console.warn(`无效的后端URL: ${url}`);
            return false;
          }
        });
      }
    }
  } catch (error) {
    console.error('解析BACKEND_URLS失败:', error);
  }
  return DEFAULT_BACKENDS;
}

// 获取北京时间字符串（统一格式）
export function getBeijingTimeString(date = new Date()) {
  try {
    // 使用UTC+8的方式计算北京时间
    const beijingDate = new Date(date.getTime() + 8 * 60 * 60 * 1000);
    const year = beijingDate.getUTCFullYear();
    const month = String(beijingDate.getUTCMonth() + 1).padStart(2, '0');
    const day = String(beijingDate.getUTCDate()).padStart(2, '0');
    const hour = String(beijingDate.getUTCHours()).padStart(2, '0');
    const minute = String(beijingDate.getUTCMinutes()).padStart(2, '0');
    const second = String(beijingDate.getUTCSeconds()).padStart(2, '0');
    return `${year}-${month}-${day} ${hour}:${minute}:${second}`;
  } catch (error) {
    // 回退到ISO格式
    return date.toISOString().replace('T', ' ').substring(0, 19);
  }
}

// 获取北京日期字符串（YYYY-MM-DD格式）
export function getBeijingDateString(date = new Date()) {
  try {
    // 方法1：使用 Intl.DateTimeFormat 确保格式正确
    const formatter = new Intl.DateTimeFormat('zh-CN', {
      timeZone: 'Asia/Shanghai',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    });
    
    const formatted = formatter.format(date);
    // 格式可能是 "2025/01/20" 或 "2025-01-20"
    const cleanDate = formatted.replace(/\//g, '-');
    const parts = cleanDate.split('-');
    
    // 确保格式为 YYYY-MM-DD
    if (parts.length === 3) {
      const year = parts[0].padStart(4, '0');
      const month = parts[1].padStart(2, '0');
      const day = parts[2].padStart(2, '0');
      return `${year}-${month}-${day}`;
    }
    
    // 方法2：备用方案
    const beijingDate = new Date(date.getTime() + 8 * 60 * 60 * 1000);
    const year = beijingDate.getUTCFullYear();
    const month = String(beijingDate.getUTCMonth() + 1).padStart(2, '0');
    const day = String(beijingDate.getUTCDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  } catch (error) {
    // 方法3：最后备选
    const isoDate = date.toISOString();
    return isoDate.substring(0, 10); // "YYYY-MM-DD"
  }
}

// 获取北京时间短格式（HH:MM:SS）
export function getBeijingTimeShort(date = new Date()) {
  try {
    const beijingDate = new Date(date.getTime() + 8 * 60 * 60 * 1000);
    const hour = String(beijingDate.getUTCHours()).padStart(2, '0');
    const minute = String(beijingDate.getUTCMinutes()).padStart(2, '0');
    const second = String(beijingDate.getUTCSeconds()).padStart(2, '0');
    return `${hour}:${minute}:${second}`;
  } catch (error) {
    return date.toISOString().substring(11, 19);
  }
}

// 错误日志记录
export function logError(message, error, requestId) {
  const errorEntry = {
    timestamp: new Date().toISOString(),
    beijingTime: getBeijingTimeString(),
    requestId: requestId || 'unknown',
    message: message,
    error: error?.message || String(error),
    stack: error?.stack
  };
  
  console.error(`[${requestId || 'system'}] ${message}: ${error?.message || error}`);
}

// 平滑权重调整函数（指数加权移动平均）
export function smoothWeightAdjustment(currentWeight, targetWeight, adjustmentFactor) {
  // 使用指数加权移动平均进行平滑调整
  // 公式: newWeight = currentWeight + (targetWeight - currentWeight) * adjustmentFactor
  const adjustment = (targetWeight - currentWeight) * adjustmentFactor;
  const newWeight = currentWeight + adjustment;
  
  return newWeight;
}

// 计算响应时间得分（0-100分）
export function calculateResponseTimeScore(responseTime, env) {
  const MAX_RESPONSE_TIME = getConfig(env, 'HEALTH_CHECK_TIMEOUT', DEFAULT_HEALTH_CHECK_TIMEOUT);
  const IDEAL_RESPONSE_TIME = 100; // 理想响应时间100ms
  const PENALTY_THRESHOLD = 500; // 超过500ms开始扣分
  
  if (!responseTime || responseTime <= 0) {
    return 0;
  }
  
  if (responseTime <= IDEAL_RESPONSE_TIME) {
    return 100;
  }
  
  if (responseTime <= PENALTY_THRESHOLD) {
    // 100-500ms之间线性扣分
    const score = 100 - ((responseTime - IDEAL_RESPONSE_TIME) / (PENALTY_THRESHOLD - IDEAL_RESPONSE_TIME)) * 30;
    return Math.max(0, Math.round(score));
  }
  
  // 超过500ms指数扣分
  const excess = responseTime - PENALTY_THRESHOLD;
  const penalty = Math.min(70, excess / 10); // 每增加10ms扣1分，最多扣70分
  const score = 70 - penalty;
  return Math.max(0, Math.round(score));
}

// 获取状态表情符号
function getStatusEmoji(status, value) {
  // 根据状态和值返回对应的表情符号
  if (status === 'healthy') {
    if (value === true) return '🟢';
    if (value === false) return '🔴';
    return '⚪';
  }
  
  if (status === 'weight') {
    if (value >= 80) return '🏆';
    if (value >= 60) return '🟢';
    if (value >= 40) return '🟡';
    if (value >= 20) return '🟠';
    return '🔴';
  }
  
  if (status === 'response_time') {
    if (value < 300) return '⚡';
    if (value < 600) return '🏃‍♂️';
    if (value < 1000) return '🚶‍♂️';
    return '🐢';
  }
  
  if (status === 'status_code') {
    if (value >= 200 && value < 300) return '✅';
    if (value >= 300 && value < 400) return '🔄';
    if (value >= 400 && value < 500) return '⚠️';
    if (value >= 500) return '❌';
    return '❓';
  }
  
  return '🔘';
}

// 简单的HTML转义函数
function escapeHtmlSimple(text) {
  if (typeof text !== 'string') return text;
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

// 【美化】格式化Telegram消息
export function formatTelegramMessage(notificationData) {
  const beijingTime = getBeijingTimeString();
  const [date, time] = beijingTime.split(' ');
  
  // 通用头部
  let message = '';
  
  // 根据通知类型定制标题
  switch (notificationData.type) {
    case 'request':
      const success = notificationData.success;
      const titleEmoji = success ? '✅' : '❌';
      const statusEmoji = getStatusEmoji('healthy', success);
      
      message += `<b>${titleEmoji} 订阅请求通知</b>\n`;
      message += `<code>━━━━━━━━━━━━━━━━━━━</code>\n\n`;
      
      // 状态摘要
      message += `<b>📊 状态摘要</b>\n`;
      message += `${statusEmoji} 状态: <b>${success ? '成功' : '失败'}</b>\n`;
      
      // 请求信息
      message += `\n<b>📝 请求信息</b>\n`;
      message += `🆔 ID: <code>${notificationData.request_id}</code>\n`;
      message += `📍 IP: ${notificationData.client_ip || '未知'}\n`;
      message += `🕐 时间: ${date} ${time}\n`;
      
      // 后端信息
      if (notificationData.backend_url) {
        message += `\n<b>🔗 后端信息</b>\n`;
        // 简化URL显示
        try {
          const urlObj = new URL(notificationData.backend_url);
          const domain = urlObj.hostname;
          const shortDomain = domain.length > 20 ? domain.substring(0, 20) + '...' : domain;
          
          message += `🌐 域名: <code>${shortDomain}</code>\n`;
        } catch {
          message += `🌐 后端: <code>${notificationData.backend_url.substring(0, 30)}...</code>\n`;
        }
        
        if (notificationData.backend_weight) {
          const weightEmoji = getStatusEmoji('weight', notificationData.backend_weight);
          message += `${weightEmoji} 权重: <b>${notificationData.backend_weight}</b>\n`;
        }
        
        if (notificationData.backend_selection_time) {
          message += `⏱️ 选择耗时: ${notificationData.backend_selection_time}ms\n`;
        }
      }
      
      // 响应详情
      message += `\n<b>📈 响应详情</b>\n`;
      
      if (notificationData.status_code) {
        const statusEmoji = getStatusEmoji('status_code', notificationData.status_code);
        message += `${statusEmoji} 状态码: <b>${notificationData.status_code}</b>\n`;
      }
      
      if (notificationData.response_time) {
        const speedEmoji = getStatusEmoji('response_time', notificationData.response_time);
        message += `${speedEmoji} 响应时间: <b>${notificationData.response_time}ms</b>\n`;
      }
      
      if (notificationData.total_time) {
        message += `⏱️ 总耗时: ${notificationData.total_time}ms\n`;
      }
      
      // 错误信息（如果有）
      if (!success && notificationData.error) {
        const errorShort = notificationData.error.length > 100 
          ? notificationData.error.substring(0, 100) + '...' 
          : notificationData.error;
        message += `\n<b>⚠️ 错误信息</b>\n`;
        message += `<code>${escapeHtmlSimple(errorShort)}</code>\n`;
      }
      break;
      
    case 'health_change':
      const changeType = notificationData.change_type;
      let titleIcon = '🔄';
      if (changeType && changeType.includes('切换')) titleIcon = '🔄';
      if (changeType && changeType.includes('恢复')) titleIcon = '🆕';
      if (changeType && changeType.includes('不可用')) titleIcon = '⚠️';
      
      message += `<b>${titleIcon} 健康状态变化</b>\n`;
      message += `<code>━━━━━━━━━━━━━━━━━━━</code>\n\n`;
      
      // 变化概览
      message += `<b>📊 变化概览</b>\n`;
      message += `📅 日期: ${date}\n`;
      message += `🕐 时间: ${time}\n`;
      message += `🔄 类型: <b>${changeType || '状态变化'}</b>\n`;
      
      const healthyBackends = notificationData.healthy_backends || 0;
      const totalBackends = notificationData.total_backends || 0;
      const healthPercent = totalBackends > 0 
        ? Math.round((healthyBackends / totalBackends) * 100) 
        : 0;
      
      message += `💚 健康率: <b>${healthyBackends}/${totalBackends}</b> (${healthPercent}%)\n`;
      
      // 后端详细信息
      if (notificationData.current_backend || notificationData.previous_backend) {
        message += `\n<b>🔗 后端详情</b>\n`;
        
        if (notificationData.previous_backend && notificationData.current_backend) {
          try {
            const prevUrl = new URL(notificationData.previous_backend);
            const currUrl = new URL(notificationData.current_backend);
            message += `⬅️ 原后端: <code>${prevUrl.hostname}</code>\n`;
            message += `➡️ 新后端: <code>${currUrl.hostname}</code>\n`;
          } catch {
            message += `⬅️ 原后端: <code>${notificationData.previous_backend.substring(0, 30)}...</code>\n`;
            message += `➡️ 新后端: <code>${notificationData.current_backend.substring(0, 30)}...</code>\n`;
          }
        } else if (notificationData.current_backend) {
          try {
            const currUrl = new URL(notificationData.current_backend);
            message += `🎉 恢复后端: <code>${currUrl.hostname}</code>\n`;
          } catch {
            message += `🎉 恢复后端: <code>${notificationData.current_backend.substring(0, 30)}...</code>\n`;
          }
        } else if (notificationData.previous_backend) {
          try {
            const prevUrl = new URL(notificationData.previous_backend);
            message += `⚠️ 失效后端: <code>${prevUrl.hostname}</code>\n`;
          } catch {
            message += `⚠️ 失效后端: <code>${notificationData.previous_backend.substring(0, 30)}...</code>\n`;
          }
        }
        
        // 权重和响应时间信息
        if (notificationData.highest_weight_info) {
          const info = notificationData.highest_weight_info;
          const weightEmoji = getStatusEmoji('weight', info.weight || 0);
          const speedEmoji = getStatusEmoji('response_time', info.current_response_time || info.avg_response_time || 0);
          
          message += `${weightEmoji} 权重: <b>${info.weight || 0}</b>\n`;
          message += `${speedEmoji} 当前响应: <b>${info.current_response_time || 0}ms</b>\n`;
          message += `📊 平均响应: <b>${info.avg_response_time || 0}ms</b>\n`;
        }
      }
      
      // 权重排行榜（如果有）
      if (notificationData.weight_statistics && notificationData.weight_statistics.length > 0) {
        message += `\n<b>🏆 权重排行榜</b>\n`;
        message += `<code>────────────────</code>\n`;
        
        // 只显示前5个
        const topBackends = [...notificationData.weight_statistics]
          .sort((a, b) => (b.weight || 0) - (a.weight || 0))
          .slice(0, 5);
        
        topBackends.forEach((backend, index) => {
          const rankEmoji = index === 0 ? '🥇' : 
                           index === 1 ? '🥈' : 
                           index === 2 ? '🥉' : '•';
          const healthEmoji = backend.healthy ? '✅' : '❌';
          
          try {
            const urlObj = new URL(backend.url);
            const shortDomain = urlObj.hostname.length > 15 
              ? urlObj.hostname.substring(0, 12) + '...' 
              : urlObj.hostname;
            
            const weightEmoji = getStatusEmoji('weight', backend.weight || 0);
            
            message += `${rankEmoji} ${healthEmoji} <code>${shortDomain}</code>\n`;
            message += `    ${weightEmoji} ${backend.weight || 0} | ⏱️ ${backend.responseTime || 0}ms\n`;
          } catch {
            const shortUrl = backend.url.length > 20 
              ? backend.url.substring(0, 17) + '...' 
              : backend.url;
            const weightEmoji = getStatusEmoji('weight', backend.weight || 0);
            
            message += `${rankEmoji} ${healthEmoji} <code>${shortUrl}</code>\n`;
            message += `    ${weightEmoji} ${backend.weight || 0} | ⏱️ ${backend.responseTime || 0}ms\n`;
          }
        });
      }
      
      // 原因（如果有）
      if (notificationData.reason) {
        message += `\n<b>📝 变化原因</b>\n`;
        message += `${notificationData.reason}\n`;
      }
      break;
      
    case 'error':
      const errorType = notificationData.error_type || '未知错误';
      let errorIcon = '⚠️';
      if (errorType.includes('超时')) errorIcon = '⏱️';
      if (errorType.includes('连接')) errorIcon = '🔌';
      if (errorType.includes('网络')) errorIcon = '📡';
      
      message += `<b>${errorIcon} 系统错误</b>\n`;
      message += `<code>━━━━━━━━━━━━━━━━━━━</code>\n\n`;
      
      // 错误摘要
      message += `<b>📊 错误摘要</b>\n`;
      message += `📅 日期: ${date}\n`;
      message += `🕐 时间: ${time}\n`;
      message += `🆔 ID: <code>${notificationData.request_id}</code>\n`;
      message += `⚠️ 类型: ${errorType}\n`;
      
      // 错误详情
      if (notificationData.error_message) {
        const errorShort = notificationData.error_message.length > 120 
          ? notificationData.error_message.substring(0, 120) + '...' 
          : notificationData.error_message;
        message += `\n<b>📝 错误详情</b>\n`;
        message += `<code>${escapeHtmlSimple(errorShort)}</code>\n`;
      }
      
      // 上下文信息
      if (notificationData.backend_url || notificationData.client_ip) {
        message += `\n<b>📋 上下文信息</b>\n`;
        
        if (notificationData.client_ip) {
          message += `📍 IP: ${notificationData.client_ip}\n`;
        }
        
        if (notificationData.backend_url) {
          try {
            const urlObj = new URL(notificationData.backend_url);
            message += `🌐 后端: <code>${urlObj.hostname}</code>\n`;
          } catch {
            message += `🌐 后端: <code>${notificationData.backend_url.substring(0, 30)}...</code>\n`;
          }
        }
        
        if (notificationData.backend_weight) {
          const weightEmoji = getStatusEmoji('weight', notificationData.backend_weight);
          message += `${weightEmoji} 权重: ${notificationData.backend_weight}\n`;
        }
      }
      break;
      
    default:
      message += `<b>📢 系统通知</b>\n`;
      message += `<code>━━━━━━━━━━━━━━━━━━━</code>\n\n`;
      message += `📅 日期: ${date}\n`;
      message += `🕐 时间: ${time}\n`;
      message += `📋 内容:\n<code>${JSON.stringify(notificationData.data, null, 2).substring(0, 150)}</code>\n`;
  }
  
  // 添加页脚
  message += `\n<code>━━━━━━━━━━━━━━━━━━━</code>\n`;
  message += `<i>🚀 SubConverter智能负载均衡系统</i>\n`;
  message += `<i>⏰ 北京时间: ${time} | 📅 ${date}</i>`;
  
  return message;
}