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

// 【美化】格式化Telegram消息（美化版）
export function formatTelegramMessage(notificationData) {
  const beijingTime = getBeijingTimeString();
  
  let message = '';
  
  switch (notificationData.type) {
    case 'request':
      const success = notificationData.success;
      const emoji = success ? '✅' : '❌';
      
      message = `<b>${emoji} 📡 订阅转换请求通知</b>\n`;
      message += `<i>━━━━━━━━━━━━━━━━━━━</i>\n\n`;
      
      // 基本信息
      message += `<b>📊 基本信息</b>\n`;
      message += `<b>🆔 请求ID:</b> <code>${notificationData.request_id}</code>\n`;
      message += `<b>📍 客户端IP:</b> ${notificationData.client_ip || '未知'}\n`;
      message += `<b>🕐 时间:</b> ${beijingTime}\n\n`;
      
      // 后端信息
      message += `<b>🔗 后端信息</b>\n`;
      message += `<b>后端地址:</b> <code>${notificationData.backend_url || '未知'}</code>\n`;
      if (notificationData.backend_weight) {
        const weightEmoji = notificationData.backend_weight >= 70 ? '🟢' : 
                          notificationData.backend_weight >= 40 ? '🟡' : '🔴';
        message += `<b>后端权重:</b> ${weightEmoji} ${notificationData.backend_weight}\n`;
      }
      if (notificationData.backend_selection_time) {
        message += `<b>选择耗时:</b> ${notificationData.backend_selection_time}ms\n`;
      }
      message += `<b>负载算法:</b> ${notificationData.algorithm || '智能加权轮询'}\n\n`;
      
      // 响应信息
      message += `<b>⚡ 响应信息</b>\n`;
      message += `<b>状态:</b> ${success ? '<b>🟢 成功</b>' : '<b>🔴 失败</b>'}\n`;
      if (notificationData.status_code) {
        const statusEmoji = notificationData.status_code >= 200 && notificationData.status_code < 300 ? '🟢' : 
                          notificationData.status_code >= 300 && notificationData.status_code < 400 ? '🟡' : '🔴';
        message += `<b>状态码:</b> ${statusEmoji} ${notificationData.status_code}\n`;
      }
      message += `<b>响应时间:</b> ${notificationData.response_time || 0}ms\n`;
      if (notificationData.total_time) {
        message += `<b>总耗时:</b> ${notificationData.total_time}ms\n`;
      }
      
      if (!success && notificationData.error) {
        message += `\n<b>❌ 错误信息:</b>\n<code>${notificationData.error.substring(0, 100)}</code>\n`;
      }
      break;
      
    case 'health_change':
      message = `<b>🔄 🌡️ 后端健康状态变化</b>\n`;
      message += `<i>━━━━━━━━━━━━━━━━━━━</i>\n\n`;
      
      message += `<b>📊 变化概况</b>\n`;
      message += `<b>🕐 时间:</b> ${beijingTime}\n`;
      message += `<b>🔄 变化类型:</b> ${notificationData.change_type}\n`;
      message += `<b>💚 健康后端:</b> ${notificationData.healthy_backends}/${notificationData.total_backends}\n`;
      message += `<b>⚡ 最快响应:</b> ${notificationData.response_time || 0}ms\n\n`;
      
      // 只有在后端切换时才显示原后端
      if (notificationData.change_type === '后端切换' && notificationData.previous_backend) {
        message += `<b>⬅️ 原后端:</b>\n<code>${notificationData.previous_backend}</code>\n`;
      }
      
      if (notificationData.current_backend) {
        message += `<b>➡️ 现后端:</b> <code>${notificationData.current_backend}</code>\n\n`;
      } else if (notificationData.healthy_backends === 0) {
        message += `<b>⚠️ 当前后端:</b> <i>无可用后端</i>\n\n`;
      }
      
      // 显示原因
      if (notificationData.reason) {
        message += `<b>📝 变化原因:</b> ${notificationData.reason}\n\n`;
      }
      
      // 显示权重信息
      if (notificationData.weight_statistics && notificationData.weight_statistics.length > 0) {
        message += `<b>⚖️ 权重变化统计</b>\n`;
        message += `<i>─────────────────</i>\n`;
        
        // 按权重排序
        const sortedStats = [...notificationData.weight_statistics]
          .sort((a, b) => b.weight - a.weight)
          .slice(0, 8); // 只显示前8个
        
        sortedStats.forEach((stat, index) => {
          const rankEmoji = index === 0 ? '🥇' : 
                           index === 1 ? '🥈' : 
                           index === 2 ? '🥉' : '•';
          const healthEmoji = stat.healthy ? '✅' : '❌';
          const truncatedUrl = stat.url.length > 25 ? stat.url.substring(0, 22) + '...' : stat.url;
          
          // 权重颜色标记
          let weightText = `${stat.weight}`;
          if (stat.weight >= 70) {
            weightText = `<b>${stat.weight}</b> 🟢`;
          } else if (stat.weight >= 40) {
            weightText = `${stat.weight} 🟡`;
          } else {
            weightText = `${stat.weight} 🔴`;
          }
          
          // 响应时间标记
          let responseTimeText = '';
          if (stat.responseTime) {
            if (stat.responseTime < 300) {
              responseTimeText = ` ⚡${stat.responseTime}ms`;
            } else if (stat.responseTime < 800) {
              responseTimeText = ` ⏱️${stat.responseTime}ms`;
            } else {
              responseTimeText = ` 🐌${stat.responseTime}ms`;
            }
          }
          
          message += `${rankEmoji} ${healthEmoji} <code>${truncatedUrl}</code>\n`;
          message += `   权重: ${weightText}${responseTimeText}\n`;
          
          // 每3个后添加一个空行
          if ((index + 1) % 3 === 0) {
            message += '\n';
          }
        });
        
        // 显示总结
        const avgWeight = Math.round(sortedStats.reduce((sum, stat) => sum + stat.weight, 0) / sortedStats.length);
        const healthyCount = sortedStats.filter(stat => stat.healthy).length;
        const unhealthyCount = sortedStats.length - healthyCount;
        
        message += `\n<b>📈 统计总结</b>\n`;
        message += `平均权重: ${avgWeight} | 健康: ${healthyCount}个 | 异常: ${unhealthyCount}个\n`;
      }
      break;
      
    case 'error':
      message = `<b>🚨 ⚠️ 系统错误通知</b>\n`;
      message += `<i>━━━━━━━━━━━━━━━━━━━</i>\n\n`;
      
      message += `<b>📊 错误信息</b>\n`;
      message += `<b>🕐 时间:</b> ${beijingTime}\n`;
      message += `<b>🆔 请求ID:</b> <code>${notificationData.request_id}</code>\n`;
      message += `<b>❌ 错误类型:</b> ${notificationData.error_type}\n`;
      message += `<b>📝 错误详情:</b>\n<code>${notificationData.error_message?.substring(0, 150) || '无错误信息'}</code>\n\n`;
      
      if (notificationData.backend_url) {
        message += `<b>🔗 相关后端:</b> <code>${notificationData.backend_url}</code>\n`;
      }
      if (notificationData.client_ip) {
        message += `<b>📍 客户端IP:</b> ${notificationData.client_ip}\n`;
      }
      if (notificationData.backend_weight) {
        message += `<b>⚖️ 后端权重:</b> ${notificationData.backend_weight}\n`;
      }
      break;
      
    default:
      message = `<b>📢 🔔 系统通知</b>\n`;
      message += `<i>━━━━━━━━━━━━━━━━━━━</i>\n\n`;
      message += `<b>🕐 时间:</b> ${beijingTime}\n`;
      message += `<b>📋 内容:</b>\n<code>${JSON.stringify(notificationData.data, null, 2).substring(0, 200)}</code>\n`;
  }
  
  // 添加分隔线和时间戳
  message += `\n<i>━━━━━━━━━━━━━━━━━━━</i>\n`;
  message += `<i>📅 ${beijingTime.split(' ')[0]} | 🕒 ${beijingTime.split(' ')[1]}</i>\n`;
  message += `<i>🚀 智能加权轮询系统 v2.0</i>`;
  
  return message;
}