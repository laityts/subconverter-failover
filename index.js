// 默认常量（可通过环境变量覆盖）
const DEFAULT_CACHE_TTL = 60 * 1000; // 健康状态缓存1分钟
const DEFAULT_HEALTH_CHECK_TIMEOUT = 2000; // 健康检查超时2秒
const TG_MESSAGE_MAX_LENGTH = 4096; // Telegram消息最大长度
const DEFAULT_CONCURRENT_HEALTH_CHECKS = 5; // 并发健康检查数量
const DEFAULT_FAST_CHECK_TIMEOUT = 800; // 快速检查超时800ms
const DEFAULT_FAST_CHECK_CACHE_TTL = 2000; // 快速检查缓存2秒
const DEFAULT_KV_WRITE_COOLDOWN = 30 * 1000; // KV写入冷却时间30秒
const DEFAULT_HEALTHY_WEIGHT_INCREMENT = 10; // 健康状态权重增量
const DEFAULT_FAILURE_WEIGHT_DECREMENT = 20; // 故障权重减量
const DEFAULT_MAX_WEIGHT = 100; // 最大权重
const DEFAULT_MIN_WEIGHT = 10; // 最小权重
const DEFAULT_WEIGHT_RECOVERY_RATE = 5; // 权重恢复速率
const DEFAULT_BACKEND_STALE_THRESHOLD = 30 * 1000; // 后端信息过期阈值30秒

// 默认后端列表
const DEFAULT_BACKENDS = [];

// 全局缓存对象
let cache = {
  backends: null,
  lastUpdated: 0,
  healthStatus: null,
  healthLastUpdated: 0,
  lastAvailableBackend: null,
  backendVersions: new Map(),
  fastHealthChecks: new Map(),
  healthyBackendsList: [],
  healthyBackendsLastUpdated: 0,
  ipNotificationTimestamps: new Map(),
  ipNotificationBackends: new Map(),
  backendVersionCache: new Map(),
  lastKVWriteTimes: new Map(),
  lastHealthNotificationStatus: null,
  lastServiceStatus: 'unknown', // 改为'unknown'，表示初始状态
  backendWeights: new Map(), // 新增：后端权重
  backendFailureCounts: new Map(), // 新增：后端失败计数
  lastSuccessfulRequests: new Map(), // 新增：最后成功请求时间
  weightedBackendCache: [], // 新增：加权后端缓存
  weightedCacheLastUpdated: 0, // 新增：加权缓存最后更新时间
  requestCounts: new Map(), // 新增：请求计数
  errorLogs: [], // 新增：错误日志（限制大小）
  performanceStats: { // 新增：性能统计
    totalRequests: 0,
    successfulRequests: 0,
    failedRequests: 0,
    avgResponseTime: 0,
    lastResetTime: Date.now()
  }
};

// 生成唯一请求ID用于日志追踪
function generateRequestId() {
  return Array.from(crypto.getRandomValues(new Uint8Array(4)))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}

// 统一配置读取函数（支持类型转换和验证）
function getConfig(env, key, defaultValue) {
  // 如果环境变量中不存在该键，返回默认值
  if (!(key in env)) {
    return defaultValue;
  }
  
  const value = env[key];
  
  // 如果值是空字符串，返回默认值
  if (value === '') {
    return defaultValue;
  }
  
  // 根据默认值的类型进行转换
  if (typeof defaultValue === 'number') {
    const num = parseInt(value, 10);
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
  
  // 字符串类型直接返回
  return value;
}

// 验证配置值的有效性
function validateConfig(env, requestId) {
  const configs = [
    { key: 'CACHE_TTL', min: 1000, max: 300000, defaultValue: DEFAULT_CACHE_TTL },
    { key: 'HEALTH_CHECK_TIMEOUT', min: 100, max: 10000, defaultValue: DEFAULT_HEALTH_CHECK_TIMEOUT },
    { key: 'CONCURRENT_HEALTH_CHECKS', min: 1, max: 20, defaultValue: DEFAULT_CONCURRENT_HEALTH_CHECKS },
    { key: 'FAST_CHECK_TIMEOUT', min: 100, max: 5000, defaultValue: DEFAULT_FAST_CHECK_TIMEOUT },
    { key: 'FAST_CHECK_CACHE_TTL', min: 500, max: 30000, defaultValue: DEFAULT_FAST_CHECK_CACHE_TTL },
    { key: 'KV_WRITE_COOLDOWN', min: 5000, max: 300000, defaultValue: DEFAULT_KV_WRITE_COOLDOWN },
    { key: 'MAX_WEIGHT', min: 10, max: 1000, defaultValue: DEFAULT_MAX_WEIGHT },
    { key: 'MIN_WEIGHT', min: 1, max: 100, defaultValue: DEFAULT_MIN_WEIGHT },
    { key: 'WEIGHT_RECOVERY_RATE', min: 1, max: 100, defaultValue: DEFAULT_WEIGHT_RECOVERY_RATE },
    { key: 'FAILURE_WEIGHT_DECREMENT', min: 1, max: 100, defaultValue: DEFAULT_FAILURE_WEIGHT_DECREMENT },
    { key: 'BACKEND_STALE_THRESHOLD', min: 1000, max: 300000, defaultValue: DEFAULT_BACKEND_STALE_THRESHOLD }
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
  
  if (errors.length > 0 && requestId) {
    console.warn(`[${requestId}] 配置验证警告:`, errors);
  }
  
  return errors;
}

// 获取环境变量中的后端列表
function getBackendsFromEnv(env) {
  try {
    if (env.BACKEND_URLS) {
      const backends = JSON.parse(env.BACKEND_URLS);
      
      // 验证后端URL格式
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

// KV写入节流检查
function canWriteKV(key, cooldown, env) {
  const now = Date.now();
  const lastWriteTime = cache.lastKVWriteTimes.get(key) || 0;
  
  if (now - lastWriteTime < cooldown) {
    return false;
  }
  
  cache.lastKVWriteTimes.set(key, now);
  return true;
}

// 检查是否需要发送IP通知
function shouldSendIPNotification(clientIp, backendUrl) {
  const lastBackend = cache.ipNotificationBackends.get(clientIp);
  
  // 如果从未发送过通知，或者后端发生变化，需要发送
  return !lastBackend || lastBackend !== backendUrl;
}

// 更新IP通知记录
function updateIPNotificationRecord(clientIp, backendUrl) {
  const now = Date.now();
  cache.ipNotificationTimestamps.set(clientIp, now);
  cache.ipNotificationBackends.set(clientIp, backendUrl);
  
  // 定期清理过期的IP记录（24小时）
  const maxAge = 24 * 60 * 60 * 1000; // 24小时
  for (const [ip, timestamp] of cache.ipNotificationTimestamps.entries()) {
    if (now - timestamp > maxAge) {
      cache.ipNotificationTimestamps.delete(ip);
      cache.ipNotificationBackends.delete(ip);
    }
  }
}

// 获取后端版本信息（优化版本）
async function getBackendVersion(backendUrl, requestId) {
  const cacheKey = `version_${backendUrl}`;
  const cached = cache.backendVersionCache.get(cacheKey);
  const now = Date.now();
  
  // 版本信息缓存5分钟
  if (cached && now - cached.timestamp < 5 * 60 * 1000) {
    return cached.version;
  }
  
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), getConfig({}, 'FAST_CHECK_TIMEOUT', DEFAULT_FAST_CHECK_TIMEOUT));
    
    const response = await fetch(`${backendUrl}/version`, {
      signal: controller.signal,
      headers: { 
        'User-Agent': 'subconverter-failover-worker/1.0',
        'Accept': 'text/plain',
        'X-Request-ID': requestId
      }
    });
    
    clearTimeout(timeoutId);
    
    if (response.status === 200) {
      const text = await response.text();
      const version = text.trim();
      
      // 缓存版本信息
      cache.backendVersionCache.set(cacheKey, {
        version: version || '未知版本',
        timestamp: now
      });
      
      return version || '未知版本';
    }
  } catch (error) {
    logError(`获取后端版本失败: ${backendUrl}`, error, requestId);
  }
  
  // 返回默认值
  return '未知版本';
}

// 智能缓存失效检查
function isCacheValid(cacheTimestamp, maxAge, backendUrl = null) {
  if (!cacheTimestamp) return false;
  
  const now = Date.now();
  const age = now - cacheTimestamp;
  
  // 如果有后端URL，检查后端信息是否过时
  if (backendUrl) {
    const lastSuccess = cache.lastSuccessfulRequests.get(backendUrl) || 0;
    if (lastSuccess > 0 && now - lastSuccess > getConfig({}, 'BACKEND_STALE_THRESHOLD', DEFAULT_BACKEND_STALE_THRESHOLD)) {
      return false; // 后端信息已过时
    }
  }
  
  return age < maxAge;
}

// 更新后端权重
function updateBackendWeight(backendUrl, success, env) {
  const MAX_WEIGHT = getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT);
  const MIN_WEIGHT = getConfig(env, 'MIN_WEIGHT', DEFAULT_MIN_WEIGHT);
  const WEIGHT_RECOVERY_RATE = getConfig(env, 'WEIGHT_RECOVERY_RATE', DEFAULT_WEIGHT_RECOVERY_RATE);
  const FAILURE_WEIGHT_DECREMENT = getConfig(env, 'FAILURE_WEIGHT_DECREMENT', DEFAULT_FAILURE_WEIGHT_DECREMENT);
  
  let currentWeight = cache.backendWeights.get(backendUrl) || MAX_WEIGHT;
  let failureCount = cache.backendFailureCounts.get(backendUrl) || 0;
  
  if (success) {
    // 成功请求：增加权重，减少失败计数
    currentWeight = Math.min(MAX_WEIGHT, currentWeight + WEIGHT_RECOVERY_RATE);
    failureCount = Math.max(0, failureCount - 1);
    
    // 记录最后成功时间
    cache.lastSuccessfulRequests.set(backendUrl, Date.now());
    
    // 更新性能统计
    cache.performanceStats.successfulRequests++;
  } else {
    // 失败请求：减少权重，增加失败计数
    currentWeight = Math.max(MIN_WEIGHT, currentWeight - FAILURE_WEIGHT_DECREMENT);
    failureCount++;
    
    // 更新性能统计
    cache.performanceStats.failedRequests++;
  }
  
  cache.backendWeights.set(backendUrl, currentWeight);
  cache.backendFailureCounts.set(backendUrl, failureCount);
  
  // 重置加权缓存
  cache.weightedBackendCache = [];
  cache.weightedCacheLastUpdated = 0;
  
  return currentWeight;
}

// 获取加权后端列表
function getWeightedBackends(backends, env) {
  const now = Date.now();
  const MAX_WEIGHT = getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT);
  const MIN_WEIGHT = getConfig(env, 'MIN_WEIGHT', DEFAULT_MIN_WEIGHT);
  const BACKEND_STALE_THRESHOLD = getConfig(env, 'BACKEND_STALE_THRESHOLD', DEFAULT_BACKEND_STALE_THRESHOLD);
  
  // 如果加权缓存有效且未过期（10秒），直接返回
  if (cache.weightedBackendCache.length > 0 && 
      now - cache.weightedCacheLastUpdated < 10000) {
    return cache.weightedBackendCache;
  }
  
  const weightedList = [];
  
  for (const backend of backends) {
    const weight = cache.backendWeights.get(backend) || MAX_WEIGHT;
    const failureCount = cache.backendFailureCounts.get(backend) || 0;
    
    // 如果连续失败次数过多，暂时排除
    if (failureCount > 5) {
      continue;
    }
    
    // 检查后端信息是否过时
    const lastSuccess = cache.lastSuccessfulRequests.get(backend);
    if (lastSuccess && now - lastSuccess > BACKEND_STALE_THRESHOLD) {
      continue;
    }
    
    // 根据权重添加相应数量的条目
    const entryCount = Math.max(1, Math.floor(weight / 10));
    for (let i = 0; i < entryCount; i++) {
      weightedList.push({
        url: backend,
        weight: weight
      });
    }
  }
  
  // 如果没有可用的后端，至少包含一个
  if (weightedList.length === 0 && backends.length > 0) {
    weightedList.push({
      url: backends[0],
      weight: MIN_WEIGHT
    });
  }
  
  // 随机打乱列表
  for (let i = weightedList.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [weightedList[i], weightedList[j]] = [weightedList[j], weightedList[i]];
  }
  
  cache.weightedBackendCache = weightedList;
  cache.weightedCacheLastUpdated = now;
  
  return weightedList;
}

// 加权轮询选择后端
function selectBackendByWeight(backends, requestId, env) {
  const weightedBackends = getWeightedBackends(backends, env);
  
  if (weightedBackends.length === 0) {
    return null;
  }
  
  // 选择第一个（已经随机打乱）
  const selected = weightedBackends[0];
  
  // 记录选择
  const requestCount = cache.requestCounts.get(selected.url) || 0;
  cache.requestCounts.set(selected.url, requestCount + 1);
  
  console.log(`[${requestId}] 加权选择后端: ${selected.url}, 权重: ${selected.weight}`);
  return selected.url;
}

// 清理过期缓存
function cleanupExpiredCache(env) {
  const now = Date.now();
  
  // 清理fastHealthChecks缓存（超过10秒）
  for (const [key, value] of cache.fastHealthChecks.entries()) {
    if (now - value.timestamp > 10000) {
      cache.fastHealthChecks.delete(key);
    }
  }
  
  // 清理backendVersionCache缓存（超过30分钟）
  for (const [key, value] of cache.backendVersionCache.entries()) {
    if (now - value.timestamp > 30 * 60 * 1000) {
      cache.backendVersionCache.delete(key);
    }
  }
  
  // 清理错误日志（保留最近的100条）
  if (cache.errorLogs.length > 100) {
    cache.errorLogs = cache.errorLogs.slice(-100);
  }
  
  // 清理过期的IP记录（24小时）
  for (const [ip, timestamp] of cache.ipNotificationTimestamps.entries()) {
    if (now - timestamp > 24 * 60 * 60 * 1000) {
      cache.ipNotificationTimestamps.delete(ip);
      cache.ipNotificationBackends.delete(ip);
    }
  }
}

// 错误日志记录
function logError(message, error, requestId) {
  const errorEntry = {
    timestamp: new Date().toISOString(),
    requestId: requestId || 'unknown',
    message: message,
    error: error?.message || String(error),
    stack: error?.stack
  };
  
  cache.errorLogs.push(errorEntry);
  
  // 控制台输出简化版本
  console.error(`[${requestId || 'system'}] ${message}: ${error?.message || error}`);
}

// 发送订阅转换请求通知（异步）- 优化版，添加耗时统计
async function sendSubconverterRequestNotification(clientIp, backendUrl, backendSelectionTime, responseTime, requestId, env, version = null) {
  const botToken = getConfig(env, 'TG_BOT_TOKEN', '');
  const chatId = getConfig(env, 'TG_CHAT_ID', '');
  
  if (!botToken || !chatId) {
    return false;
  }
  
  try {
    // 获取北京时间（修复时间转换）
    const now = new Date();
    const beijingTime = new Date(now.getTime() + 8 * 60 * 60 * 1000);
    const beijingTimeStr = beijingTime.toISOString().replace('T', ' ').substring(0, 19);
    
    // 如果版本为null，尝试获取版本
    let finalVersion = version;
    if (finalVersion === null) {
      finalVersion = await getBackendVersion(backendUrl, requestId);
    }
    
    // 计算总耗时
    const totalTime = backendSelectionTime + responseTime;
    
    // 创建通知消息
    let message = `🔔 订阅转换请求通知\n\n`;
    message += `⏰ 请求时间: ${beijingTimeStr} (北京时间)\n`;
    message += `🚀 使用后端: ${backendUrl}\n`;
    message += `📦 版本: ${finalVersion}\n\n`;
    message += `⏱️ 耗时统计:\n`;
    message += `  ├─ 后端选择耗时: ${backendSelectionTime}ms\n`;
    message += `  ├─ 请求响应耗时: ${responseTime}ms\n`;
    message += `  └─ 总耗时: ${totalTime}ms\n\n`;
    message += `📝 请求ID: ${requestId}\n`;
    message += `🌐 客户端IP: ${clientIp}`;
    
    // 发送到Telegram
    const telegramUrl = `https://api.telegram.org/bot${botToken}/sendMessage`;
    const response = await fetch(telegramUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        chat_id: chatId,
        text: message,
        parse_mode: 'Markdown',
        disable_web_page_preview: true,
        disable_notification: false
      })
    });
    
    if (response.ok) {
      console.log(`[${requestId}] 订阅转换请求通知发送成功，IP: ${clientIp}`);
      return true;
    } else {
      logError(`订阅转换请求通知发送失败，状态码: ${response.status}`, null, requestId);
      return false;
    }
  } catch (error) {
    logError('订阅转换请求通知发送异常', error, requestId);
    return false;
  }
}

// 极速健康检查（优化版本，只检查最基本功能）
async function ultraFastHealthCheck(url, requestId, env) {
  const cacheKey = `ultrafast_health_${url}`;
  const cached = cache.fastHealthChecks.get(cacheKey);
  const now = Date.now();
  
  const FAST_CHECK_TIMEOUT = getConfig(env, 'FAST_CHECK_TIMEOUT', DEFAULT_FAST_CHECK_TIMEOUT);
  const FAST_CHECK_CACHE_TTL = getConfig(env, 'FAST_CHECK_CACHE_TTL', DEFAULT_FAST_CHECK_CACHE_TTL);
  const BACKEND_STALE_THRESHOLD = getConfig(env, 'BACKEND_STALE_THRESHOLD', DEFAULT_BACKEND_STALE_THRESHOLD);
  
  // 智能缓存检查
  if (cached && isCacheValid(cached.timestamp, FAST_CHECK_CACHE_TTL, url)) {
    return cached.result;
  }
  
  // 清理过期的缓存条目
  if (cached && !isCacheValid(cached.timestamp, FAST_CHECK_CACHE_TTL * 2)) {
    cache.fastHealthChecks.delete(cacheKey);
  }
  
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), FAST_CHECK_TIMEOUT);
    
    const startTime = Date.now();
    const response = await fetch(`${url}/version`, {
      signal: controller.signal,
      headers: { 
        'User-Agent': 'subconverter-failover-worker/1.0',
        'Accept': 'text/plain',
        'X-Request-ID': requestId
      },
      cf: {
        cacheTtl: 0,
        scrapeShield: false,
        polish: 'off'
      }
    });
    const responseTime = Date.now() - startTime;
    
    clearTimeout(timeoutId);
    
    // 快速验证：只需要200状态码
    const result = {
      healthy: response.status === 200,
      responseTime,
      timestamp: now,
      status: response.status
    };
    
    // 如果健康，尝试读取版本
    if (result.healthy) {
      try {
        const text = await response.text().catch(() => '');
        const version = text.trim();
        result.version = version || '未知版本';
        
        // 更新版本缓存
        if (version) {
          cache.backendVersionCache.set(`version_${url}`, {
            version: version,
            timestamp: now
          });
        }
        
        // 更新后端权重
        updateBackendWeight(url, true, env);
      } catch (e) {
        result.version = '未知版本';
        updateBackendWeight(url, false, env);
      }
    } else {
      result.version = '未知版本';
      updateBackendWeight(url, false, env);
    }
    
    // 缓存极速检查结果
    cache.fastHealthChecks.set(cacheKey, {
      result,
      timestamp: now
    });
    
    // 更新健康后端列表缓存
    if (result.healthy) {
      updateHealthyBackendsList(url, responseTime);
    }
    
    return result;
  } catch (error) {
    const result = {
      healthy: false,
      responseTime: null,
      timestamp: now,
      error: error.name,
      version: '未知版本'
    };
    
    // 缓存失败结果
    cache.fastHealthChecks.set(cacheKey, {
      result,
      timestamp: now
    });
    
    // 更新后端权重
    updateBackendWeight(url, false, env);
    
    return result;
  }
}

// 更新健康后端列表（按响应时间排序）
function updateHealthyBackendsList(url, responseTime) {
  if (!cache.healthyBackendsList) {
    cache.healthyBackendsList = [];
  }
  
  const existingIndex = cache.healthyBackendsList.findIndex(item => item.url === url);
  const now = Date.now();
  
  if (existingIndex >= 0) {
    // 更新现有记录
    cache.healthyBackendsList[existingIndex] = {
      url,
      responseTime,
      lastChecked: now
    };
  } else {
    // 添加新记录
    cache.healthyBackendsList.push({
      url,
      responseTime,
      lastChecked: now
    });
  }
  
  // 按响应时间排序（最快的排前面）
  cache.healthyBackendsList.sort((a, b) => a.responseTime - b.responseTime);
  cache.healthyBackendsLastUpdated = now;
}

// 获取排序后的健康后端列表
function getSortedHealthyBackends(forceRefresh = false) {
  const now = Date.now();
  
  // 清理过期的记录
  cache.healthyBackendsList = cache.healthyBackendsList.filter(
    item => now - item.lastChecked < 10000
  );
  
  // 如果缓存过期（5秒）或强制刷新，返回空数组让外部重新检查
  if (forceRefresh || !cache.healthyBackendsList || 
      now - cache.healthyBackendsLastUpdated > 5000) {
    return [];
  }
  
  return cache.healthyBackendsList;
}

// 带缓存的详细健康检查
async function checkBackendHealth(url, requestId, env) {
  const cacheKey = `health_${url}`;
  const cached = cache.backendVersions.get(cacheKey);
  const now = Date.now();
  
  // 获取配置的超时时间
  const healthCheckTimeout = getConfig(env, 'HEALTH_CHECK_TIMEOUT', DEFAULT_HEALTH_CHECK_TIMEOUT);
  const cacheTtl = getConfig(env, 'CACHE_TTL', DEFAULT_CACHE_TTL);
  
  // 智能缓存检查
  if (cached && isCacheValid(cached.timestamp, cacheTtl, url)) {
    return cached.result;
  }
  
  // 清理过期的缓存条目
  if (cached && !isCacheValid(cached.timestamp, cacheTtl * 2)) {
    cache.backendVersions.delete(cacheKey);
  }
  
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), healthCheckTimeout);
    
    const startTime = Date.now();
    const response = await fetch(`${url}/version`, {
      signal: controller.signal,
      headers: { 
        'User-Agent': 'subconverter-failover-worker/1.0',
        'Accept': 'text/plain',
        'X-Request-ID': requestId
      }
    });
    const responseTime = Date.now() - startTime;
    
    clearTimeout(timeoutId);
    
    let result;
    if (response.status === 200) {
      const text = await response.text();
      const healthy = text.includes('subconverter');
      const version = text.trim().substring(0, 50);
      result = {
        healthy,
        version: healthy ? version : '未知版本',
        timestamp: new Date().toISOString(),
        status: response.status,
        responseTime
      };
      
      // 更新版本缓存
      if (healthy && version) {
        cache.backendVersionCache.set(`version_${url}`, {
          version: version,
          timestamp: now
        });
      }
      
      // 更新后端权重
      updateBackendWeight(url, healthy, env);
    } else {
      result = { 
        healthy: false, 
        status: response.status,
        timestamp: new Date().toISOString(),
        responseTime,
        version: '未知版本'
      };
      updateBackendWeight(url, false, env);
    }
    
    // 缓存结果
    cache.backendVersions.set(cacheKey, {
      result,
      timestamp: now
    });
    
    return result;
  } catch (error) {
    const result = { 
      healthy: false, 
      error: error.name === 'AbortError' ? 'Timeout' : error.message,
      timestamp: new Date().toISOString(),
      version: '未知版本'
    };
    
    // 缓存失败结果
    cache.backendVersions.set(cacheKey, {
      result,
      timestamp: now
    });
    
    // 更新后端权重
    updateBackendWeight(url, false, env);
    
    return result;
  }
}

// 获取后端列表（带缓存）
async function getBackends(env, requestId) {
  const now = Date.now();
  const cacheTtl = getConfig(env, 'CACHE_TTL', DEFAULT_CACHE_TTL);
  
  // 智能缓存检查
  if (cache.backends && isCacheValid(cache.lastUpdated, cacheTtl)) {
    return cache.backends;
  }
  
  try {
    const backends = getBackendsFromEnv(env);
    
    // 更新缓存
    cache.backends = backends;
    cache.lastUpdated = now;
    
    // 初始化后端权重
    const MAX_WEIGHT = getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT);
    for (const backend of backends) {
      if (!cache.backendWeights.has(backend)) {
        cache.backendWeights.set(backend, MAX_WEIGHT);
      }
    }
    
    return backends;
  } catch (error) {
    logError('获取后端列表失败', error, requestId);
    return cache.backends || DEFAULT_BACKENDS;
  }
}

// 获取健康状态（带缓存）
async function getHealthStatus(kv, requestId, env) {
  const now = Date.now();
  const cacheTtl = getConfig(env, 'CACHE_TTL', DEFAULT_CACHE_TTL);
  
  // 智能缓存检查
  if (cache.healthStatus && isCacheValid(cache.healthLastUpdated, cacheTtl)) {
    return cache.healthStatus;
  }
  
  try {
    const status = await kv.get('health_status', 'json');
    const healthStatus = status || {};
    
    // 更新缓存
    cache.healthStatus = healthStatus;
    cache.healthLastUpdated = now;
    
    return healthStatus;
  } catch (error) {
    logError('获取健康状态失败', error, requestId);
    return cache.healthStatus || {};
  }
}

// 检查健康状态是否真正发生变化
function hasHealthStatusChanged(oldStatus, newStatus) {
  if (!oldStatus || !newStatus) return true;
  
  const oldKeys = Object.keys(oldStatus);
  const newKeys = Object.keys(newStatus);
  
  // 键的数量不同，肯定变化了
  if (oldKeys.length !== newKeys.length) return true;
  
  // 比较每个后端的基本健康状态
  for (const key of oldKeys) {
    const oldHealth = oldStatus[key];
    const newHealth = newStatus[key];
    
    if (!newHealth) return true;
    
    // 只比较核心的健康状态，忽略时间戳等辅助信息
    if (oldHealth.healthy !== newHealth.healthy) return true;
  }
  
  return false;
}

// 保存健康状态（优化：减少写入）
async function saveHealthStatus(kv, newStatus, requestId, env) {
  try {
    const KV_WRITE_COOLDOWN = getConfig(env, 'KV_WRITE_COOLDOWN', DEFAULT_KV_WRITE_COOLDOWN);
    
    // 检查KV写入节流
    if (!canWriteKV('health_status', KV_WRITE_COOLDOWN, env)) {
      console.log(`[${requestId}] 健康状态写入被节流，仅更新内存缓存`);
      
      // 只更新内存缓存
      const dataToSave = {
        ...newStatus,
        last_updated: new Date().toISOString()
      };
      cache.healthStatus = dataToSave;
      cache.healthLastUpdated = Date.now();
      return true;
    }
    
    // 先获取当前状态
    let currentStatus = cache.healthStatus;
    if (!currentStatus) {
      try {
        const stored = await kv.get('health_status', 'json');
        currentStatus = stored || {};
      } catch (e) {
        currentStatus = {};
      }
    }
    
    // 检查状态是否真正发生变化
    if (!hasHealthStatusChanged(currentStatus, newStatus)) {
      console.log(`[${requestId}] 健康状态未变化，跳过KV写入`);
      
      // 只更新内存缓存（不写入KV）
      const dataToSave = {
        ...newStatus,
        last_updated: new Date().toISOString()
      };
      cache.healthStatus = dataToSave;
      cache.healthLastUpdated = Date.now();
      return true;
    }
    
    // 状态发生变化，才写入KV
    const dataToSave = {
      ...newStatus,
      last_updated: new Date().toISOString()
    };
    await kv.put('health_status', JSON.stringify(dataToSave));
    
    // 更新缓存
    cache.healthStatus = dataToSave;
    cache.healthLastUpdated = Date.now();
    
    console.log(`[${requestId}] 健康状态已更新并保存到KV`);
    return true;
  } catch (error) {
    logError('保存健康状态失败', error, requestId);
    return false;
  }
}

// 获取上次可用后端
async function getLastAvailableBackend(kv, requestId) {
  // 首先检查内存缓存
  if (cache.lastAvailableBackend) {
    return cache.lastAvailableBackend;
  }
  
  try {
    const lastBackend = await kv.get('last_available_backend', 'text');
    if (lastBackend) {
      cache.lastAvailableBackend = lastBackend;
    }
    return lastBackend;
  } catch (error) {
    logError('获取上次可用后端失败', error, requestId);
    return null;
  }
}

// 保存上次可用后端（优化：减少写入）
async function saveLastAvailableBackend(kv, backendUrl, requestId, env) {
  try {
    const KV_WRITE_COOLDOWN = getConfig(env, 'KV_WRITE_COOLDOWN', DEFAULT_KV_WRITE_COOLDOWN);
    
    // 检查KV写入节流
    if (!canWriteKV('last_available_backend', KV_WRITE_COOLDOWN, env)) {
      console.log(`[${requestId}] 上次可用后端写入被节流，仅更新内存缓存`);
      cache.lastAvailableBackend = backendUrl;
      return true;
    }
    
    // 检查是否与当前值相同
    const currentBackend = cache.lastAvailableBackend;
    if (currentBackend === backendUrl) {
      console.log(`[${requestId}] 上次可用后端未变化，跳过KV写入`);
      return true;
    }
    
    // 只有当值发生变化时才写入KV
    await kv.put('last_available_backend', backendUrl);
    cache.lastAvailableBackend = backendUrl;
    
    console.log(`[${requestId}] 上次可用后端已更新为: ${backendUrl}`);
    return true;
  } catch (error) {
    logError('保存上次可用后端失败', error, requestId);
    return false;
  }
}

// 并行极速健康检查（优化版本）- 修复并发控制
async function parallelUltraFastHealthChecks(urls, requestId, env) {
  const results = new Map();
  const CONCURRENT_HEALTH_CHECKS = getConfig(env, 'CONCURRENT_HEALTH_CHECKS', DEFAULT_CONCURRENT_HEALTH_CHECKS);
  
  // 实现真正的并发控制
  const executeWithConcurrency = async (tasks, maxConcurrent) => {
    const results = [];
    const executing = new Set();
    
    for (const task of tasks) {
      // 如果达到最大并发数，等待一个任务完成
      if (executing.size >= maxConcurrent) {
        await Promise.race(executing);
      }
      
      const taskPromise = task();
      executing.add(taskPromise);
      taskPromise.finally(() => executing.delete(taskPromise));
      results.push(taskPromise);
    }
    
    return Promise.allSettled(results);
  };
  
  const tasks = urls.map(url => async () => {
    try {
      const health = await ultraFastHealthCheck(url, `${requestId}-${url}`, env);
      return { url, health };
    } catch (error) {
      return { url, health: { healthy: false, error: error.name, version: '未知版本' } };
    }
  });
  
  const checkResults = await executeWithConcurrency(tasks, CONCURRENT_HEALTH_CHECKS);
  
  // 处理结果
  checkResults.forEach(result => {
    if (result.status === 'fulfilled') {
      const { url, health } = result.value;
      results.set(url, health);
    }
  });
  
  return results;
}

// 智能查找可用后端（订阅转换请求专用）- 优化版本，包含加权轮询
async function findAvailableBackendForRequest(kv, requestId, env) {
  const backends = await getBackends(env, requestId);
  
  if (backends.length === 0) {
    return { backend: null, selectionTime: 0 };
  }
  
  const selectionStartTime = Date.now();
  
  // 策略0: 加权轮询选择后端
  const weightedBackend = selectBackendByWeight(backends, requestId, env);
  if (weightedBackend) {
    // 快速检查加权选择的后端
    const fastCheck = await ultraFastHealthCheck(weightedBackend, `${requestId}-weighted-${weightedBackend}`, env);
    if (fastCheck.healthy) {
      console.log(`[${requestId}] 使用加权选择后端: ${weightedBackend}, 响应时间: ${fastCheck.responseTime}ms`);
      
      // 异步更新上次可用后端
      setTimeout(() => {
        saveLastAvailableBackend(kv, weightedBackend, `${requestId}-async-weighted`, env);
      }, 0);
      
      const selectionTime = Date.now() - selectionStartTime;
      return { backend: weightedBackend, selectionTime };
    }
  }
  
  // 策略1: 检查内存中已排序的健康后端
  const healthyBackends = getSortedHealthyBackends();
  if (healthyBackends.length > 0) {
    // 直接使用响应最快的前3个进行检查
    const candidates = healthyBackends.slice(0, Math.min(3, healthyBackends.length));
    
    for (const candidate of candidates) {
      const fastCheck = await ultraFastHealthCheck(candidate.url, `${requestId}-cached-${candidate.url}`, env);
      if (fastCheck.healthy) {
        console.log(`[${requestId}] 使用缓存健康后端: ${candidate.url}, 响应时间: ${fastCheck.responseTime}ms`);
        
        // 异步更新上次可用后端
        setTimeout(() => {
          saveLastAvailableBackend(kv, candidate.url, `${requestId}-async`, env);
        }, 0);
        
        const selectionTime = Date.now() - selectionStartTime;
        return { backend: candidate.url, selectionTime };
      }
    }
  }
  
  // 策略2: 检查上次可用的后端（快速路径）
  const lastBackend = cache.lastAvailableBackend;
  if (lastBackend && backends.includes(lastBackend)) {
    const fastCheck = await ultraFastHealthCheck(lastBackend, requestId, env);
    if (fastCheck.healthy) {
      console.log(`[${requestId}] 使用上次可用后端: ${lastBackend}, 响应时间: ${fastCheck.responseTime}ms`);
      const selectionTime = Date.now() - selectionStartTime;
      return { backend: lastBackend, selectionTime };
    }
  }
  
  // 策略3: 并行极速检查所有后端
  console.log(`[${requestId}] 并行极速检查 ${backends.length} 个后端`);
  
  const checkResults = await parallelUltraFastHealthChecks(backends, requestId, env);
  
  // 找到响应最快的健康后端
  let fastestBackend = null;
  let fastestTime = Infinity;
  
  for (const [url, health] of checkResults.entries()) {
    if (health.healthy && health.responseTime < fastestTime) {
      fastestBackend = url;
      fastestTime = health.responseTime;
    }
  }
  
  if (fastestBackend) {
    console.log(`[${requestId}] 找到最快可用后端: ${fastestBackend}, 响应时间: ${fastestTime}ms`);
    
    // 异步保存到KV
    setTimeout(() => {
      saveLastAvailableBackend(kv, fastestBackend, `${requestId}-async-fastest`, env);
    }, 0);
    
    const selectionTime = Date.now() - selectionStartTime;
    return { backend: fastestBackend, selectionTime };
  }
  
  // 策略4: 如果有部分后端返回了结果但标记为不健康，尝试其中一个作为最后手段
  console.log(`[${requestId}] 极速检查失败，尝试已返回的后端`);
  
  for (const [url, health] of checkResults.entries()) {
    // 即使健康检查失败，也可能后端实际可用
    if (health.status === 200) {
      console.log(`[${requestId}] 尝试状态码200的后端: ${url}`);
      
      // 异步保存到KV
      setTimeout(() => {
        saveLastAvailableBackend(kv, url, `${requestId}-async-fallback`, env);
      }, 0);
      
      const selectionTime = Date.now() - selectionStartTime;
      return { backend: url, selectionTime };
    }
  }
  
  console.log(`[${requestId}] 所有后端均不可用`);
  const selectionTime = Date.now() - selectionStartTime;
  return { backend: null, selectionTime };
}

// 处理订阅转换请求 - 优化版本
async function handleSubconverterRequest(request, backendUrl, backendSelectionTime, requestId, env, ctx) {
  const url = new URL(request.url);
  const backendPath = url.pathname + url.search;
  
  console.log(`[${requestId}] 转发请求到后端: ${backendUrl}${backendPath}`);
  
  try {
    const requestStartTime = Date.now();
    
    // 克隆请求，但优化一些头信息
    const backendRequest = new Request(`${backendUrl}${backendPath}`, {
      method: request.method,
      headers: request.headers,
      body: request.body,
      redirect: 'follow',
      cf: {
        cacheEverything: false,
        cacheTtl: 0,
        polish: 'off',
        scrapeShield: false
      }
    });
    
    // 优化头信息
    backendRequest.headers.delete('host');
    backendRequest.headers.set('host', new URL(backendUrl).host);
    
    // 只添加必要的追踪头
    backendRequest.headers.set('X-Request-ID', requestId);
    backendRequest.headers.set('X-Forwarded-By', 'subconverter-failover-worker');
    
    const response = await fetch(backendRequest);
    const responseTime = Date.now() - requestStartTime;
    
    console.log(`[${requestId}] 后端响应时间: ${responseTime}ms, 状态码: ${response.status}`);
    
    // 更新性能统计
    cache.performanceStats.totalRequests++;
    cache.performanceStats.avgResponseTime = 
      (cache.performanceStats.avgResponseTime * (cache.performanceStats.totalRequests - 1) + responseTime) / 
      cache.performanceStats.totalRequests;
    
    // 更新后端权重
    updateBackendWeight(backendUrl, response.ok, env);
    
    // 只复制必要的响应头
    const responseHeaders = new Headers();
    
    // 复制原响应头（过滤掉一些不必要的）
    for (const [key, value] of response.headers.entries()) {
      if (!key.startsWith('cf-') && key !== 'server') {
        responseHeaders.set(key, value);
      }
    }
    
    // 添加我们的追踪头
    responseHeaders.set('X-Backend-Server', backendUrl);
    responseHeaders.set('X-Response-Time', `${responseTime}ms`);
    responseHeaders.set('X-Backend-Selection-Time', `${backendSelectionTime}ms`);
    responseHeaders.set('X-Total-Time', `${backendSelectionTime + responseTime}ms`);
    responseHeaders.set('X-Request-ID', requestId);
    
    // 添加缓存控制头
    if (!responseHeaders.has('Cache-Control')) {
      responseHeaders.set('Cache-Control', 'no-store, max-age=0');
    }
    
    // 异步发送订阅转换请求通知（不阻塞主响应）
    if (response.ok) {
      // 获取客户端IP
      const clientIp = request.headers.get('cf-connecting-ip') || 
                       request.headers.get('x-forwarded-for') || 
                       'unknown';
      
      // 检查是否需要发送通知
      if (shouldSendIPNotification(clientIp, backendUrl)) {
        // 异步发送通知，不等待结果
        ctx.waitUntil(sendSubconverterRequestNotification(
          clientIp, 
          backendUrl, 
          backendSelectionTime, 
          responseTime, 
          requestId, 
          env, 
          null
        ));
        // 更新IP通知记录
        updateIPNotificationRecord(clientIp, backendUrl);
      }
    }
    
    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: responseHeaders
    });
  } catch (error) {
    logError('转发请求失败', error, requestId);
    
    // 快速标记该后端为不健康
    const cacheKey = `ultrafast_health_${backendUrl}`;
    cache.fastHealthChecks.set(cacheKey, {
      result: {
        healthy: false,
        error: error.message,
        timestamp: new Date().toISOString(),
        version: '未知版本'
      },
      timestamp: Date.now()
    });
    
    // 更新后端权重
    updateBackendWeight(backendUrl, false, env);
    
    throw error;
  }
}

// 并发检查多个后端健康状态（优化版本）
async function concurrentHealthChecks(urls, requestId, env) {
  const results = {};
  const CONCURRENT_HEALTH_CHECKS = getConfig(env, 'CONCURRENT_HEALTH_CHECKS', DEFAULT_CONCURRENT_HEALTH_CHECKS);
  
  // 实现并发控制
  const executeWithConcurrency = async (tasks, maxConcurrent) => {
    const results = [];
    const executing = new Set();
    
    for (const task of tasks) {
      // 如果达到最大并发数，等待一个任务完成
      if (executing.size >= maxConcurrent) {
        await Promise.race(executing);
      }
      
      const taskPromise = task();
      executing.add(taskPromise);
      taskPromise.finally(() => executing.delete(taskPromise));
      results.push(taskPromise);
    }
    
    return Promise.allSettled(results);
  };
  
  const tasks = urls.map(url => async () => {
    try {
      const health = await checkBackendHealth(url, `${requestId}-${url}`, env);
      return { url, health };
    } catch (error) {
      return { url, health: { healthy: false, error: error.message, version: '未知版本' } };
    }
  });
  
  const checkResults = await executeWithConcurrency(tasks, CONCURRENT_HEALTH_CHECKS);
  
  // 处理结果
  checkResults.forEach(result => {
    if (result.status === 'fulfilled') {
      const { url, health } = result.value;
      results[url] = health;
    }
  });
  
  return results;
}

// 执行完整健康检查（检查所有后端）- 优化版本
async function performFullHealthCheck(kv, requestId, env) {
  const backends = await getBackends(env, requestId);
  
  if (backends.length === 0) {
    return {
      results: {},
      availableBackend: null,
      timestamp: new Date().toISOString()
    };
  }
  
  // 并发检查所有后端
  const results = await concurrentHealthChecks(backends, requestId, env);
  
  // 找到响应最快的健康后端
  let fastestBackend = null;
  let fastestTime = Infinity;
  
  for (const [url, health] of Object.entries(results)) {
    if (health.healthy && health.responseTime < fastestTime) {
      fastestBackend = url;
      fastestTime = health.responseTime;
    }
  }
  
  // 只有在找到健康后端时才尝试保存
  if (fastestBackend) {
    // 保存健康状态（会自动检查是否需要写入KV）
    await saveHealthStatus(kv, results, requestId, env);
    
    // 保存最快可用的后端（会自动检查是否需要写入KV）
    await saveLastAvailableBackend(kv, fastestBackend, requestId, env);
    
    console.log(`[${requestId}] 发现最快后端: ${fastestBackend}, 响应时间: ${fastestTime}ms`);
  } else {
    // 没有健康后端，只更新内存状态
    console.log(`[${requestId}] 未发现健康后端，仅更新内存状态`);
    
    // 更新内存中的健康状态（不写入KV）
    const dataToSave = {
      ...results,
      last_updated: new Date().toISOString()
    };
    cache.healthStatus = dataToSave;
    cache.healthLastUpdated = Date.now();
  }
  
  return {
    results,
    availableBackend: fastestBackend,
    fastestResponseTime: fastestTime,
    timestamp: new Date().toISOString()
  };
}

// 检查服务状态是否发生变化
function hasServiceStatusChanged(checkResults) {
  const { availableBackend } = checkResults;
  const currentStatus = availableBackend ? 'available' : 'unavailable';
  
  // 第一次检查，状态肯定变化了
  if (cache.lastServiceStatus === 'unknown') {
    cache.lastServiceStatus = currentStatus;
    return true;
  }
  
  // 状态发生变化
  if (cache.lastServiceStatus !== currentStatus) {
    const previousStatus = cache.lastServiceStatus;
    cache.lastServiceStatus = currentStatus;
    console.log(`服务状态从 ${previousStatus} 变为 ${currentStatus}`);
    return true;
  }
  
  // 状态未变化
  return false;
}

// 发送Telegram通知（只在状态变化时发送）
async function sendTelegramNotification(checkResults, requestId, env) {
  const botToken = getConfig(env, 'TG_BOT_TOKEN', '');
  const chatId = getConfig(env, 'TG_CHAT_ID', '');
  
  if (!botToken || !chatId) {
    return false;
  }
  
  try {
    const { results, availableBackend, timestamp } = checkResults;
    const backends = Object.keys(results);
    const healthyCount = Object.values(results).filter(r => r.healthy).length;
    const totalCount = backends.length;
    
    if (totalCount === 0) {
      return false;
    }
    
    const status = availableBackend ? '✅ 正常运行' : '🔴 服务异常';
    
    // 获取北京时间（修复时间转换）
    const utcTime = new Date(timestamp);
    const beijingTime = new Date(utcTime.getTime() + 8 * 60 * 60 * 1000);
    const beijingTimeStr = beijingTime.toISOString().replace('T', ' ').substring(0, 19);
    
    // 创建美观的消息
    let message = `🤖 *订阅转换服务状态报告*\n\n`;
    message += `📅 *报告时间:* ${beijingTimeStr} (北京时间)\n`;
    message += `📊 *服务状态:* ${status}\n`;
    message += `🔧 *健康后端:* ${healthyCount}/${totalCount}\n`;
    message += `📈 *状态变化:* ${hasServiceStatusChanged(checkResults) ? '是' : '否'}\n\n`;
    
    if (availableBackend) {
      message += `🚀 *当前使用后端:*\n\`${availableBackend}\`\n`;
      const version = results[availableBackend]?.version;
      if (version) {
        message += `📦 版本: ${version}\n`;
      }
      const responseTime = results[availableBackend]?.responseTime;
      if (responseTime) {
        message += `⚡ 响应时间: ${responseTime}ms\n`;
      }
      message += '\n';
    } else if (totalCount > 0) {
      message += `⚠️ *警告:* 没有可用的后端服务器！\n\n`;
    }
    
    if (totalCount > 0) {
      message += `📋 *后端详情:*\n`;
      message += `\`\`\`\n`;
      
      for (const url of backends) {
        const result = results[url];
        const MAX_WEIGHT = getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT);
        const weight = cache.backendWeights.get(url) || MAX_WEIGHT;
        const failureCount = cache.backendFailureCounts.get(url) || 0;
        const requestCount = cache.requestCounts.get(url) || 0;
        const lastSuccess = cache.lastSuccessfulRequests.get(url);
        const lastSuccessTime = lastSuccess ? Math.round((Date.now() - lastSuccess) / 1000) : '从未';
        
        const statusEmoji = result.healthy ? '✅' : '❌';
        const statusText = result.healthy ? '正常' : '异常';
        const responseTime = result.responseTime ? `${result.responseTime}ms` : '超时';
        const errorInfo = result.error ? ` (${result.error})` : '';
        
        // 提取域名
        const urlObj = new URL(url);
        const hostname = urlObj.hostname;
        
        message += `${statusEmoji} ${hostname.padEnd(25)} ${statusText.padEnd(4)} ${responseTime.padEnd(8)} 权重:${weight.toString().padEnd(3)} 失败:${failureCount.toString().padEnd(2)} 请求:${requestCount.toString().padEnd(4)} 最后成功:${lastSuccessTime}s\n`;
      }
      
      message += `\`\`\`\n`;
      
      // 添加摘要
      if (healthyCount === totalCount) {
        message += `🎉 *所有后端服务器正常运行*`;
      } else if (healthyCount === 0) {
        message += `🚨 *所有后端服务器异常，服务不可用*`;
      } else {
        message += `⚠️ *部分后端异常，建议检查*`;
      }
    } else {
      message += `📝 *提示:* 尚未配置后端服务器，请通过Dashboard配置`;
    }
    
    // 如果消息太长，进行截断
    if (message.length > TG_MESSAGE_MAX_LENGTH) {
      const originalLength = message.length;
      message = message.substring(0, TG_MESSAGE_MAX_LENGTH - 100) + '\n\n...（消息过长，已截断）';
    }
    
    // 发送到Telegram
    const telegramUrl = `https://api.telegram.org/bot${botToken}/sendMessage`;
    const response = await fetch(telegramUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        chat_id: chatId,
        text: message,
        parse_mode: 'Markdown',
        disable_web_page_preview: true,
        disable_notification: false
      })
    });
    
    if (response.ok) {
      return true;
    } else {
      return false;
    }
  } catch (error) {
    logError('Telegram通知发送异常', error, requestId);
    return false;
  }
}

// 发送服务状态变化通知
async function sendServiceStatusNotification(isAvailable, backendUrl, requestId, env) {
  const botToken = getConfig(env, 'TG_BOT_TOKEN', '');
  const chatId = getConfig(env, 'TG_CHAT_ID', '');
  
  if (!botToken || !chatId) {
    return false;
  }
  
  try {
    // 获取北京时间（修复时间转换）
    const now = new Date();
    const beijingTime = new Date(now.getTime() + 8 * 60 * 60 * 1000);
    const beijingTimeStr = beijingTime.toISOString().replace('T', ' ').substring(0, 19);
    
    let message;
    if (isAvailable) {
      message = `🟢 *服务恢复通知*\n\n`;
      message += `🎉 订阅转换服务已恢复可用\n`;
      message += `⏰ 时间: ${beijingTimeStr} (北京时间)\n`;
      message += `🚀 可用后端: \`${backendUrl}\`\n`;
      message += `✅ 服务已恢复正常，可以继续使用`;
    } else {
      message = `🔴 *服务中断通知*\n\n`;
      message += `⚠️ 订阅转换服务当前不可用\n`;
      message += `⏰ 时间: ${beijingTimeStr} (北京时间)\n`;
      message += `❌ 所有后端服务器均不可用\n`;
      message += `🚨 服务已中断，请及时检查`;
    }
    
    // 发送到Telegram
    const telegramUrl = `https://api.telegram.org/bot${botToken}/sendMessage`;
    const response = await fetch(telegramUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        chat_id: chatId,
        text: message,
        parse_mode: 'Markdown',
        disable_web_page_preview: true,
        disable_notification: true
      })
    });
    
    if (response.ok) {
      return true;
    } else {
      return false;
    }
  } catch (error) {
    logError('服务状态通知发送异常', error, requestId);
    return false;
  }
}

// API端点处理
async function handleApiRequest(request, env, requestId) {
  const url = new URL(request.url);
  const kv = env.SUB_BACKENDS;
  
  // 健康检查API
  if (url.pathname === '/api/health' && request.method === 'GET') {
    try {
      const backends = await getBackends(env, requestId);
      const health = await getHealthStatus(kv, requestId, env);
      const lastAvailable = await getLastAvailableBackend(kv, requestId);
      
      const healthyCount = Object.values(health).filter(h => h.healthy).length;
      const totalCount = backends.length;
      
      return new Response(JSON.stringify({
        status: 'ok',
        timestamp: new Date().toISOString(),
        backends_count: totalCount,
        healthy_backends: healthyCount,
        unhealthy_backends: totalCount - healthyCount,
        last_available_backend: lastAvailable,
        backends: backends.map(url => ({
          url,
          health: health[url] || { healthy: null },
          weight: cache.backendWeights.get(url) || getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT),
          failure_count: cache.backendFailureCounts.get(url) || 0,
          request_count: cache.requestCounts.get(url) || 0,
          last_success: cache.lastSuccessfulRequests.get(url) || null
        })),
        kv_writes_saved: cache.lastKVWriteTimes.size,
        performance_stats: cache.performanceStats
      }), {
        headers: { 
          'Content-Type': 'application/json; charset=utf-8',
          'Cache-Control': 'no-cache'
        }
      });
    } catch (error) {
      return new Response(JSON.stringify({ 
        error: error.message,
        request_id: requestId
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    }
  }
  
  // 手动触发健康检查API
  if (url.pathname === '/api/health-check' && request.method === 'POST') {
    try {
      const checkResults = await performFullHealthCheck(kv, requestId, env);
      
      // 发送Telegram通知（只在状态变化时）
      const botToken = getConfig(env, 'TG_BOT_TOKEN', '');
      const chatId = getConfig(env, 'TG_CHAT_ID', '');
      if (botToken && chatId) {
        // 检查服务状态是否变化
        if (hasServiceStatusChanged(checkResults)) {
          await sendTelegramNotification(checkResults, requestId, env);
        }
      }
      
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        results: checkResults.results,
        available_backend: checkResults.availableBackend,
        fastest_response_time: checkResults.fastestResponseTime,
        timestamp: checkResults.timestamp,
        kv_write_optimized: true
      }), {
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    } catch (error) {
      return new Response(JSON.stringify({ 
        error: error.message,
        request_id: requestId
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    }
  }
  
  // 查看当前配置的后端列表
  if (url.pathname === '/api/config' && request.method === 'GET') {
    try {
      const backends = await getBackends(env, requestId);
      
      return new Response(JSON.stringify({ 
        backends,
        config: {
          cache_ttl: getConfig(env, 'CACHE_TTL', DEFAULT_CACHE_TTL),
          health_check_timeout: getConfig(env, 'HEALTH_CHECK_TIMEOUT', DEFAULT_HEALTH_CHECK_TIMEOUT),
          concurrent_health_checks: getConfig(env, 'CONCURRENT_HEALTH_CHECKS', DEFAULT_CONCURRENT_HEALTH_CHECKS),
          fast_check_timeout: getConfig(env, 'FAST_CHECK_TIMEOUT', DEFAULT_FAST_CHECK_TIMEOUT),
          fast_check_cache_ttl: getConfig(env, 'FAST_CHECK_CACHE_TTL', DEFAULT_FAST_CHECK_CACHE_TTL),
          kv_write_cooldown: getConfig(env, 'KV_WRITE_COOLDOWN', DEFAULT_KV_WRITE_COOLDOWN),
          max_weight: getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT),
          min_weight: getConfig(env, 'MIN_WEIGHT', DEFAULT_MIN_WEIGHT),
          weight_recovery_rate: getConfig(env, 'WEIGHT_RECOVERY_RATE', DEFAULT_WEIGHT_RECOVERY_RATE),
          failure_weight_decrement: getConfig(env, 'FAILURE_WEIGHT_DECREMENT', DEFAULT_FAILURE_WEIGHT_DECREMENT),
          backend_stale_threshold: getConfig(env, 'BACKEND_STALE_THRESHOLD', DEFAULT_BACKEND_STALE_THRESHOLD)
        },
        timestamp: new Date().toISOString()
      }), {
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    } catch (error) {
      return new Response(JSON.stringify({ 
        error: error.message,
        request_id: requestId
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    }
  }
  
  // 清理缓存API - 优化缓存重置逻辑
  if (url.pathname === '/api/clear-cache' && request.method === 'POST') {
    try {
      // 清理内存缓存（修复：重置lastServiceStatus为'unknown'）
      cache = {
        backends: null,
        lastUpdated: 0,
        healthStatus: null,
        healthLastUpdated: 0,
        lastAvailableBackend: null,
        backendVersions: new Map(),
        fastHealthChecks: new Map(),
        healthyBackendsList: [],
        healthyBackendsLastUpdated: 0,
        ipNotificationTimestamps: new Map(),
        ipNotificationBackends: new Map(),
        backendVersionCache: new Map(),
        lastKVWriteTimes: new Map(),
        lastHealthNotificationStatus: null,
        lastServiceStatus: 'unknown', // 修复：改为'unknown'
        backendWeights: new Map(),
        backendFailureCounts: new Map(),
        lastSuccessfulRequests: new Map(),
        weightedBackendCache: [],
        weightedCacheLastUpdated: 0,
        requestCounts: new Map(),
        errorLogs: [],
        performanceStats: {
          totalRequests: 0,
          successfulRequests: 0,
          failedRequests: 0,
          avgResponseTime: 0,
          lastResetTime: Date.now()
        }
      };
      
      // 清理KV中的健康状态
      await kv.put('health_status', JSON.stringify({}));
      await kv.put('last_available_backend', '');
      
      return new Response(JSON.stringify({
        success: true,
        message: '缓存已清理',
        request_id: requestId,
        timestamp: new Date().toISOString()
      }), {
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    } catch (error) {
      return new Response(JSON.stringify({ 
        error: error.message,
        request_id: requestId
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    }
  }
  
  // 性能测试API
  if (url.pathname === '/api/benchmark' && request.method === 'GET') {
    try {
      const backends = await getBackends(env, requestId);
      const results = {};
      
      // 测试每个后端的响应时间
      const testPromises = backends.map(async (url) => {
        const startTime = Date.now();
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 3000);
          
          const response = await fetch(`${url}/version`, {
            signal: controller.signal,
            headers: { 'User-Agent': 'subconverter-failover-benchmark/1.0' }
          });
          
          clearTimeout(timeoutId);
          const responseTime = Date.now() - startTime;
          
          let version = '未知版本';
          if (response.status === 200) {
            const text = await response.text();
            version = text.trim() || '未知版本';
          }
          
          results[url] = {
            status: response.status,
            responseTime,
            healthy: response.status === 200,
            version: version
          };
        } catch (error) {
          results[url] = {
            status: 0,
            responseTime: Date.now() - startTime,
            healthy: false,
            error: error.name,
            version: '未知版本'
          };
        }
      });
      
      await Promise.allSettled(testPromises);
      
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        benchmark_time: new Date().toISOString(),
        results
      }), {
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    } catch (error) {
      return new Response(JSON.stringify({ 
        error: error.message,
        request_id: requestId
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    }
  }
  
  // KV写入统计API
  if (url.pathname === '/api/kv-stats' && request.method === 'GET') {
    try {
      const MAX_WEIGHT = getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT);
      
      const stats = {
        last_write_times: Array.from(cache.lastKVWriteTimes.entries()).map(([key, time]) => ({
          key,
          time: new Date(time).toISOString(),
          ago: Date.now() - time
        })),
        total_writes_saved: cache.lastKVWriteTimes.size,
        optimization_enabled: true,
        kv_write_cooldown: getConfig(env, 'KV_WRITE_COOLDOWN', DEFAULT_KV_WRITE_COOLDOWN),
        current_service_status: cache.lastServiceStatus,
        backend_weights: Array.from(cache.backendWeights.entries()).map(([url, weight]) => ({
          url,
          weight,
          failure_count: cache.backendFailureCounts.get(url) || 0,
          request_count: cache.requestCounts.get(url) || 0
        })),
        performance_stats: cache.performanceStats,
        cache_sizes: {
          fast_health_checks: cache.fastHealthChecks.size,
          backend_versions: cache.backendVersions.size,
          healthy_backends: cache.healthyBackendsList.length,
          error_logs: cache.errorLogs.length
        }
      };
      
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        stats,
        timestamp: new Date().toISOString()
      }), {
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    } catch (error) {
      return new Response(JSON.stringify({ 
        error: error.message,
        request_id: requestId
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    }
  }
  
  // 错误日志API
  if (url.pathname === '/api/error-logs' && request.method === 'GET') {
    try {
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        error_logs: cache.errorLogs.slice(-50), // 返回最近50条错误日志
        total_errors: cache.errorLogs.length,
        timestamp: new Date().toISOString()
      }), {
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    } catch (error) {
      return new Response(JSON.stringify({ 
        error: error.message,
        request_id: requestId
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    }
  }
  
  // 重置权重API
  if (url.pathname === '/api/reset-weights' && request.method === 'POST') {
    try {
      const backends = await getBackends(env, requestId);
      const MAX_WEIGHT = getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT);
      
      // 重置所有后端权重
      for (const backend of backends) {
        cache.backendWeights.set(backend, MAX_WEIGHT);
        cache.backendFailureCounts.set(backend, 0);
        cache.requestCounts.set(backend, 0);
        cache.lastSuccessfulRequests.set(backend, Date.now());
      }
      
      // 重置加权缓存
      cache.weightedBackendCache = [];
      cache.weightedCacheLastUpdated = 0;
      
      return new Response(JSON.stringify({
        success: true,
        message: '后端权重已重置',
        request_id: requestId,
        timestamp: new Date().toISOString()
      }), {
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    } catch (error) {
      return new Response(JSON.stringify({ 
        error: error.message,
        request_id: requestId
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    }
  }
  
  return new Response(JSON.stringify({ error: '未找到API端点' }), {
    status: 404,
    headers: { 'Content-Type': 'application/json; charset=utf-8' }
  });
}

// 简单的状态页面
function createStatusPage(requestId, backends, health, availableBackend, env) {
  const healthyCount = Object.values(health).filter(h => h.healthy).length;
  const totalCount = backends.length;
  const status = availableBackend ? '🟢 正常运行' : totalCount > 0 ? '🔴 服务异常' : '⚪ 未配置';
  
  // 获取北京时间（修复时间转换）
  const now = new Date();
  const beijingTime = new Date(now.getTime() + 8 * 60 * 60 * 1000);
  const beijingTimeStr = beijingTime.toISOString().replace('T', ' ').substring(0, 19);
  
  const MAX_WEIGHT = getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT);
  const MIN_WEIGHT = getConfig(env, 'MIN_WEIGHT', DEFAULT_MIN_WEIGHT);
  const KV_WRITE_COOLDOWN = getConfig(env, 'KV_WRITE_COOLDOWN', DEFAULT_KV_WRITE_COOLDOWN);
  
  const html = `
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>订阅转换服务状态</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 20px;
        }
        .status-container {
            background: white;
            padding: 2rem;
            border-radius: 10px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            width: 100%;
            max-width: 1000px;
        }
        h1 {
            color: #333;
            margin-bottom: 1.5rem;
            text-align: center;
            font-weight: 300;
        }
        .status-header {
            text-align: center;
            margin-bottom: 2rem;
        }
        .status-badge {
            display: inline-block;
            padding: 8px 16px;
            border-radius: 20px;
            font-weight: 500;
            font-size: 14px;
            margin-bottom: 10px;
        }
        .status-healthy { background: #d4edda; color: #155724; }
        .status-unhealthy { background: #f8d7da; color: #721c24; }
        .status-unconfigured { background: #e2e3e5; color: #383d41; }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 15px;
            margin-bottom: 2rem;
        }
        .stat-card {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            text-align: center;
        }
        .stat-value {
            font-size: 24px;
            font-weight: bold;
            color: #2c3e50;
            margin-bottom: 5px;
        }
        .stat-label {
            font-size: 12px;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .current-backend {
            background: #e7f5ff;
            border: 1px solid #bbdefb;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 2rem;
        }
        .current-backend h3 {
            color: #1971c2;
            margin-bottom: 10px;
        }
        .backend-url {
            font-family: monospace;
            font-size: 14px;
            color: #495057;
            word-break: break-all;
        }
        .backends-list {
            margin-bottom: 2rem;
        }
        .backend-item {
            display: flex;
            align-items: center;
            padding: 12px;
            border: 1px solid #e9ecef;
            border-radius: 6px;
            margin-bottom: 8px;
            background: #fff;
        }
        .health-indicator {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 12px;
        }
        .health-up { background: #28a745; }
        .health-down { background: #dc3545; }
        .health-unknown { background: #ffc107; }
        .backend-info {
            flex: 1;
        }
        .backend-meta {
            font-size: 12px;
            color: #6c757d;
            margin-top: 2px;
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
        }
        .meta-item {
            display: flex;
            align-items: center;
            gap: 4px;
        }
        .weight-badge {
            display: inline-block;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: bold;
        }
        .weight-high { background: #d4edda; color: #155724; }
        .weight-medium { background: #fff3cd; color: #856404; }
        .weight-low { background: #f8d7da; color: #721c24; }
        .footer {
            text-align: center;
            color: #6c757d;
            font-size: 12px;
            margin-top: 2rem;
        }
        .request-id {
            font-family: monospace;
            font-size: 11px;
            color: #adb5bd;
        }
        .time-info {
            margin-bottom: 10px;
            text-align: center;
            color: #495057;
            font-size: 14px;
        }
        .info-section {
            background: #f8f9fa;
            border: 1px solid #e9ecef;
            padding: 15px;
            border-radius: 8px;
            margin-top: 20px;
        }
        .info-section h3 {
            color: #495057;
            margin-bottom: 10px;
            font-weight: 400;
        }
        .info-section ul {
            margin-left: 20px;
            color: #6c757d;
            font-size: 14px;
        }
        .info-section code {
            background: #e9ecef;
            padding: 2px 6px;
            border-radius: 4px;
            font-family: monospace;
        }
        .api-links {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-top: 20px;
        }
        .api-link {
            background: #007bff;
            color: white;
            padding: 8px 16px;
            border-radius: 4px;
            text-decoration: none;
            font-size: 14px;
            transition: background 0.3s;
        }
        .api-link:hover {
            background: #0056b3;
        }
        .backend-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 5px;
        }
        .backend-name {
            font-weight: 500;
            color: #495057;
        }
        .performance-stats {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 10px;
            margin-bottom: 20px;
        }
        .perf-stat {
            background: #e9ecef;
            padding: 10px;
            border-radius: 6px;
            text-align: center;
        }
        .perf-label {
            font-size: 11px;
            color: #6c757d;
            text-transform: uppercase;
        }
        .perf-value {
            font-size: 18px;
            font-weight: bold;
            color: #495057;
        }
    </style>
</head>
<body>
    <div class="status-container">
        <h1>🚀 订阅转换后端状态 (优化版)</h1>
        
        <div class="time-info">
            北京时间: ${beijingTimeStr}
        </div>
        
        <div class="status-header">
            <div class="status-badge ${availableBackend ? 'status-healthy' : (totalCount > 0 ? 'status-unhealthy' : 'status-unconfigured')}">
                ${status}
            </div>
        </div>
        
        <div class="performance-stats">
            <div class="perf-stat">
                <div class="perf-value">${cache.performanceStats.totalRequests}</div>
                <div class="perf-label">总请求数</div>
            </div>
            <div class="perf-stat">
                <div class="perf-value">${cache.performanceStats.successfulRequests}</div>
                <div class="perf-label">成功请求</div>
            </div>
            <div class="perf-stat">
                <div class="perf-value">${Math.round(cache.performanceStats.avgResponseTime)}ms</div>
                <div class="perf-label">平均响应</div>
            </div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">${totalCount}</div>
                <div class="stat-label">总后端数</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">${healthyCount}</div>
                <div class="stat-label">健康后端</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">${cache.lastKVWriteTimes.size}</div>
                <div class="stat-label">KV写入节省</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">${cache.errorLogs.length}</div>
                <div class="stat-label">错误日志</div>
            </div>
        </div>
        
        ${availableBackend ? `
        <div class="current-backend">
            <h3>当前使用后端</h3>
            <div class="backend-url">${availableBackend}</div>
            ${health[availableBackend]?.version && health[availableBackend].version !== '未知版本' ? `
            <div class="backend-meta">
                <span class="meta-item">版本: ${health[availableBackend].version}</span>
                ${health[availableBackend]?.responseTime ? `<span class="meta-item">响应时间: ${health[availableBackend].responseTime}ms</span>` : ''}
                <span class="meta-item">权重: <span class="weight-badge ${getWeightClass(cache.backendWeights.get(availableBackend) || MAX_WEIGHT)}">${cache.backendWeights.get(availableBackend) || MAX_WEIGHT}</span></span>
                <span class="meta-item">请求数: ${cache.requestCounts.get(availableBackend) || 0}</span>
            </div>
            ` : ''}
        </div>
        ` : totalCount > 0 ? `
        <div class="current-backend" style="background: #f8d7da; border-color: #f5c6cb;">
            <h3 style="color: #721c24;">⚠️ 服务异常</h3>
            <div>所有后端服务器均不可用，服务已中断</div>
        </div>
        ` : `
        <div class="current-backend" style="background: #e2e3e5; border-color: #d6d8db;">
            <h3 style="color: #383d41;">⚪ 未配置</h3>
            <div>尚未配置后端服务器，请在Cloudflare Dashboard中配置BACKEND_URLS</div>
        </div>
        `}
        
        ${totalCount > 0 ? `
        <div class="backends-list">
            <h3 style="margin-bottom: 10px; color: #495057; font-weight: 400;">后端状态详情</h3>
            ${backends.map(url => {
              const status = health[url] || { healthy: null };
              const weight = cache.backendWeights.get(url) || MAX_WEIGHT;
              const failureCount = cache.backendFailureCounts.get(url) || 0;
              const requestCount = cache.requestCounts.get(url) || 0;
              const lastSuccess = cache.lastSuccessfulRequests.get(url);
              const lastSuccessTime = lastSuccess ? Math.round((Date.now() - lastSuccess) / 1000) : '从未';
              
              const statusClass = status.healthy === true ? 'health-up' : 
                                status.healthy === false ? 'health-down' : 'health-unknown';
              const statusText = status.healthy === true ? '正常' : 
                                status.healthy === false ? '异常' : '未知';
              
              // 修复：将UTC时间转换为北京时间
              let timestamp = '从未检查';
              if (status.timestamp) {
                const utcDate = new Date(status.timestamp);
                const beijingDate = new Date(utcDate.getTime() + 8 * 60 * 60 * 1000);
                timestamp = beijingDate.toLocaleTimeString('zh-CN');
              }
              
              return `
              <div class="backend-item">
                  <div class="health-indicator ${statusClass}"></div>
                  <div class="backend-info">
                      <div class="backend-header">
                          <div class="backend-name">${url}</div>
                          <span class="weight-badge ${getWeightClass(weight)}">权重: ${weight}</span>
                      </div>
                      <div class="backend-meta">
                          <span class="meta-item">状态: ${statusText}</span>
                          <span class="meta-item">最后检查: ${timestamp}</span>
                          ${status.responseTime ? `<span class="meta-item">响应: ${status.responseTime}ms</span>` : ''}
                          <span class="meta-item">失败: ${failureCount}</span>
                          <span class="meta-item">请求: ${requestCount}</span>
                          <span class="meta-item">最后成功: ${lastSuccessTime}秒前</span>
                          ${status.version && status.version !== '未知版本' ? `<span class="meta-item">版本: ${status.version.substring(0, 30)}</span>` : ''}
                          ${status.error ? `<span class="meta-item" style="color: #dc3545;">错误: ${status.error}</span>` : ''}
                      </div>
                  </div>
              </div>`;
            }).join('')}
        </div>
        ` : ''}
        
        <div class="info-section" style="background: #d4edda; border-color: #c3e6cb;">
            <h3 style="color: #155724;">💾 KV写入优化</h3>
            <ul>
                <li>写入节流: ${KV_WRITE_COOLDOWN/1000}秒内不重复写入相同数据</li>
                <li>状态变化检测: 仅当健康状态真正变化时才写入KV</li>
                <li>内存缓存优先: 使用内存缓存减少KV读取次数</li>
                <li>预估减少: KV写入次数减少90%以上</li>
                <li>已节省写入: ${cache.lastKVWriteTimes.size}次</li>
            </ul>
        </div>
        
        <div class="info-section" style="background: #fff3cd; border-color: #ffeaa7;">
            <h3 style="color: #856404;">⚡ 智能缓存与加权轮询</h3>
            <ul>
                <li>智能缓存失效: 基于后端响应状态动态调整缓存时间</li>
                <li>加权轮询算法: 根据后端健康状况分配权重 (${MIN_WEIGHT}-${MAX_WEIGHT})</li>
                <li>自动故障转移: 失败后端权重降低，健康后端权重恢复</li>
                <li>响应时间优化: 优先选择响应最快、最稳定的后端</li>
                <li>内存缓存清理: 自动清理过期缓存，防止内存泄漏</li>
            </ul>
        </div>
        
        <div class="info-section" style="background: #d1ecf1; border-color: #bee5eb;">
            <h3 style="color: #0c5460;">🔔 通知与监控系统</h3>
            <ul>
                <li>定时健康检查: 每10分钟执行一次，只在状态变化时发送报告</li>
                <li>请求耗时统计: 显示后端选择耗时和请求响应耗时</li>
                <li>服务状态监控: 实时监控服务可用性，自动发送状态变化通知</li>
                <li>错误日志记录: 自动记录错误信息，便于问题排查</li>
                <li>性能统计: 实时统计请求成功率、平均响应时间等指标</li>
            </ul>
        </div>
        
        <div class="api-links">
            <a href="/api/health" class="api-link">健康状态API</a>
            <a href="/api/config" class="api-link">配置信息</a>
            <a href="/api/kv-stats" class="api-link">KV统计</a>
            <a href="/api/benchmark" class="api-link">性能测试</a>
            <a href="/api/error-logs" class="api-link">错误日志</a>
            <a href="/api/reset-weights" class="api-link" onclick="return confirm('确定要重置所有后端权重吗？')">重置权重</a>
        </div>
        
        <div class="footer">
            <div>请求ID: <span class="request-id">${requestId}</span></div>
            <div>最后更新: ${new Date().toLocaleString('zh-CN')}</div>
            <div>当前服务状态: ${availableBackend ? 'available' : 'unavailable'}</div>
            <div>性能统计重置时间: ${new Date(cache.performanceStats.lastResetTime).toLocaleString('zh-CN')}</div>
        </div>
    </div>
    
    <script>
        function getWeightClass(weight) {
            if (weight >= 70) return 'weight-high';
            if (weight >= 40) return 'weight-medium';
            return 'weight-low';
        }
        
        // 自动刷新页面（每30秒）
        setTimeout(() => {
            window.location.reload();
        }, 30000);
    </script>
</body>
</html>`;
  
  return new Response(html, {
    headers: { 
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-cache, no-store, must-revalidate'
    }
  });
}

// 辅助函数：获取权重类名
function getWeightClass(weight) {
  if (weight >= 70) return 'weight-high';
  if (weight >= 40) return 'weight-medium';
  return 'weight-low';
}

// 主处理函数
export default {
  async fetch(request, env, ctx) {
    const requestId = generateRequestId();
    const url = new URL(request.url);
    
    console.log(`[${requestId}] 收到请求: ${request.method} ${url.pathname}`);
    
    // 验证配置
    validateConfig(env, requestId);
    
    // 执行缓存清理
    cleanupExpiredCache(env);
    
    // 更新总请求数
    cache.performanceStats.totalRequests++;
    
    // 根路径显示状态页面
    if (url.pathname === '/' || url.pathname === '/status') {
      try {
        const kv = env.SUB_BACKENDS;
        const backends = await getBackends(env, requestId);
        const health = await getHealthStatus(kv, requestId, env);
        const availableBackend = await getLastAvailableBackend(kv, requestId);
        
        return createStatusPage(requestId, backends, health, availableBackend, env);
      } catch (error) {
        logError('创建状态页面失败', error, requestId);
        return new Response('服务状态页面暂时不可用', {
          status: 500,
          headers: { 'Content-Type': 'text/plain; charset=utf-8' }
        });
      }
    }
    
    // API路由
    if (url.pathname.startsWith('/api/')) {
      return handleApiRequest(request, env, requestId);
    }
    
    // 订阅转换请求
    try {
      const kv = env.SUB_BACKENDS;
      
      // 检查是否有后端配置
      const backends = await getBackends(env, requestId);
      if (backends.length === 0) {
        return new Response('未配置后端服务器，请在Cloudflare Dashboard中配置BACKEND_URLS', {
          status: 503,
          headers: { 
            'Content-Type': 'text/plain; charset=utf-8',
            'Cache-Control': 'no-cache',
            'X-Request-ID': requestId
          }
        });
      }
      
      const previousAvailableBackend = await getLastAvailableBackend(kv, requestId);
      
      // 查找可用后端（包含耗时统计）
      const { backend: backendUrl, selectionTime: backendSelectionTime } = await findAvailableBackendForRequest(kv, requestId, env);
      
      if (!backendUrl) {
        console.log(`[${requestId}] 无可用后端，返回503`);
        
        // 发送服务不可用通知（如果之前是可用的）
        if (previousAvailableBackend) {
          const botToken = getConfig(env, 'TG_BOT_TOKEN', '');
          const chatId = getConfig(env, 'TG_CHAT_ID', '');
          if (botToken && chatId) {
            ctx.waitUntil(sendServiceStatusNotification(false, null, requestId, env));
          }
        }
        
        return new Response('所有后端服务均不可用，请稍后重试', {
          status: 503,
          headers: { 
            'Content-Type': 'text/plain; charset=utf-8',
            'Cache-Control': 'no-cache',
            'Retry-After': '30',
            'X-Request-ID': requestId,
            'X-Backend-Selection-Time': `${backendSelectionTime}ms`
          }
        });
      }
      
      console.log(`[${requestId}] 使用后端: ${backendUrl}, 后端选择耗时: ${backendSelectionTime}ms`);
      
      // 发送服务恢复通知（如果之前是不可用的）
      if (!previousAvailableBackend) {
        const botToken = getConfig(env, 'TG_BOT_TOKEN', '');
        const chatId = getConfig(env, 'TG_CHAT_ID', '');
        if (botToken && chatId) {
          ctx.waitUntil(sendServiceStatusNotification(true, backendUrl, requestId, env));
        }
      }
      
      const response = await handleSubconverterRequest(
        request, 
        backendUrl, 
        backendSelectionTime, 
        requestId, 
        env, 
        ctx
      );
      
      // 记录成功请求
      console.log(`[${requestId}] 请求处理完成，状态码: ${response.status}`);
      
      return response;
    } catch (error) {
      logError('处理请求失败', error, requestId);
      return new Response(`服务错误: ${error.message}`, {
        status: 500,
        headers: { 
          'Content-Type': 'text/plain; charset=utf-8',
          'Cache-Control': 'no-cache',
          'X-Request-ID': requestId
        }
      });
    }
  },
  
  // Cron触发器处理
  async scheduled(event, env, ctx) {
    const requestId = generateRequestId();
    console.log(`[${requestId}] Cron触发，开始执行健康检查`);
    
    try {
      const kv = env.SUB_BACKENDS;
      const checkResults = await performFullHealthCheck(kv, requestId, env);
      
      // 执行缓存清理
      cleanupExpiredCache(env);
      
      // 发送Telegram通知（只在状态变化时）
      const botToken = getConfig(env, 'TG_BOT_TOKEN', '');
      const chatId = getConfig(env, 'TG_CHAT_ID', '');
      if (botToken && chatId) {
        // 检查服务状态是否变化
        if (hasServiceStatusChanged(checkResults)) {
          console.log(`[${requestId}] 服务状态变化，发送Telegram通知`);
          ctx.waitUntil(sendTelegramNotification(checkResults, requestId, env));
        } else {
          console.log(`[${requestId}] 服务状态未变化，跳过Telegram通知`);
        }
      } else {
        console.log(`[${requestId}] Telegram通知未配置，跳过发送`);
      }
      
      console.log(`[${requestId}] Cron健康检查完成，KV写入已优化`);
    } catch (error) {
      logError('Cron健康检查失败', error, requestId);
    }
  }
};