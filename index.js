// 默认常量（可通过环境变量覆盖）
const DEFAULT_CACHE_TTL = 60 * 1000; // 健康状态缓存1分钟
const DEFAULT_HEALTH_CHECK_TIMEOUT = 2000; // 健康检查超时2秒
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

// 全局缓存对象（简化缓存，状态页面将主要从D1读取）
let cache = {
  backends: null,
  lastUpdated: 0,
  lastAvailableBackend: null,
  backendVersionCache: new Map(),
  fastHealthChecks: new Map(),
  ipNotificationTimestamps: new Map(),
  ipNotificationBackends: new Map(),
  lastHealthNotificationStatus: null,
  lastServiceStatus: 'unknown',
  lastAvailableBackendForStatus: null,
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
  },
  // 简化D1写入统计，状态页面从D1直接读取
  d1WriteStats: {
    dailyCount: 0,
    lastResetDate: null,
    totalCount: 0
  }
};

// D1数据库操作类
class D1Database {
  constructor(db) {
    this.db = db;
  }

  // 保存健康检查结果到D1
  async saveHealthCheckResult(data, requestId) {
    try {
      const stmt = this.db.prepare(`
        INSERT INTO health_check_results 
        (timestamp, beijing_time, results, available_backend, fastest_response_time, backend_changed)
        VALUES (?, ?, ?, ?, ?, ?)
      `);
      
      const result = await stmt.bind(
        data.timestamp || new Date().toISOString(),
        getBeijingTimeString(),
        JSON.stringify(data.results || {}),
        data.available_backend || null,
        data.fastest_response_time || 0,
        data.backend_changed ? 1 : 0
      ).run();
      
      return result;
    } catch (error) {
      console.error(`[${requestId}] 保存健康检查结果到D1失败:`, error);
      throw error;
    }
  }

  // 保存请求结果到D1
  async saveRequestResult(data, requestId) {
    try {
      const stmt = this.db.prepare(`
        INSERT INTO request_results 
        (request_id, client_ip, backend_url, backend_selection_time, response_time, status_code, success, timestamp, beijing_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      
      const result = await stmt.bind(
        data.request_id || requestId,
        data.client_ip || 'unknown',
        data.backend_url || '',
        data.backend_selection_time || 0,
        data.response_time || 0,
        data.status_code || 0,
        data.success ? 1 : 0,
        data.timestamp || new Date().toISOString(),
        getBeijingTimeString()
      ).run();
      
      return result;
    } catch (error) {
      console.error(`[${requestId}] 保存请求结果到D1失败:`, error);
      throw error;
    }
  }

  // 更新后端状态到D1
  async updateBackendStatus(backendUrl, data, requestId) {
    try {
      // 先检查是否存在
      const existing = await this.db
        .prepare('SELECT 1 FROM backend_status WHERE backend_url = ?')
        .bind(backendUrl)
        .first();
      
      if (existing) {
        // 更新现有记录
        const stmt = this.db.prepare(`
          UPDATE backend_status 
          SET healthy = ?, last_checked = ?, weight = ?, failure_count = ?, 
              request_count = ?, last_success = ?, version = ?, response_time = ?, updated_at = CURRENT_TIMESTAMP
          WHERE backend_url = ?
        `);
        
        await stmt.bind(
          data.healthy ? 1 : 0,
          data.last_checked || new Date().toISOString(),
          data.weight || 100,
          data.failure_count || 0,
          data.request_count || 0,
          data.last_success || null,
          data.version || '未知版本',
          data.response_time || 0,
          backendUrl
        ).run();
      } else {
        // 插入新记录
        const stmt = this.db.prepare(`
          INSERT INTO backend_status 
          (backend_url, healthy, last_checked, weight, failure_count, request_count, last_success, version, response_time)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        
        await stmt.bind(
          backendUrl,
          data.healthy ? 1 : 0,
          data.last_checked || new Date().toISOString(),
          data.weight || 100,
          data.failure_count || 0,
          data.request_count || 0,
          data.last_success || null,
          data.version || '未知版本',
          data.response_time || 0
        ).run();
      }
      
      return true;
    } catch (error) {
      console.error(`[${requestId}] 更新后端状态到D1失败:`, error);
      throw error;
    }
  }

  // 获取最近N次健康检查结果
  async getRecentHealthChecks(limit = 10) {
    try {
      const { results } = await this.db
        .prepare('SELECT * FROM health_check_results ORDER BY id DESC LIMIT ?')
        .bind(limit)
        .all();
      return results || [];
    } catch (error) {
      console.error('获取最近健康检查结果失败:', error);
      return [];
    }
  }

  // 获取最近N次请求结果
  async getRecentRequests(limit = 50) {
    try {
      const { results } = await this.db
        .prepare('SELECT * FROM request_results ORDER BY id DESC LIMIT ?')
        .bind(limit)
        .all();
      return results || [];
    } catch (error) {
      console.error('获取最近请求结果失败:', error);
      return [];
    }
  }

  // 获取所有后端最新状态
  async getAllBackendStatus() {
    try {
      const { results } = await this.db
        .prepare('SELECT * FROM backend_status ORDER BY updated_at DESC')
        .all();
      return results || [];
    } catch (error) {
      console.error('获取后端状态失败:', error);
      return [];
    }
  }

  // 获取D1写入统计
  async getD1WriteStats() {
    try {
      // 获取今日健康检查写入次数
      const today = getBeijingDateString(new Date());
      const healthCheckStmt = this.db
        .prepare('SELECT COUNT(*) as count FROM health_check_results WHERE date(created_at) = ?')
        .bind(today);
      
      const requestResultStmt = this.db
        .prepare('SELECT COUNT(*) as count FROM request_results WHERE date(created_at) = ?')
        .bind(today);
      
      const backendStatusStmt = this.db
        .prepare('SELECT COUNT(*) as count FROM backend_status WHERE date(created_at) = ?')
        .bind(today);
      
      const [healthCheckResult, requestResult, backendStatusResult] = await Promise.all([
        healthCheckStmt.first(),
        requestResultStmt.first(),
        backendStatusStmt.first()
      ]);
      
      // 获取总记录数
      const totalHealthChecks = await this.db
        .prepare('SELECT COUNT(*) as count FROM health_check_results')
        .first();
      
      const totalRequests = await this.db
        .prepare('SELECT COUNT(*) as count FROM request_results')
        .first();
      
      const totalBackendStatus = await this.db
        .prepare('SELECT COUNT(*) as count FROM backend_status')
        .first();
      
      return {
        today: {
          health_checks: healthCheckResult?.count || 0,
          request_results: requestResult?.count || 0,
          backend_status: backendStatusResult?.count || 0,
          total: (healthCheckResult?.count || 0) + (requestResult?.count || 0) + (backendStatusResult?.count || 0)
        },
        total: {
          health_checks: totalHealthChecks?.count || 0,
          request_results: totalRequests?.count || 0,
          backend_status: totalBackendStatus?.count || 0,
          total: (totalHealthChecks?.count || 0) + (totalRequests?.count || 0) + (totalBackendStatus?.count || 0)
        },
        beijing_date: today
      };
    } catch (error) {
      console.error('获取D1写入统计失败:', error);
      return null;
    }
  }

  // 获取状态页面数据（从D1读取最新数据）
  async getStatusPageData() {
    try {
      // 获取最近一次健康检查结果
      const recentHealthChecks = await this.getRecentHealthChecks(1);
      const latestCheck = recentHealthChecks.length > 0 ? recentHealthChecks[0] : null;
      
      // 获取最近请求统计
      const recentRequests = await this.getRecentRequests(100);
      
      // 获取所有后端状态
      const backendStatus = await this.getAllBackendStatus();
      
      // 获取D1写入统计
      const d1Stats = await this.getD1WriteStats();
      
      // 获取错误日志（如果有error_logs表）
      let errorLogs = [];
      try {
        const { results } = await this.db
          .prepare('SELECT * FROM error_logs ORDER BY id DESC LIMIT 50')
          .all();
        errorLogs = results || [];
      } catch (e) {
        // 如果error_logs表不存在，忽略
      }
      
      return {
        latestHealthCheck: latestCheck,
        recentRequests: recentRequests,
        backendStatus: backendStatus,
        d1Stats: d1Stats,
        errorLogs: errorLogs,
        timestamp: Date.now(),
        beijingTime: getBeijingTimeString()
      };
    } catch (error) {
      console.error('获取状态页面数据失败:', error);
      return null;
    }
  }

  // 清理旧数据（保留最近7天的数据）
  async cleanupOldData(daysToKeep = 7) {
    try {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);
      const cutoffStr = cutoffDate.toISOString();
      
      // 删除健康检查结果
      const healthCheckResult = await this.db
        .prepare('DELETE FROM health_check_results WHERE created_at < ?')
        .bind(cutoffStr)
        .run();
      
      // 删除请求结果
      const requestResult = await this.db
        .prepare('DELETE FROM request_results WHERE created_at < ?')
        .bind(cutoffStr)
        .run();
      
      // 删除后端状态记录
      const backendStatusResult = await this.db
        .prepare('DELETE FROM backend_status WHERE created_at < ?')
        .bind(cutoffStr)
        .run();
      
      console.log(`数据清理完成: 删除了 ${healthCheckResult.changes} 条健康检查记录, ${requestResult.changes} 条请求记录, ${backendStatusResult.changes} 条后端状态记录`);
      
      return {
        health_checks_deleted: healthCheckResult.changes,
        requests_deleted: requestResult.changes,
        backend_status_deleted: backendStatusResult.changes
      };
    } catch (error) {
      console.error('清理旧数据失败:', error);
      return null;
    }
  }
}

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

// 获取北京时间字符串
function getBeijingTimeString(date = new Date()) {
  try {
    // 使用toLocaleString并指定时区
    return date.toLocaleString('zh-CN', { 
      timeZone: 'Asia/Shanghai',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    });
  } catch (error) {
    return date.toISOString().replace('T', ' ').substring(0, 19) + ' (UTC)';
  }
}

// 获取北京时间字符串（短格式，仅时间）
function getBeijingTimeShort(date = new Date()) {
  try {
    return date.toLocaleTimeString('zh-CN', { 
      timeZone: 'Asia/Shanghai',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    });
  } catch (error) {
    return date.toISOString().substring(11, 19);
  }
}

// 获取北京日期字符串（YYYY-MM-DD格式）
function getBeijingDateString(date = new Date()) {
  try {
    const beijingDate = new Date(date.getTime() + 8 * 60 * 60 * 1000);
    const year = beijingDate.getUTCFullYear();
    const month = String(beijingDate.getUTCMonth() + 1).padStart(2, '0');
    const day = String(beijingDate.getUTCDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  } catch (error) {
    return date.toISOString().substring(0, 10);
  }
}

// 获取后端版本信息
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
    console.error(`[${requestId}] 获取后端版本失败: ${backendUrl}`, error);
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
function updateBackendWeight(backendUrl, success, env, responseTime = null) {
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
    [weightedList[i], weightedList[j]] = [weightedList[i], weightedList[j]];
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
  
  // 记录请求计数
  const requestCount = (cache.requestCounts.get(selected.url) || 0) + 1;
  cache.requestCounts.set(selected.url, requestCount);
  
  console.log(`[${requestId}] 加权选择后端: ${selected.url}, 权重: ${selected.weight}, 请求计数: ${requestCount}`);
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
  
  // 检查并重置D1写入统计（每日北京时间0点）
  const todayBeijing = getBeijingDateString(new Date());
  if (cache.d1WriteStats.lastResetDate !== todayBeijing) {
    console.log(`检测到日期变化，重置D1写入统计: ${cache.d1WriteStats.lastResetDate} -> ${todayBeijing}`);
    cache.d1WriteStats.dailyCount = 0;
    cache.d1WriteStats.lastResetDate = todayBeijing;
  }
}

// 错误日志记录
function logError(message, error, requestId) {
  const errorEntry = {
    timestamp: new Date().toISOString(),
    beijingTime: getBeijingTimeString(),
    requestId: requestId || 'unknown',
    message: message,
    error: error?.message || String(error),
    stack: error?.stack
  };
  
  cache.errorLogs.push(errorEntry);
  
  // 控制台输出简化版本
  console.error(`[${requestId || 'system'}] ${message}: ${error?.message || error}`);
}

// 极速健康检查
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
        updateBackendWeight(url, true, env, responseTime);
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

// 带缓存的详细健康检查
async function checkBackendHealth(url, requestId, env) {
  const cacheKey = `health_${url}`;
  const cached = cache.fastHealthChecks.get(cacheKey);
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
    cache.fastHealthChecks.delete(cacheKey);
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
      updateBackendWeight(url, healthy, env, responseTime);
      
      // 增加请求计数
      const requestCount = (cache.requestCounts.get(url) || 0) + 1;
      cache.requestCounts.set(url, requestCount);
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
    cache.fastHealthChecks.set(cacheKey, {
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
    cache.fastHealthChecks.set(cacheKey, {
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
    
    // 初始化后端权重和统计
    const MAX_WEIGHT = getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT);
    for (const backend of backends) {
      if (!cache.backendWeights.has(backend)) {
        cache.backendWeights.set(backend, MAX_WEIGHT);
      }
      if (!cache.backendFailureCounts.has(backend)) {
        cache.backendFailureCounts.set(backend, 0);
      }
      if (!cache.requestCounts.has(backend)) {
        cache.requestCounts.set(backend, 0);
      }
      if (!cache.lastSuccessfulRequests.has(backend)) {
        cache.lastSuccessfulRequests.set(backend, 0);
      }
    }
    
    return backends;
  } catch (error) {
    logError('获取后端列表失败', error, requestId);
    return cache.backends || DEFAULT_BACKENDS;
  }
}

// 获取上次可用后端
async function getLastAvailableBackend(db, requestId) {
  // 首先检查内存缓存
  if (cache.lastAvailableBackend) {
    return cache.lastAvailableBackend;
  }
  
  try {
    // 从D1获取最新的健康检查结果
    if (db) {
      try {
        const recentChecks = await db.getRecentHealthChecks(1);
        if (recentChecks.length > 0) {
          const latestCheck = recentChecks[0];
          if (latestCheck.available_backend) {
            cache.lastAvailableBackend = latestCheck.available_backend;
            return latestCheck.available_backend;
          }
        }
      } catch (dbError) {
        logError('从D1获取上次可用后端失败', dbError, requestId);
      }
    }
    
    return null;
  } catch (error) {
    logError('获取上次可用后端失败', error, requestId);
    return null;
  }
}

// 保存上次可用后端
async function saveLastAvailableBackend(db, backendUrl, requestId, env) {
  try {
    // 检查是否与当前值相同
    const currentBackend = cache.lastAvailableBackend;
    if (currentBackend === backendUrl) {
      console.log(`[${requestId}] 上次可用后端未变化，跳过更新`);
      return true;
    }
    
    cache.lastAvailableBackend = backendUrl;
    
    console.log(`[${requestId}] 上次可用后端已更新为: ${backendUrl}`);
    return true;
  } catch (error) {
    logError('保存上次可用后端失败', error, requestId);
    return false;
  }
}

// 并行极速健康检查
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

// 智能查找可用后端（订阅转换请求专用）
async function findAvailableBackendForRequest(db, requestId, env) {
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
        saveLastAvailableBackend(db, weightedBackend, `${requestId}-async-weighted`, env);
      }, 0);
      
      const selectionTime = Date.now() - selectionStartTime;
      return { backend: weightedBackend, selectionTime };
    }
  }
  
  // 策略1: 检查上次可用的后端（快速路径）
  const lastBackend = cache.lastAvailableBackend;
  if (lastBackend && backends.includes(lastBackend)) {
    const fastCheck = await ultraFastHealthCheck(lastBackend, requestId, env);
    if (fastCheck.healthy) {
      console.log(`[${requestId}] 使用上次可用后端: ${lastBackend}, 响应时间: ${fastCheck.responseTime}ms`);
      const selectionTime = Date.now() - selectionStartTime;
      return { backend: lastBackend, selectionTime };
    }
  }
  
  // 策略2: 并行极速检查所有后端
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
    
    // 异步保存到D1
    setTimeout(() => {
      saveLastAvailableBackend(db, fastestBackend, `${requestId}-async-fastest`, env);
    }, 0);
    
    const selectionTime = Date.now() - selectionStartTime;
    return { backend: fastestBackend, selectionTime };
  }
  
  // 策略3: 如果有部分后端返回了结果但标记为不健康，尝试其中一个作为最后手段
  console.log(`[${requestId}] 极速检查失败，尝试已返回的后端`);
  
  for (const [url, health] of checkResults.entries()) {
    // 即使健康检查失败，也可能后端实际可用
    if (health.status === 200) {
      console.log(`[${requestId}] 尝试状态码200的后端: ${url}`);
      
      // 异步保存到D1
      setTimeout(() => {
        saveLastAvailableBackend(db, url, `${requestId}-async-fallback`, env);
      }, 0);
      
      const selectionTime = Date.now() - selectionStartTime;
      return { backend: url, selectionTime };
    }
  }
  
  console.log(`[${requestId}] 所有后端均不可用`);
  const selectionTime = Date.now() - selectionStartTime;
  return { backend: null, selectionTime };
}

// 处理订阅转换请求
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
    const success = response.ok;
    
    console.log(`[${requestId}] 后端响应时间: ${responseTime}ms, 状态码: ${response.status}, 成功: ${success}`);
    
    // 更新性能统计
    cache.performanceStats.totalRequests++;
    cache.performanceStats.avgResponseTime = 
      (cache.performanceStats.avgResponseTime * (cache.performanceStats.totalRequests - 1) + responseTime) / 
      cache.performanceStats.totalRequests;
    
    // 更新后端权重和最后成功时间
    updateBackendWeight(backendUrl, success, env, responseTime);
    
    // 异步保存请求结果到D1
    if (env.DB) {
      const db = new D1Database(env.DB);
      const clientIp = request.headers.get('cf-connecting-ip') || 
                       request.headers.get('x-forwarded-for') || 
                       'unknown';
      
      const requestData = {
        backend_url: backendUrl,
        backend_selection_time: backendSelectionTime,
        response_time: responseTime,
        status_code: response.status,
        success: success,
        client_ip: clientIp
      };
      
      // 异步写入D1，不阻塞主响应
      ctx.waitUntil(db.saveRequestResult(requestData, requestId));
    }
    
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
    
    // 尝试记录失败的请求到D1
    if (env.DB) {
      try {
        const db = new D1Database(env.DB);
        const clientIp = request.headers.get('cf-connecting-ip') || 
                         request.headers.get('x-forwarded-for') || 
                         'unknown';
        
        const requestData = {
          backend_url: backendUrl,
          backend_selection_time: backendSelectionTime,
          response_time: 0,
          status_code: 0,
          success: false,
          client_ip: clientIp,
          error: error.message
        };
        
        ctx.waitUntil(db.saveRequestResult(requestData, `${requestId}-failed`));
      } catch (dbError) {
        // 忽略D1写入错误
      }
    }
    
    throw error;
  }
}

// 并发检查多个后端健康状态
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

// 检查当前使用后端是否发生变化
function hasAvailableBackendChanged(newAvailableBackend) {
  const previousBackend = cache.lastAvailableBackendForStatus;
  
  // 第一次检查，状态肯定变化了
  if (cache.lastServiceStatus === 'unknown') {
    return true;
  }
  
  // 检查后端是否发生变化
  return previousBackend !== newAvailableBackend;
}

// 执行完整健康检查（检查所有后端）
async function performFullHealthCheck(db, requestId, env) {
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
    
    // 更新后端状态到D1
    try {
      const backendData = {
        healthy: health.healthy,
        last_checked: new Date().toISOString(),
        weight: cache.backendWeights.get(url) || getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT),
        failure_count: cache.backendFailureCounts.get(url) || 0,
        request_count: cache.requestCounts.get(url) || 0,
        last_success: cache.lastSuccessfulRequests.get(url) > 0 ? 
          new Date(cache.lastSuccessfulRequests.get(url)).toISOString() : null,
        version: health.version || '未知版本',
        response_time: health.responseTime || 0
      };
      
      // 异步写入D1，不阻塞主流程
      if (db) {
        setTimeout(async () => {
          try {
            await db.updateBackendStatus(url, backendData, `${requestId}-async`);
          } catch (error) {
            logError(`异步更新后端状态失败: ${url}`, error, `${requestId}-async`);
          }
        }, 0);
      }
    } catch (error) {
      logError(`准备后端状态数据失败: ${url}`, error, requestId);
    }
  }
  
  // 检查当前使用后端是否发生变化
  const backendChanged = hasAvailableBackendChanged(fastestBackend);
  
  // 总是写入健康检查结果到D1（无论是否变化）
  if (db) {
    try {
      const healthCheckData = {
        results,
        available_backend: fastestBackend,
        fastest_response_time: fastestTime,
        backend_changed: backendChanged
      };
      
      await db.saveHealthCheckResult(healthCheckData, requestId);
      
      // 更新D1写入统计
      cache.d1WriteStats.dailyCount++;
      cache.d1WriteStats.totalCount++;
      
      console.log(`[${requestId}] 健康检查结果已保存到D1，可用后端: ${fastestBackend}`);
    } catch (error) {
      logError('保存健康检查结果到D1失败', error, requestId);
    }
  }
  
  // 更新状态变化记录
  cache.lastServiceStatus = fastestBackend ? 'available' : 'unavailable';
  cache.lastAvailableBackendForStatus = fastestBackend;
  
  console.log(`[${requestId}] 健康检查完成，发现最快后端: ${fastestBackend}, 响应时间: ${fastestTime}ms，已写入D1`);
  
  return {
    results,
    availableBackend: fastestBackend,
    fastestResponseTime: fastestTime,
    timestamp: new Date().toISOString(),
    backendChanged: backendChanged
  };
}

// API端点处理
async function handleApiRequest(request, env, requestId) {
  const url = new URL(request.url);
  const db = env.DB ? new D1Database(env.DB) : null;
  
  // 健康检查API
  if (url.pathname === '/api/health' && request.method === 'GET') {
    try {
      const backends = await getBackends(env, requestId);
      
      // 获取D1统计数据
      let d1Stats = null;
      let recentHealthChecks = [];
      let recentRequests = [];
      let backendStatus = [];
      
      if (db) {
        try {
          d1Stats = await db.getD1WriteStats();
          recentHealthChecks = await db.getRecentHealthChecks(5);
          recentRequests = await db.getRecentRequests(10);
          backendStatus = await db.getAllBackendStatus();
        } catch (dbError) {
          logError('获取D1数据失败', dbError, requestId);
        }
      }
      
      const totalCount = backends.length;
      
      // 计算健康后端数量
      let healthyCount = 0;
      for (const backend of backends) {
        const cached = cache.fastHealthChecks.get(`health_${backend}`);
        if (cached && cached.result && cached.result.healthy) {
          healthyCount++;
        }
      }
      
      return new Response(JSON.stringify({
        status: 'ok',
        timestamp: new Date().toISOString(),
        beijing_time: getBeijingTimeString(),
        backends_count: totalCount,
        healthy_backends: healthyCount,
        unhealthy_backends: totalCount - healthyCount,
        last_available_backend: cache.lastAvailableBackend,
        backends: backends.map(url => ({
          url,
          weight: cache.backendWeights.get(url) || getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT),
          failure_count: cache.backendFailureCounts.get(url) || 0,
          request_count: cache.requestCounts.get(url) || 0,
          last_success: cache.lastSuccessfulRequests.get(url) || 0,
          last_success_text: cache.lastSuccessfulRequests.get(url) && cache.lastSuccessfulRequests.get(url) > 0 ? 
            `${Math.round((Date.now() - cache.lastSuccessfulRequests.get(url)) / 1000)}秒前` : 
            '从未'
        })),
        d1_stats: {
          memory_stats: cache.d1WriteStats,
          database_stats: d1Stats
        },
        performance_stats: cache.performanceStats,
        d1_data_available: {
          recent_health_checks_count: recentHealthChecks.length,
          recent_requests_count: recentRequests.length,
          backend_status_count: backendStatus.length
        }
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
  
  // D1写入统计API
  if (url.pathname === '/api/d1-stats' && request.method === 'GET') {
    try {
      if (!db) {
        return new Response(JSON.stringify({ 
          error: 'D1数据库未配置',
          request_id: requestId
        }), {
          status: 503,
          headers: { 'Content-Type': 'application/json; charset=utf-8' }
        });
      }
      
      const d1Stats = await db.getD1WriteStats();
      const recentHealthChecks = await db.getRecentHealthChecks(20);
      const recentRequests = await db.getRecentRequests(50);
      const backendStatus = await db.getAllBackendStatus();
      
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        stats: {
          d1_write_stats: {
            memory_stats: cache.d1WriteStats,
            database_stats: d1Stats,
            today_beijing_date: getBeijingDateString(new Date())
          },
          recent_health_checks: recentHealthChecks,
          recent_requests: recentRequests.slice(0, 20),
          backend_status: backendStatus,
          table_counts: {
            health_check_results: recentHealthChecks.length,
            request_results: recentRequests.length,
            backend_status: backendStatus.length
          }
        },
        timestamp: new Date().toISOString(),
        beijing_time: getBeijingTimeString()
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
  
  // D1数据清理API
  if (url.pathname === '/api/cleanup-d1' && request.method === 'POST') {
    try {
      if (!db) {
        return new Response(JSON.stringify({ 
          error: 'D1数据库未配置',
          request_id: requestId
        }), {
          status: 503,
          headers: { 'Content-Type': 'application/json; charset=utf-8' }
        });
      }
      
      const params = url.searchParams;
      const daysToKeep = parseInt(params.get('days') || '7', 10);
      
      const result = await db.cleanupOldData(daysToKeep);
      
      return new Response(JSON.stringify({
        success: true,
        message: `D1数据清理完成，保留最近${daysToKeep}天的数据`,
        cleanup_result: result,
        request_id: requestId,
        timestamp: new Date().toISOString(),
        beijing_time: getBeijingTimeString()
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
  
  // 手动触发健康检查API
  if (url.pathname === '/api/health-check' && request.method === 'POST') {
    try {
      const checkResults = await performFullHealthCheck(db, requestId, env);
      
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        results: checkResults.results,
        available_backend: checkResults.availableBackend,
        fastest_response_time: checkResults.fastestResponseTime,
        timestamp: checkResults.timestamp,
        beijing_time: getBeijingTimeString(new Date(checkResults.timestamp)),
        backend_changed: checkResults.backendChanged,
        d1_write_success: !!db
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

  // 立即执行健康检查并返回数据的API
  if (url.pathname === '/api/health-check-immediate' && request.method === 'GET') {
    try {
      // 立即执行健康检查
      const checkResults = await performFullHealthCheck(db, requestId, env);
    
      // 获取最新的D1统计数据
      let d1Stats = null;
      if (db) {
        d1Stats = await db.getD1WriteStats();
      }
    
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        message: '健康检查已完成并写入D1数据库',
        results: checkResults.results,
        available_backend: checkResults.availableBackend,
        fastest_response_time: checkResults.fastestResponseTime,
        backend_changed: checkResults.backendChanged,
        d1_stats: d1Stats,
        timestamp: new Date().toISOString(),
        beijing_time: getBeijingTimeString()
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
          max_weight: getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT),
          min_weight: getConfig(env, 'MIN_WEIGHT', DEFAULT_MIN_WEIGHT),
          weight_recovery_rate: getConfig(env, 'WEIGHT_RECOVERY_RATE', DEFAULT_WEIGHT_RECOVERY_RATE),
          failure_weight_decrement: getConfig(env, 'FAILURE_WEIGHT_DECREMENT', DEFAULT_FAILURE_WEIGHT_DECREMENT),
          backend_stale_threshold: getConfig(env, 'BACKEND_STALE_THRESHOLD', DEFAULT_BACKEND_STALE_THRESHOLD)
        },
        timestamp: new Date().toISOString(),
        beijing_time: getBeijingTimeString()
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
  
  // 清理缓存API
  if (url.pathname === '/api/clear-cache' && request.method === 'POST') {
    try {
      // 清理内存缓存
      cache = {
        backends: null,
        lastUpdated: 0,
        lastAvailableBackend: null,
        backendVersionCache: new Map(),
        fastHealthChecks: new Map(),
        ipNotificationTimestamps: new Map(),
        ipNotificationBackends: new Map(),
        lastHealthNotificationStatus: null,
        lastServiceStatus: 'unknown',
        lastAvailableBackendForStatus: null,
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
        },
        d1WriteStats: {
          dailyCount: 0,
          lastResetDate: getBeijingDateString(new Date()),
          totalCount: 0
        }
      };
      
      return new Response(JSON.stringify({
        success: true,
        message: '缓存已清理',
        request_id: requestId,
        timestamp: new Date().toISOString(),
        beijing_time: getBeijingTimeString()
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
        beijing_time: getBeijingTimeString(),
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
  
  // D1写入统计API
  if (url.pathname === '/api/kv-stats' && request.method === 'GET') {
    try {
      // 获取D1统计
      let d1Stats = null;
      let tableStats = null;
      
      if (env.DB) {
        d1Stats = await db.getD1WriteStats();
        const recentHealthChecks = await db.getRecentHealthChecks(5);
        const recentRequests = await db.getRecentRequests(10);
        tableStats = {
          health_check_results: recentHealthChecks.length,
          request_results: recentRequests.length
        };
      }
      
      const MAX_WEIGHT = getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT);
      
      const stats = {
        d1_write_stats: {
          memory_stats: cache.d1WriteStats,
          database_stats: d1Stats,
          table_stats: tableStats,
          today_beijing_date: getBeijingDateString(new Date())
        },
        backend_weights: Array.from(cache.backendWeights.entries()).map(([url, weight]) => ({
          url,
          weight,
          failure_count: cache.backendFailureCounts.get(url) || 0,
          request_count: cache.requestCounts.get(url) || 0,
          last_success: cache.lastSuccessfulRequests.get(url) || 0,
          last_success_text: cache.lastSuccessfulRequests.get(url) && cache.lastSuccessfulRequests.get(url) > 0 ? 
            `${Math.round((Date.now() - cache.lastSuccessfulRequests.get(url)) / 1000)}秒前` : 
            '从未'
        })),
        performance_stats: cache.performanceStats,
        cache_sizes: {
          fast_health_checks: cache.fastHealthChecks.size,
          backend_versions: cache.backendVersionCache.size,
          error_logs: cache.errorLogs.length
        }
      };
      
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        stats,
        timestamp: new Date().toISOString(),
        beijing_time: getBeijingTimeString(),
        note: '状态数据直接从D1数据库读取，缓存仅用于性能优化'
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
        error_logs: cache.errorLogs.slice(-50),
        total_errors: cache.errorLogs.length,
        timestamp: new Date().toISOString(),
        beijing_time: getBeijingTimeString()
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
  if (url.pathname === '/api/reset-weights' && (request.method === 'POST' || request.method === 'GET')) {
    try {
      // 如果是GET请求，检查是否有confirm参数
      if (request.method === 'GET') {
        const params = url.searchParams;
        const confirmReset = params.get('confirm');
        
        if (confirmReset !== 'true') {
          return new Response(JSON.stringify({
            error: '请使用POST请求或添加confirm=true参数',
            message: '重置权重需要使用POST请求。您也可以添加?confirm=true参数来确认操作。',
            request_id: requestId
          }), {
            status: 405,
            headers: { 'Content-Type': 'application/json; charset=utf-8' }
          });
        }
      }
      
      const backends = await getBackends(env, requestId);
      const MAX_WEIGHT = getConfig(env, 'MAX_WEIGHT', DEFAULT_MAX_WEIGHT);
      
      // 重置所有后端权重
      const resetResults = [];
      for (const backend of backends) {
        const oldWeight = cache.backendWeights.get(backend) || MAX_WEIGHT;
        const oldFailures = cache.backendFailureCounts.get(backend) || 0;
        const oldRequests = cache.requestCounts.get(backend) || 0;
        
        cache.backendWeights.set(backend, MAX_WEIGHT);
        cache.backendFailureCounts.set(backend, 0);
        cache.requestCounts.set(backend, 0);
        cache.lastSuccessfulRequests.set(backend, Date.now());
        
        resetResults.push({
          url: backend,
          old_weight: oldWeight,
          old_failures: oldFailures,
          old_requests: oldRequests,
          new_weight: MAX_WEIGHT,
          reset_time: new Date().toISOString()
        });
      }
      
      // 重置加权缓存
      cache.weightedBackendCache = [];
      cache.weightedCacheLastUpdated = 0;
      
      console.log(`[${requestId}] 权重已重置，共重置 ${backends.length} 个后端`);
      
      return new Response(JSON.stringify({
        success: true,
        message: `后端权重已重置，共重置 ${backends.length} 个后端`,
        backends_reset: backends.length,
        reset_results: resetResults,
        request_id: requestId,
        timestamp: new Date().toISOString(),
        beijing_time: getBeijingTimeString()
      }), {
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      });
    } catch (error) {
      logError('重置权重失败', error, requestId);
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

// 创建状态页面（从D1数据库读取数据）
async function createStatusPage(requestId, env) {
  const db = env.DB ? new D1Database(env.DB) : null;
  
  if (!db) {
    return new Response('D1数据库未配置，无法显示状态页面', {
      status: 503,
      headers: { 'Content-Type': 'text/html; charset=utf-8' }
    });
  }
  
  try {
    // 从D1读取最新数据
    const statusData = await db.getStatusPageData();
    
    if (!statusData) {
      return new Response('无法从D1数据库读取状态数据', {
        status: 500,
        headers: { 'Content-Type': 'text/html; charset=utf-8' }
      });
    }
    
    const {
      latestHealthCheck,
      recentRequests,
      backendStatus,
      d1Stats,
      errorLogs,
      timestamp,
      beijingTime
    } = statusData;
    
    // 获取后端列表
    const backends = getBackendsFromEnv(env);
    const totalBackends = backends.length;
    
    // 计算统计信息
    const totalRequests = recentRequests.length;
    const successfulRequests = recentRequests.filter(req => req.success).length;
    const failedRequests = totalRequests - successfulRequests;
    
    // 从健康检查结果中获取数据
    let healthyBackends = 0;
    let fastestBackend = null;
    let fastestResponseTime = Infinity;
    let checkResults = {};
    let availableBackend = null;
    let checkTime = beijingTime;
    
    if (latestHealthCheck) {
      checkResults = typeof latestHealthCheck.results === 'string' 
        ? JSON.parse(latestHealthCheck.results) 
        : latestHealthCheck.results;
      availableBackend = latestHealthCheck.available_backend;
      checkTime = latestHealthCheck.beijing_time || checkTime;
      
      // 计算健康后端数量和最快后端
      for (const [url, health] of Object.entries(checkResults)) {
        if (health.healthy) {
          healthyBackends++;
          if (health.responseTime < fastestResponseTime) {
            fastestResponseTime = health.responseTime;
            fastestBackend = url;
          }
        }
      }
    }
    
    // 当前时间的北京时间
    const beijingNowStr = getBeijingTimeString();
    
    // D1写入统计
    const d1DailyWrites = d1Stats ? d1Stats.today.total : cache.d1WriteStats.dailyCount;
    const d1TotalWrites = d1Stats ? d1Stats.total.total : cache.d1WriteStats.totalCount;
    const todayBeijingDate = getBeijingDateString(new Date());
    
    // 构建HTML页面
    const html = `
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>订阅转换服务状态 (D1数据库)</title>
    <style>
        * { 
            margin: 0; 
            padding: 0; 
            box-sizing: border-box; 
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 12px;
            -webkit-text-size-adjust: 100%;
            -webkit-font-smoothing: antialiased;
        }
        
        .container {
            background: white;
            padding: 16px;
            border-radius: 12px;
            box-shadow: 0 8px 32px rgba(0,0,0,0.1);
            max-width: 100%;
            margin: 0 auto;
            overflow: hidden;
        }
        
        h1 {
            color: #333;
            margin-bottom: 16px;
            text-align: center;
            font-size: 20px;
            font-weight: 600;
            line-height: 1.3;
        }
        
        .status-header {
            text-align: center;
            margin-bottom: 20px;
        }
        
        .status-badge {
            display: inline-block;
            padding: 10px 18px;
            border-radius: 20px;
            font-weight: 600;
            font-size: 16px;
            margin-bottom: 12px;
            max-width: 90%;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        
        .status-healthy { 
            background: #d4edda; 
            color: #155724; 
            border: 1px solid #c3e6cb;
        }
        
        .status-unhealthy { 
            background: #f8d7da; 
            color: #721c24; 
            border: 1px solid #f5c6cb;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 10px;
            margin-bottom: 20px;
        }
        
        @media (min-width: 480px) {
            .stats-grid {
                gap: 12px;
            }
        }
        
        .stat-card {
            background: #f8f9fa;
            padding: 14px 10px;
            border-radius: 8px;
            text-align: center;
            min-height: 70px;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }
        
        .stat-value {
            font-size: 22px;
            font-weight: 700;
            color: #2c3e50;
            margin-bottom: 4px;
            line-height: 1.2;
        }
        
        .stat-label {
            font-size: 12px;
            color: #6c757d;
            line-height: 1.3;
        }
        
        .current-backend {
            background: #e7f5ff;
            border: 1px solid #bbdefb;
            padding: 14px;
            border-radius: 10px;
            margin-bottom: 20px;
        }
        
        .current-backend h3 {
            color: #1971c2;
            margin-bottom: 8px;
            font-size: 16px;
            font-weight: 600;
        }
        
        .backend-url {
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
            font-size: 12px;
            color: #495057;
            word-break: break-all;
            margin-bottom: 12px;
            line-height: 1.4;
            padding: 8px;
            background: rgba(255, 255, 255, 0.7);
            border-radius: 6px;
            border: 1px solid #dee2e6;
        }
        
        .backends-list {
            margin-bottom: 20px;
        }
        
        .backends-list h3 {
            margin-bottom: 12px;
            color: #495057;
            font-size: 16px;
            font-weight: 600;
        }
        
        .backend-item {
            padding: 12px;
            border: 1px solid #e9ecef;
            border-radius: 8px;
            margin-bottom: 10px;
            background: #fff;
            font-size: 14px;
        }
        
        .health-indicator {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            display: inline-block;
            margin-right: 8px;
            flex-shrink: 0;
        }
        
        .health-up { background: #28a745; }
        .health-down { background: #dc3545; }
        
        .backend-info {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 8px;
            flex-wrap: wrap;
            gap: 6px;
        }
        
        .backend-name {
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
            font-size: 13px;
            color: #495057;
            font-weight: 500;
            word-break: break-all;
            flex: 1;
            min-width: 0;
        }
        
        .backend-meta {
            font-size: 11px;
            color: #6c757d;
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            margin-top: 6px;
        }
        
        .meta-item {
            background: #f8f9fa;
            padding: 3px 6px;
            border-radius: 4px;
            white-space: nowrap;
        }
        
        .info-section {
            background: #f8f9fa;
            border: 1px solid #e9ecef;
            padding: 14px;
            border-radius: 10px;
            margin-top: 16px;
            font-size: 14px;
        }
        
        .info-section h3 {
            color: #495057;
            margin-bottom: 12px;
            font-size: 16px;
            font-weight: 600;
        }
        
        .info-section ul {
            margin-left: 16px;
            color: #6c757d;
            line-height: 1.5;
        }
        
        .info-section li {
            margin-bottom: 6px;
        }
        
        .action-buttons {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 20px;
            justify-content: center;
        }
        
        .action-btn {
            background: #007bff;
            color: white;
            padding: 10px 14px;
            border-radius: 6px;
            text-decoration: none;
            transition: all 0.2s ease;
            border: none;
            cursor: pointer;
            font-size: 13px;
            min-width: 0;
            flex: 1;
            min-width: 120px;
            max-width: calc(50% - 4px);
            text-align: center;
            display: flex;
            align-items: center;
            justify-content: center;
            height: 40px;
        }
        
        .action-btn:hover, .action-btn:active {
            background: #0056b3;
            transform: translateY(-1px);
        }
        
        .action-btn-secondary {
            background: #28a745;
        }
        
        .action-btn-secondary:hover, .action-btn-secondary:active {
            background: #1e7e34;
        }
        
        .action-btn-danger {
            background: #dc3545;
        }
        
        .action-btn-danger:hover, .action-btn-danger:active {
            background: #c82333;
        }
        
        .footer {
            text-align: center;
            color: #6c757d;
            font-size: 11px;
            margin-top: 20px;
            line-height: 1.5;
            padding: 12px 0 0 0;
            border-top: 1px solid #e9ecef;
        }
        
        .time-info {
            text-align: center;
            color: #495057;
            margin-bottom: 16px;
            font-size: 13px;
            line-height: 1.4;
        }
        
        .d1-stats {
            background: #d4edda;
            border: 1px solid #c3e6cb;
            padding: 12px;
            border-radius: 8px;
            margin-top: 16px;
        }
        
        .d1-stats h3 {
            color: #155724;
            margin-bottom: 8px;
            font-size: 16px;
            font-weight: 600;
        }
        
        /* 响应式调整 */
        @media (max-width: 360px) {
            .container {
                padding: 12px;
            }
            
            h1 {
                font-size: 18px;
            }
            
            .stat-card {
                padding: 10px 8px;
                min-height: 60px;
            }
            
            .stat-value {
                font-size: 20px;
            }
            
            .stat-label {
                font-size: 11px;
            }
            
            .action-btn {
                min-width: 110px;
                padding: 9px 12px;
                font-size: 12px;
            }
        }
        
        @media (min-width: 640px) {
            .container {
                padding: 24px;
                max-width: 640px;
            }
            
            h1 {
                font-size: 24px;
            }
            
            .stats-grid {
                grid-template-columns: repeat(4, 1fr);
                gap: 15px;
            }
            
            .stat-card {
                padding: 18px 12px;
            }
            
            .stat-value {
                font-size: 24px;
            }
            
            .stat-label {
                font-size: 13px;
            }
            
            .backend-item {
                padding: 14px;
            }
            
            .action-buttons {
                gap: 10px;
            }
            
            .action-btn {
                flex: 0 1 auto;
                max-width: none;
                min-width: 140px;
            }
        }
        
        /* 按钮状态 */
        .action-btn:disabled {
            opacity: 0.6;
            cursor: not-allowed;
            transform: none !important;
        }
        
        /* 触摸设备优化 */
        @media (hover: none) {
            .action-btn:hover {
                transform: none;
            }
            
            .action-btn:active {
                transform: scale(0.98);
            }
        }
        
        /* 深色模式支持 */
        @media (prefers-color-scheme: dark) {
            body {
                background: linear-gradient(135deg, #4c51bf 0%, #6b21a8 100%);
            }
            
            .container {
                background: #1a1a1a;
                color: #e0e0e0;
            }
            
            h1 {
                color: #e0e0e0;
            }
            
            .stat-card {
                background: #2d2d2d;
            }
            
            .stat-value {
                color: #ffffff;
            }
            
            .stat-label {
                color: #a0a0a0;
            }
            
            .current-backend {
                background: #1e3a5f;
                border-color: #3b82f6;
            }
            
            .current-backend h3 {
                color: #93c5fd;
            }
            
            .backend-url {
                background: #2d2d2d;
                border-color: #404040;
                color: #d1d5db;
            }
            
            .backend-item {
                background: #2d2d2d;
                border-color: #404040;
            }
            
            .backend-name {
                color: #d1d5db;
            }
            
            .meta-item {
                background: #3d3d3d;
                color: #b0b0b0;
            }
            
            .info-section {
                background: #2d2d2d;
                border-color: #404040;
            }
            
            .info-section h3 {
                color: #d1d5db;
            }
            
            .info-section ul {
                color: #a0a0a0;
            }
            
            .d1-stats {
                background: #1e453e;
                border-color: #059669;
            }
            
            .d1-stats h3 {
                color: #34d399;
            }
        }
        
        /* 高对比度模式 */
        @media (prefers-contrast: high) {
            .status-healthy {
                border: 2px solid #155724;
            }
            
            .status-unhealthy {
                border: 2px solid #721c24;
            }
            
            .backend-item {
                border: 1px solid #000;
            }
            
            .action-btn {
                border: 1px solid #000;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 订阅转换服务状态 (D1数据库)</h1>
        
        <div class="time-info">
            页面生成时间: ${beijingNowStr}<br>
            数据更新时间: ${checkTime}
        </div>
        
        <div class="status-header">
            <div class="status-badge ${healthyBackends > 0 ? 'status-healthy' : 'status-unhealthy'}">
                ${healthyBackends > 0 ? '🟢 服务正常' : '🔴 服务异常'}
            </div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">${totalBackends}</div>
                <div class="stat-label">总后端</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">${healthyBackends}</div>
                <div class="stat-label">健康后端</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">${d1DailyWrites}</div>
                <div class="stat-label">今日D1写入</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">${totalRequests}</div>
                <div class="stat-label">总请求</div>
            </div>
        </div>
        
        ${availableBackend ? `
        <div class="current-backend">
            <h3>当前使用后端</h3>
            <div class="backend-url">${availableBackend}</div>
            <div class="backend-meta">
                <span class="meta-item">响应时间: ${fastestResponseTime !== Infinity ? fastestResponseTime + 'ms' : '未知'}</span>
                <span class="meta-item">最后检查: ${checkTime}</span>
            </div>
        </div>
        ` : totalBackends > 0 ? `
        <div class="current-backend" style="background: #f8d7da; border-color: #f5c6cb;">
            <h3 style="color: #721c24;">⚠️ 服务异常</h3>
            <div style="color: #721c24;">所有后端服务器均不可用</div>
        </div>
        ` : `
        <div class="current-backend" style="background: #e2e3e5; border-color: #d6d8db;">
            <h3 style="color: #383d41;">⚪ 未配置</h3>
            <div style="color: #383d41;">尚未配置后端服务器</div>
        </div>
        `}
        
        ${totalBackends > 0 ? `
        <div class="backends-list">
            <h3>后端状态详情</h3>
            ${backends.map(url => {
              const status = checkResults[url] || { healthy: false, version: '未知版本' };
              const dbStatus = backendStatus.find(b => b.backend_url === url);
              const weight = dbStatus ? dbStatus.weight : (cache.backendWeights.get(url) || 100);
              const failureCount = dbStatus ? dbStatus.failure_count : (cache.backendFailureCounts.get(url) || 0);
              const requestCount = dbStatus ? dbStatus.request_count : (cache.requestCounts.get(url) || 0);
              
              const statusClass = status.healthy ? 'health-up' : 'health-down';
              const statusText = status.healthy ? '正常' : '异常';
              
              return `
              <div class="backend-item">
                  <div class="backend-info">
                      <div>
                          <span class="health-indicator ${statusClass}"></span>
                          <span class="backend-name">${url}</span>
                      </div>
                      <span>${statusText}</span>
                  </div>
                  <div class="backend-meta">
                      <span class="meta-item">版本: ${status.version || '未知'}</span>
                      <span class="meta-item">权重: ${weight}</span>
                      <span class="meta-item">失败: ${failureCount}</span>
                      <span class="meta-item">请求: ${requestCount}</span>
                      ${status.responseTime ? `<span class="meta-item">响应: ${status.responseTime}ms</span>` : ''}
                  </div>
              </div>`;
            }).join('')}
        </div>
        ` : ''}
        
        <div class="d1-stats">
            <h3>💾 D1数据库统计</h3>
            <div class="backend-meta">
                <span class="meta-item">今日写入: ${d1DailyWrites}次</span>
                <span class="meta-item">总写入: ${d1TotalWrites}次</span>
                <span class="meta-item">健康检查记录: ${d1Stats?.total?.health_checks || 0}条</span>
                <span class="meta-item">请求记录: ${d1Stats?.total?.request_results || 0}条</span>
                <span class="meta-item">后端状态记录: ${d1Stats?.total?.backend_status || 0}条</span>
            </div>
        </div>
        
        <div class="info-section">
            <h3>📋 系统信息</h3>
            <ul>
                <li><strong>数据来源:</strong> D1数据库（实时读取）</li>
                <li><strong>定时任务:</strong> 每2分钟执行一次健康检查并写入D1</li>
                <li><strong>订阅转换请求:</strong> 每次请求结果都写入D1</li>
                <li><strong>错误日志:</strong> ${errorLogs.length}条记录</li>
                <li><strong>请求ID:</strong> ${requestId}</li>
            </ul>
        </div>
        
        <div class="action-buttons">
            <button class="action-btn" onclick="performHealthCheck()" id="healthCheckBtn">🚀 手动健康检查</button>
            <a href="/api/health" class="action-btn">📊 健康状态API</a>
            <a href="/api/config" class="action-btn">⚙️ 配置信息</a>
            <a href="/api/d1-stats" class="action-btn">💾 D1统计</a>
            <button class="action-btn action-btn-danger" onclick="cleanupD1Data()">🗑️ 清理旧数据</button>
            <button class="action-btn action-btn-secondary" onclick="resetWeights()">🔄 重置权重</button>
        </div>
        
        <div class="footer">
            <div>页面数据直接从D1数据库读取，保证数据一致性</div>
            <div>定时任务每2分钟更新数据，订阅转换请求实时记录</div>
            <div>D1数据库无写入限制，可永久存储历史数据</div>
        </div>
    </div>
    
    <script>
        // 确保脚本在DOM加载后执行
        document.addEventListener('DOMContentLoaded', function() {
            // 移动端触摸优化
            const buttons = document.querySelectorAll('.action-btn');
            buttons.forEach(btn => {
                btn.addEventListener('touchstart', function() {
                    this.style.opacity = '0.8';
                });
                
                btn.addEventListener('touchend', function() {
                    this.style.opacity = '1';
                });
            });
            
            // 防止双击缩放
            let lastTouchEnd = 0;
            document.addEventListener('touchend', function(event) {
                const now = Date.now();
                if (now - lastTouchEnd <= 300) {
                    event.preventDefault();
                }
                lastTouchEnd = now;
            }, false);
        });
        
        function performHealthCheck() {
            const btn = document.getElementById('healthCheckBtn');
            const originalText = btn.textContent;
            const originalHTML = btn.innerHTML;
            
            btn.innerHTML = '🔄 检查中...';
            btn.disabled = true;
            
            fetch('/api/health-check', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                }
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    // 显示成功消息
                    showToast('健康检查完成！页面即将刷新...', 'success');
                    setTimeout(() => {
                        window.location.reload();
                    }, 1500);
                } else {
                    showToast('健康检查失败：' + (data.error || '未知错误'), 'error');
                    btn.innerHTML = originalHTML;
                    btn.disabled = false;
                }
            })
            .catch(error => {
                showToast('请求失败：' + error.message, 'error');
                btn.innerHTML = originalHTML;
                btn.disabled = false;
            });
        }
        
        function cleanupD1Data() {
            if (confirm('确定要清理7天前的旧数据吗？此操作不可撤销。')) {
                showToast('正在清理数据...', 'info');
                
                fetch('/api/cleanup-d1?days=7', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    }
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        showToast(data.message + ' 页面即将刷新。', 'success');
                        setTimeout(() => {
                            window.location.reload();
                        }, 1000);
                    } else {
                        showToast('数据清理失败：' + (data.error || '未知错误'), 'error');
                    }
                })
                .catch(error => {
                    showToast('请求失败：' + error.message, 'error');
                });
            }
        }
        
        function resetWeights() {
            if (confirm('确定要重置所有后端权重吗？这会将所有后端权重恢复到最大值，并清空失败计数。')) {
                showToast('正在重置权重...', 'info');
                
                fetch('/api/reset-weights', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    }
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        showToast(data.message || '权重重置成功！页面即将刷新。', 'success');
                        setTimeout(() => {
                            window.location.reload();
                        }, 1000);
                    } else {
                        showToast('权重重置失败：' + (data.error || '未知错误'), 'error');
                    }
                })
                .catch(error => {
                    showToast('请求失败：' + error.message, 'error');
                });
            }
        }
        
        // 简单的Toast通知函数
        function showToast(message, type = 'info') {
            // 移除已有的toast
            const existingToast = document.querySelector('.toast-notification');
            if (existingToast) {
                existingToast.remove();
            }
            
            const toast = document.createElement('div');
            toast.className = 'toast-notification';
            toast.innerHTML = message;
            
            // 样式
            toast.style.position = 'fixed';
            toast.style.bottom = '20px';
            toast.style.left = '50%';
            toast.style.transform = 'translateX(-50%)';
            toast.style.backgroundColor = type === 'success' ? '#28a745' : 
                                         type === 'error' ? '#dc3545' : '#007bff';
            toast.style.color = 'white';
            toast.style.padding = '12px 20px';
            toast.style.borderRadius = '8px';
            toast.style.zIndex = '1000';
            toast.style.boxShadow = '0 4px 12px rgba(0,0,0,0.15)';
            toast.style.fontSize = '14px';
            toast.style.maxWidth = '90%';
            toast.style.textAlign = 'center';
            toast.style.animation = 'fadeIn 0.3s ease';
            
            document.body.appendChild(toast);
            
            // 3秒后自动移除
            setTimeout(() => {
                toast.style.animation = 'fadeOut 0.3s ease';
                setTimeout(() => {
                    if (toast.parentNode) {
                        toast.remove();
                    }
                }, 300);
            }, 3000);
        }
        
        // 添加CSS动画
        const style = document.createElement('style');
        style.textContent = \`
            @keyframes fadeIn {
                from { opacity: 0; transform: translateX(-50%) translateY(20px); }
                to { opacity: 1; transform: translateX(-50%) translateY(0); }
            }
            @keyframes fadeOut {
                from { opacity: 1; transform: translateX(-50%) translateY(0); }
                to { opacity: 0; transform: translateX(-50%) translateY(20px); }
            }
        \`;
        document.head.appendChild(style);
        
        // 每60秒自动刷新页面（仅在用户不操作时）
        let lastActivity = Date.now();
        const refreshInterval = 60000; // 60秒
        
        // 监听用户活动
        ['click', 'touchstart', 'scroll', 'keydown'].forEach(event => {
            document.addEventListener(event, () => {
                lastActivity = Date.now();
            });
        });
        
        // 设置自动刷新检查
        setInterval(() => {
            const now = Date.now();
            if (now - lastActivity > refreshInterval) {
                // 用户60秒内无操作，询问是否刷新
                if (confirm('页面已加载60秒，是否刷新以获取最新数据？')) {
                    window.location.reload();
                } else {
                    // 用户取消，重置活动时间
                    lastActivity = now;
                }
            }
        }, 30000); // 每30秒检查一次
    </script>
</body>
</html>`;
    
    return new Response(html, {
      headers: { 
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-cache, no-store, must-revalidate'
      }
    });
  } catch (error) {
    logError('创建状态页面失败', error, requestId);
    return new Response('状态页面暂时不可用，请稍后重试', {
      status: 500,
      headers: { 'Content-Type': 'text/html; charset=utf-8' }
    });
  }
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
    
    // 状态页面处理
    if (url.pathname === '/' || url.pathname === '/status') {
      try {
        // 从D1数据库读取数据生成状态页面
        return createStatusPage(requestId, env);
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
      
      const previousAvailableBackend = cache.lastAvailableBackend;
      
      // 查找可用后端
      const db = env.DB ? new D1Database(env.DB) : null;
      const { backend: backendUrl, selectionTime: backendSelectionTime } = await findAvailableBackendForRequest(db, requestId, env);
      
      if (!backendUrl) {
        console.log(`[${requestId}] 无可用后端，返回503`);
        
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
      
      const response = await handleSubconverterRequest(
        request, 
        backendUrl, 
        backendSelectionTime, 
        requestId, 
        env, 
        ctx
      );
      
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
    console.log(`[${requestId}] Cron触发，开始执行健康检查（D1数据库）`);
    
    try {
      const db = env.DB ? new D1Database(env.DB) : null;
      await performFullHealthCheck(db, requestId, env);
      
      // 执行缓存清理
      cleanupExpiredCache(env);
      
      console.log(`[${requestId}] Cron健康检查完成，已写入D1，今日D1写入次数: ${cache.d1WriteStats.dailyCount}`);
      
      // 每周清理一次旧数据（每周日执行）
      const now = new Date();
      if (now.getUTCDay() === 0 && db) { // 0表示周日
        console.log(`[${requestId}] 周日执行D1数据清理`);
        ctx.waitUntil(db.cleanupOldData(7));
      }
    } catch (error) {
      logError('Cron健康检查失败', error, requestId);
    }
  }
};