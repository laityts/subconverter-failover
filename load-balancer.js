import { 
  smoothWeightAdjustment, 
  calculateResponseTimeScore,
  getConfig
} from './utils.js';

export class SmartWeightedLoadBalancer {
  constructor(env) {
    this.env = env;
    this.backendStats = new Map(); // 缓存后端统计信息
    this.lastUpdateTime = new Map(); // 最后更新时间
    this.weightCache = new Map(); // 权重缓存
    this.responseTimeHistory = new Map(); // 响应时间历史记录
    this.maxHistorySize = getConfig(env, 'RESPONSE_TIME_WINDOW', 10);
  }

  // 添加响应时间到历史记录
  addResponseTimeToHistory(backendUrl, responseTime) {
    if (!this.responseTimeHistory.has(backendUrl)) {
      this.responseTimeHistory.set(backendUrl, []);
    }
    
    const history = this.responseTimeHistory.get(backendUrl);
    history.push({
      responseTime,
      timestamp: Date.now()
    });
    
    // 保留最近的记录
    if (history.length > this.maxHistorySize) {
      history.shift();
    }
    
    // 清理过时的记录（超过5分钟）
    const now = Date.now();
    const fiveMinutesAgo = now - 5 * 60 * 1000;
    const filtered = history.filter(item => item.timestamp > fiveMinutesAgo);
    this.responseTimeHistory.set(backendUrl, filtered);
  }

  // 计算平均响应时间
  calculateAverageResponseTime(backendUrl) {
    const history = this.responseTimeHistory.get(backendUrl) || [];
    if (history.length === 0) {
      return null;
    }
    
    const total = history.reduce((sum, item) => sum + item.responseTime, 0);
    return total / history.length;
  }

  // 计算响应时间稳定性得分（0-100）
  calculateResponseTimeStability(backendUrl) {
    const history = this.responseTimeHistory.get(backendUrl) || [];
    if (history.length < 3) {
      return 70; // 默认稳定性得分
    }
    
    // 计算标准差
    const times = history.map(item => item.responseTime);
    const mean = times.reduce((a, b) => a + b) / times.length;
    const squaredDiffs = times.map(time => Math.pow(time - mean, 2));
    const variance = squaredDiffs.reduce((a, b) => a + b) / times.length;
    const stdDev = Math.sqrt(variance);
    
    // 标准差越小，稳定性越高
    const coefficientOfVariation = stdDev / mean;
    
    // 转换到0-100分
    let stabilityScore = 100 * Math.exp(-coefficientOfVariation);
    
    return Math.max(0, Math.min(100, Math.round(stabilityScore)));
  }

  // 智能计算后端权重
  async calculateBackendWeight(backendUrl, healthResult, db, requestId) {
    const MAX_WEIGHT = getConfig(this.env, 'MAX_WEIGHT', 100);
    const MIN_WEIGHT = getConfig(this.env, 'MIN_WEIGHT', 10);
    const BASE_WEIGHT = getConfig(this.env, 'BASE_WEIGHT', 50);
    const WEIGHT_ADJUSTMENT_FACTOR = getConfig(this.env, 'WEIGHT_ADJUSTMENT_FACTOR', 0.3);
    const SUCCESS_BOOST = getConfig(this.env, 'SUCCESS_BOOST', 8);
    const FAILURE_PENALTY = getConfig(this.env, 'FAILURE_PENALTY', 15);
    const HEALTH_THRESHOLD = getConfig(this.env, 'HEALTH_THRESHOLD', 0.7);
    const WEIGHT_RECOVERY_RATE = getConfig(this.env, 'WEIGHT_RECOVERY_RATE', 2);
    
    // 获取当前权重（从缓存或数据库）
    let currentWeight = this.weightCache.get(backendUrl) || BASE_WEIGHT;
    
    try {
      if (db) {
        const backendStatus = await db.getBackendStatus(backendUrl);
        if (backendStatus && backendStatus.weight) {
          currentWeight = backendStatus.weight;
          this.weightCache.set(backendUrl, currentWeight);
        }
      }
    } catch (error) {
      console.warn(`[${requestId}] 获取后端权重失败: ${error.message}`);
    }
    
    // 计算目标权重
    let targetWeight = BASE_WEIGHT;
    
    // 1. 健康状态权重调整
    if (healthResult.healthy) {
      // 健康状态奖励
      targetWeight += SUCCESS_BOOST;
      
      // 响应时间得分权重调整（0-100分转换为0-30权重）
      if (healthResult.responseTimeScore) {
        const responseTimeBonus = (healthResult.responseTimeScore / 100) * 30;
        targetWeight += responseTimeBonus;
      }
      
      // 响应时间历史稳定性奖励
      const stabilityScore = this.calculateResponseTimeStability(backendUrl);
      const stabilityBonus = (stabilityScore / 100) * 20;
      targetWeight += stabilityBonus;
      
      // 添加响应时间到历史记录
      if (healthResult.responseTime) {
        this.addResponseTimeToHistory(backendUrl, healthResult.responseTime);
      }
    } else {
      // 不健康状态惩罚
      targetWeight -= FAILURE_PENALTY;
    }
    
    // 2. 从数据库获取历史成功率
    try {
      if (db) {
        const stats = await db.getBackendStats(backendUrl);
        if (stats && stats.success_rate) {
          const successRate = stats.success_rate;
          
          // 成功率高于阈值奖励，低于阈值惩罚
          if (successRate >= HEALTH_THRESHOLD) {
            const successBonus = (successRate - HEALTH_THRESHOLD) * 50;
            targetWeight += successBonus;
          } else {
            const successPenalty = (HEALTH_THRESHOLD - successRate) * 50;
            targetWeight -= successPenalty;
          }
          
          // 请求量奖励（处理更多请求的后端获得更高权重）
          const requestVolumeBonus = Math.min(20, Math.log10(stats.total_requests + 1) * 5);
          targetWeight += requestVolumeBonus;
        }
      }
    } catch (error) {
      // 忽略数据库错误
    }
    
    // 3. 时间衰减恢复
    const now = Date.now();
    const lastUpdate = this.lastUpdateTime.get(backendUrl) || now;
    const timeSinceLastUpdate = now - lastUpdate;
    const minutesSinceLastUpdate = timeSinceLastUpdate / (60 * 1000);
    
    // 每分钟恢复一定权重（向基准权重恢复）
    if (minutesSinceLastUpdate > 0) {
      const recoveryAmount = minutesSinceLastUpdate * WEIGHT_RECOVERY_RATE;
      
      if (currentWeight < BASE_WEIGHT) {
        // 低于基准权重，向上恢复
        targetWeight = Math.min(BASE_WEIGHT, currentWeight + recoveryAmount);
      } else if (currentWeight > BASE_WEIGHT) {
        // 高于基准权重，向下恢复（避免权重过高）
        targetWeight = Math.max(BASE_WEIGHT, currentWeight - recoveryAmount * 0.5);
      }
    }
    
    // 4. 平滑权重调整
    const adjustedWeight = smoothWeightAdjustment(
      currentWeight,
      targetWeight,
      WEIGHT_ADJUSTMENT_FACTOR
    );
    
    // 5. 限制权重范围
    const finalWeight = Math.max(MIN_WEIGHT, Math.min(MAX_WEIGHT, Math.round(adjustedWeight)));
    
    // 更新缓存
    this.weightCache.set(backendUrl, finalWeight);
    this.lastUpdateTime.set(backendUrl, now);
    
    console.log(`[${requestId}] 后端权重计算: ${backendUrl}, 当前: ${currentWeight}, 目标: ${targetWeight.toFixed(1)}, 调整后: ${finalWeight}`);
    
    return finalWeight;
  }

  // 选择最优后端（加权轮询）
  async selectOptimalBackend(availableBackends, requestId, env, db) {
    const backends = Array.from(availableBackends.entries());
    
    if (backends.length === 0) {
      return null;
    }
    
    // 计算每个后端的权重
    const weightedBackends = [];
    const backendWeights = [];
    
    for (const [url, health] of backends) {
      const weight = await this.calculateBackendWeight(url, health, db, requestId);
      
      // 健康状态权重乘数
      let weightMultiplier = 1.0;
      if (health.healthy) {
        // 健康后端获得额外权重提升
        weightMultiplier = 1.5;
      } else {
        // 不健康后端权重减半
        weightMultiplier = 0.5;
      }
      
      const finalWeight = Math.max(1, Math.round(weight * weightMultiplier));
      
      for (let i = 0; i < finalWeight; i++) {
        weightedBackends.push(url);
      }
      
      backendWeights.push({
        url,
        weight: finalWeight,
        originalWeight: weight,
        healthy: health.healthy,
        responseTime: health.responseTime
      });
    }
    
    if (weightedBackends.length === 0) {
      return null;
    }
    
    // 使用加权随机选择
    const index = Math.floor(Math.random() * weightedBackends.length);
    const selectedBackend = weightedBackends[index];
    
    // 记录选择统计
    const selectedWeightInfo = backendWeights.find(b => b.url === selectedBackend);
    const totalWeight = weightedBackends.length;
    
    console.log(`[${requestId}] 加权轮询选择: ${selectedBackend}, 权重: ${selectedWeightInfo?.weight || 1}/${totalWeight}, 选中概率: ${((selectedWeightInfo?.weight || 1) / totalWeight * 100).toFixed(1)}%`);
    
    // 调试信息
    if (getConfig(env, 'DEBUG_MODE', false)) {
      console.log(`[${requestId}] 后端权重分布:`, backendWeights.map(b => ({
        url: b.url.substring(0, 30) + '...',
        weight: b.weight,
        healthy: b.healthy,
        responseTime: b.responseTime
      })));
    }
    
    return selectedBackend;
  }

  // 获取所有后端权重统计
  getWeightStatistics() {
    const stats = [];
    for (const [url, weight] of this.weightCache.entries()) {
      const lastUpdate = this.lastUpdateTime.get(url);
      const history = this.responseTimeHistory.get(url) || [];
      const avgResponseTime = this.calculateAverageResponseTime(url);
      const stability = this.calculateResponseTimeStability(url);
      
      stats.push({
        url,
        weight,
        lastUpdate: lastUpdate ? new Date(lastUpdate).toISOString() : null,
        responseTimeHistorySize: history.length,
        averageResponseTime: avgResponseTime,
        stabilityScore: stability
      });
    }
    
    return stats;
  }

  // 重置后端权重
  resetBackendWeight(backendUrl) {
    const BASE_WEIGHT = getConfig(this.env, 'BASE_WEIGHT', 50);
    this.weightCache.set(backendUrl, BASE_WEIGHT);
    this.lastUpdateTime.set(backendUrl, Date.now());
    this.responseTimeHistory.delete(backendUrl);
  }

  // 重置所有后端权重
  resetAllWeights() {
    const BASE_WEIGHT = getConfig(this.env, 'BASE_WEIGHT', 50);
    
    for (const [url] of this.weightCache.entries()) {
      this.weightCache.set(url, BASE_WEIGHT);
      this.lastUpdateTime.set(url, Date.now());
    }
    
    this.responseTimeHistory.clear();
  }
}