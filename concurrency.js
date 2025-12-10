// 并发控制器
export class HealthCheckConcurrencyController {
  constructor(config) {
    this.config = config || { maxConcurrent: 5 };
    this.activeChecks = new Map();
    this.queue = [];
    this.stats = {
      totalStarted: 0,
      totalCompleted: 0,
      maxActive: 0,
      errors: 0,
      successes: 0,
      lastReset: new Date().toISOString(),
      lastResetTime: Date.now()
    };
  }

  async scheduleCheck(url, checkFn, requestId) {
    // 检查是否需要重置统计（每5分钟重置一次）
    const now = Date.now();
    if (now - this.stats.lastResetTime > 5 * 60 * 1000) {
      this.reset();
    }
    
    const taskKey = `${url}-${Date.now()}`;
    
    const task = async () => {
      this.activeChecks.set(taskKey, {
        url,
        requestId,
        startTime: Date.now(),
        status: 'running'
      });
      
      this.stats.totalStarted++;
      this.stats.maxActive = Math.max(this.stats.maxActive, this.activeChecks.size);
      
      try {
        const result = await checkFn(url, requestId);
        this.stats.successes++;
        
        this.activeChecks.set(taskKey, {
          ...this.activeChecks.get(taskKey),
          status: 'completed',
          endTime: Date.now(),
          success: true
        });
        
        return result;
      } catch (error) {
        this.stats.errors++;
        
        this.activeChecks.set(taskKey, {
          ...this.activeChecks.get(taskKey),
          status: 'failed',
          endTime: Date.now(),
          error: error.message,
          success: false
        });
        
        throw error;
      } finally {
        // 使用微任务延迟清理，避免在finally块中产生异步操作问题
        Promise.resolve().then(() => {
          setTimeout(() => {
            this.activeChecks.delete(taskKey);
            this.stats.totalCompleted++;
            this.processQueue();
          }, 100);
        });
      }
    };

    if (this.activeChecks.size < this.config.maxConcurrent) {
      return task();
    } else {
      console.log(`[${requestId}] 健康检查队列已满，等待执行: ${url}，当前队列长度: ${this.queue.length}`);
      return new Promise((resolve, reject) => {
        this.queue.push({
          task: () => task().then(resolve).catch(reject),
          url,
          requestId,
          addedTime: Date.now()
        });
      });
    }
  }

  processQueue() {
    const now = Date.now();
    // 清理过期的队列项（超过30秒）
    this.queue = this.queue.filter(item => now - item.addedTime < 30000);
    
    while (this.queue.length > 0 && this.activeChecks.size < this.config.maxConcurrent) {
      const item = this.queue.shift();
      console.log(`[${item.requestId}] 从队列中取出任务: ${item.url}`);
      item.task();
    }
  }

  getStats() {
    const now = Date.now();
    const activeTasks = Array.from(this.activeChecks.entries()).map(([key, value]) => ({
      ...value,
      duration: value.endTime ? value.endTime - value.startTime : now - value.startTime
    }));
    
    return {
      ...this.stats,
      activeChecks: this.activeChecks.size,
      queueLength: this.queue.length,
      activeTasks: activeTasks,
      currentUtilization: this.activeChecks.size / this.config.maxConcurrent,
      avgResponseTime: this.stats.successes > 0 ? 
        activeTasks.reduce((sum, task) => sum + (task.duration || 0), 0) / Math.max(activeTasks.length, 1) : 0,
      successRate: this.stats.totalStarted > 0 ? 
        (this.stats.successes / this.stats.totalStarted * 100).toFixed(2) + '%' : '0%'
    };
  }

  reset() {
    this.activeChecks.clear();
    this.queue = [];
    this.stats = {
      totalStarted: 0,
      totalCompleted: 0,
      maxActive: 0,
      errors: 0,
      successes: 0,
      lastReset: new Date().toISOString(),
      lastResetTime: Date.now()
    };
  }
}

// 全局并发控制器实例
export const healthCheckController = new HealthCheckConcurrencyController({ maxConcurrent: 5 });