import { healthCheckController } from './concurrency.js';
import { calculateResponseTimeScore, getConfig } from './utils.js';

export class PriorityHealthCheck {
  constructor(env) {
    this.env = env;
    this.controller = healthCheckController;
    this.timeout = getConfig(env, 'HEALTH_CHECK_TIMEOUT', 2000);
    this.fastTimeout = getConfig(env, 'FAST_CHECK_TIMEOUT', 800);
  }

  // 提取版本信息
  extractVersionFromText(text) {
    if (!text || typeof text !== 'string') return 'subconverter';
    
    // 清理文本
    text = text.trim();
    
    // 尝试匹配各种版本格式
    const versionPatterns = [
      // 完整版本格式: subconverter v0.9.9-7544246 backend
      /(subconverter\s+v[\d]+\.[\d]+\.[\d]+-[\w]+(?:\s+backend)?)/i,
      
      // 标准版本格式: v0.9.9-7544246
      /(v[\d]+\.[\d]+\.[\d]+-[\w]+)/i,
      
      // 简略版本格式: subconverter v0.9.9
      /(subconverter\s+v[\d]+\.[\d]+\.[\d]+)/i,
      
      // 纯版本号: 0.9.9-7544246
      /([\d]+\.[\d]+\.[\d]+-[\w]+)/,
      
      // 基础版本号: 0.9.9
      /([\d]+\.[\d]+\.[\d]+)/,
      
      // 如果只是 subconverter 关键字
      /(subconverter)/i
    ];
    
    for (const pattern of versionPatterns) {
      const match = text.match(pattern);
      if (match) {
        // 返回匹配的第一个完整字符串
        const version = match[1].trim();
        
        // 确保格式统一
        if (version.toLowerCase().includes('subconverter')) {
          return version;
        } else if (version.startsWith('v')) {
          return `subconverter ${version}`;
        } else if (/^\d/.test(version)) {
          return `subconverter v${version}`;
        } else {
          return version;
        }
      }
    }
    
    // 如果没有匹配到任何模式，返回原始文本的前50个字符
    if (text.length <= 50) {
      return text || 'subconverter';
    }
    
    // 尝试从文本中提取任何看起来像版本的信息
    const words = text.split(/\s+/);
    for (const word of words) {
      if (word.includes('v') || /[\d]+\.[\d]+/.test(word)) {
        if (word.length <= 30) {
          return `subconverter ${word}`;
        }
      }
    }
    
    return 'subconverter';
  }

  // 优先级健康检查（快速路径）
  async priorityCheck(url, requestId) {
    const checkFn = async () => {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.fastTimeout);
      
      try {
        const startTime = Date.now();
        const response = await fetch(`${url}/version`, {
          signal: controller.signal,
          headers: { 
            'User-Agent': 'subconverter-failover-worker/1.0',
            'X-Request-ID': requestId
          },
          cf: { cacheTtl: 0 }
        });
        const responseTime = Date.now() - startTime;
        
        clearTimeout(timeoutId);
        
        if (response.status === 200) {
          try {
            const text = await response.text();
            const version = this.extractVersionFromText(text);
            const responseTimeScore = calculateResponseTimeScore(responseTime, this.env);
            
            return {
              healthy: true,
              responseTime,
              responseTimeScore,
              status: response.status,
              version: version,
              priority: 'high',
              timestamp: new Date().toISOString()
            };
          } catch (textError) {
            return {
              healthy: true,
              responseTime,
              responseTimeScore: calculateResponseTimeScore(responseTime, this.env),
              status: response.status,
              version: 'subconverter',
              priority: 'high',
              timestamp: new Date().toISOString()
            };
          }
        } else {
          return {
            healthy: false,
            responseTime,
            responseTimeScore: 0,
            status: response.status,
            version: '未知版本',
            priority: 'high',
            timestamp: new Date().toISOString()
          };
        }
      } catch (error) {
        clearTimeout(timeoutId);
        return {
          healthy: false,
          responseTime: null,
          responseTimeScore: 0,
          status: 0,
          version: '未知版本',
          priority: 'high',
          error: error.name,
          timestamp: new Date().toISOString()
        };
      }
    };

    return this.controller.scheduleCheck(url, checkFn, requestId);
  }

  // 完整健康检查
  async fullCheck(url, requestId) {
    const checkFn = async () => {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.timeout);
      
      try {
        const startTime = Date.now();
        const response = await fetch(`${url}/version`, {
          signal: controller.signal,
          headers: { 
            'User-Agent': 'subconverter-failover-worker/1.0',
            'X-Request-ID': requestId
          }
        });
        const responseTime = Date.now() - startTime;
        
        clearTimeout(timeoutId);
        
        if (response.status === 200) {
          try {
            const text = await response.text();
            const version = this.extractVersionFromText(text);
            const healthy = text.toLowerCase().includes('subconverter') || response.status === 200;
            const responseTimeScore = calculateResponseTimeScore(responseTime, this.env);
            
            return {
              healthy: healthy,
              version: version,
              responseTime: responseTime,
              responseTimeScore,
              status: response.status,
              priority: 'normal',
              timestamp: new Date().toISOString()
            };
          } catch (textError) {
            return {
              healthy: false,
              version: '未知版本',
              responseTime: responseTime,
              responseTimeScore: calculateResponseTimeScore(responseTime, this.env),
              status: response.status,
              priority: 'normal',
              timestamp: new Date().toISOString()
            };
          }
        } else {
          return {
            healthy: false,
            version: '未知版本',
            responseTime: responseTime,
            responseTimeScore: 0,
            status: response.status,
            priority: 'normal',
            timestamp: new Date().toISOString()
          };
        }
      } catch (error) {
        clearTimeout(timeoutId);
        return {
          healthy: false,
          version: '未知版本',
          responseTime: null,
          responseTimeScore: 0,
          status: 0,
          priority: 'normal',
          error: error.name,
          timestamp: new Date().toISOString()
        };
      }
    };

    return this.controller.scheduleCheck(url, checkFn, requestId);
  }
}