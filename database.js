import { 
  getBeijingTimeString, 
  getBeijingDateString,
  getBeijingTimeShort,
  getConfig
} from './utils.js';
import { healthCheckController } from './concurrency.js';

export class SafeD1Database {
  constructor(db, env) {
    this.db = db;
    this.env = env;
  }

  // 获取单个后端状态
  async getBackendStatus(backendUrl) {
    try {
      const result = await this.db
        .prepare('SELECT * FROM backend_status WHERE backend_url = ?')
        .bind(backendUrl)
        .first();
      return result || null;
    } catch (error) {
      console.error(`获取后端状态失败: ${backendUrl}`, error);
      return null;
    }
  }

  // 保存健康检查结果到D1，并限制最多10条记录
  async saveHealthCheckResult(data, requestId) {
    try {
      // 首先检查当前记录数量
      const countResult = await this.db
        .prepare('SELECT COUNT(*) as count FROM health_check_results')
        .first();
      
      const currentCount = countResult?.count || 0;
      
      // 如果超过10条，删除最旧的记录
      if (currentCount >= 10) {
        const deleteCount = currentCount - 9; // 保留9条，加上即将插入的1条，总共10条
        if (deleteCount > 0) {
          await this.db
            .prepare('DELETE FROM health_check_results WHERE id IN (SELECT id FROM health_check_results ORDER BY id ASC LIMIT ?)')
            .bind(deleteCount)
            .run();
          console.log(`[${requestId}] 删除了 ${deleteCount} 条旧的健康检查记录，保持最多10条记录`);
        }
      }
      
      // 插入新记录
      const stmt = this.db.prepare(`
        INSERT INTO health_check_results 
        (timestamp, beijing_time, results, available_backend, fastest_response_time, backend_changed, weight_statistics)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `);
      
      const result = await stmt.bind(
        data.timestamp || new Date().toISOString(),
        getBeijingTimeString(),
        JSON.stringify(data.results || {}),
        data.available_backend || null,
        data.fastest_response_time || 0,
        data.backend_changed ? 1 : 0,
        data.weight_statistics ? JSON.stringify(data.weight_statistics) : null
      ).run();
      
      console.log(`[${requestId}] 保存健康检查结果到D1，当前记录数: ${currentCount + 1}`);
      
      return result;
    } catch (error) {
      console.error(`[${requestId}] 保存健康检查结果到D1失败:`, error);
      throw error;
    }
  }

  // 获取上次健康检查结果
  async getLastHealthCheck() {
    try {
      const { results } = await this.db
        .prepare('SELECT * FROM health_check_results ORDER BY id DESC LIMIT 1')
        .all();
      return results[0] || null;
    } catch (error) {
      console.error('获取上次健康检查结果失败:', error);
      return null;
    }
  }

  // 保存请求结果到D1
  async saveRequestResult(data, requestId) {
    try {
      const stmt = this.db.prepare(`
        INSERT INTO request_results 
        (request_id, client_ip, backend_url, backend_selection_time, response_time, status_code, success, timestamp, beijing_time, backend_weight)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        getBeijingTimeString(),
        data.backend_weight || 0
      ).run();
      
      return result;
    } catch (error) {
      console.error(`[${requestId}] 保存请求结果到D1失败:`, error);
      throw error;
    }
  }

  // 保存Telegram通知记录到D1
  async saveTelegramNotification(data, requestId) {
    try {
      const stmt = this.db.prepare(`
        INSERT INTO telegram_notifications 
        (notification_type, request_id, client_ip, backend_url, status_code, response_time, success, message, sent_time, beijing_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      
      const result = await stmt.bind(
        data.notification_type || 'unknown',
        data.request_id || requestId,
        data.client_ip || 'unknown',
        data.backend_url || '',
        data.status_code || 0,
        data.response_time || 0,
        data.success ? 1 : 0,
        data.message || '',
        data.sent_time || new Date().toISOString(),
        getBeijingTimeString()
      ).run();
      
      return result;
    } catch (error) {
      console.error(`[${requestId}] 保存Telegram通知记录到D1失败:`, error);
      return null;
    }
  }

  // 智能更新后端状态和权重（使用北京时间，修复字段名）
  async updateBackendStatusWithWeight(backendUrl, healthResult, targetWeight, requestId) {
    try {
      // 先查询现有记录
      const existing = await this.getBackendStatus(backendUrl);
      const beijingTime = getBeijingTimeString();
      
      // 确保healthResult有必要的字段
      const safeHealthResult = {
        healthy: healthResult.healthy || false,
        responseTime: healthResult.responseTime || 0,
        responseTimeScore: healthResult.responseTimeScore || 0,
        status: healthResult.status || 0,
        version: healthResult.version || 'subconverter',
        error: healthResult.error || ''
      };
      
      if (existing) {
        // 更新现有记录
        const newFailureCount = safeHealthResult.healthy ? 0 : (existing.failure_count || 0) + 1;
        const newRequestCount = (existing.request_count || 0) + 1;
        const newWeight = targetWeight;
        
        // 计算成功率和平均响应时间
        let successRate = existing.success_rate || 1.0;
        let avgResponseTime = existing.avg_response_time || 0;
        
        if (safeHealthResult.healthy) {
          // 更新成功率（滑动窗口）
          const windowSize = getConfig(this.env, 'RESPONSE_TIME_WINDOW', 10);
          const successCount = (existing.success_count || 0) + 1;
          const totalInWindow = Math.min(windowSize, newRequestCount);
          successRate = successCount / totalInWindow;
          
          // 更新平均响应时间（指数加权移动平均）
          const alpha = 0.3; // 平滑因子
          if (avgResponseTime === 0) {
            avgResponseTime = safeHealthResult.responseTime || 0;
          } else {
            avgResponseTime = alpha * (safeHealthResult.responseTime || 0) + (1 - alpha) * avgResponseTime;
          }
        } else {
          // 失败时更新成功率
          const windowSize = getConfig(this.env, 'RESPONSE_TIME_WINDOW', 10);
          const successCount = existing.success_count || 0;
          const totalInWindow = Math.min(windowSize, newRequestCount);
          successRate = totalInWindow > 0 ? successCount / totalInWindow : 0;
        }
        
        const lastSuccessBeijing = safeHealthResult.healthy ? beijingTime : existing.last_success_beijing;
        
        // 修复SQL语句：只使用北京时间字段
        const stmt = this.db.prepare(`
          UPDATE backend_status 
          SET 
            healthy = ?, 
            last_checked_beijing = ?,
            weight = ?, 
            failure_count = ?,
            request_count = ?,
            success_count = ?,
            success_rate = ?,
            avg_response_time = ?,
            last_success_beijing = ?,
            version = ?, 
            response_time = ?, 
            updated_at_beijing = ?
          WHERE backend_url = ?
        `);
        
        await stmt.bind(
          safeHealthResult.healthy ? 1 : 0,
          beijingTime,                      // last_checked_beijing (北京时间)
          newWeight,
          newFailureCount,
          newRequestCount,
          safeHealthResult.healthy ? (existing.success_count || 0) + 1 : (existing.success_count || 0),
          successRate,
          Math.round(avgResponseTime),
          lastSuccessBeijing,                // last_success_beijing (北京时间)
          safeHealthResult.version || existing.version || 'subconverter',
          safeHealthResult.responseTime || existing.response_time || 0,
          beijingTime,                      // updated_at_beijing (北京时间)
          backendUrl
        ).run();
        
        console.log(`[${requestId}] 更新后端状态: ${backendUrl}, 健康: ${safeHealthResult.healthy}, 权重: ${newWeight}, 成功率: ${(successRate * 100).toFixed(1)}%, 平均响应时间: ${Math.round(avgResponseTime)}ms, 更新时间: ${beijingTime}`);
        
      } else {
        // 插入新记录
        const initialWeight = targetWeight;
        const initialFailureCount = safeHealthResult.healthy ? 0 : 1;
        const initialSuccessCount = safeHealthResult.healthy ? 1 : 0;
        const initialSuccessRate = safeHealthResult.healthy ? 1.0 : 0;
        
        const stmt = this.db.prepare(`
          INSERT INTO backend_status 
          (backend_url, healthy, last_checked_beijing, weight, failure_count, request_count, success_count, success_rate, avg_response_time, last_success_beijing, version, response_time, created_at_beijing, updated_at_beijing)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        
        await stmt.bind(
          backendUrl,
          safeHealthResult.healthy ? 1 : 0,
          beijingTime,                      // last_checked_beijing (北京时间)
          initialWeight,
          initialFailureCount,
          1,                                // request_count
          initialSuccessCount,
          initialSuccessRate,
          safeHealthResult.responseTime || 0,
          safeHealthResult.healthy ? beijingTime : null, // last_success_beijing (北京时间)
          safeHealthResult.version || 'subconverter',
          safeHealthResult.responseTime || 0,
          beijingTime,                      // created_at_beijing (北京时间)
          beijingTime                       // updated_at_beijing (北京时间)
        ).run();
        
        console.log(`[${requestId}] 插入新后端状态: ${backendUrl}, 健康: ${safeHealthResult.healthy}, 初始权重: ${initialWeight}, 创建时间: ${beijingTime}`);
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

  // 获取最近Telegram通知记录
  async getRecentTelegramNotifications(limit = 20) {
    try {
      const { results } = await this.db
        .prepare('SELECT * FROM telegram_notifications ORDER BY id DESC LIMIT ?')
        .bind(limit)
        .all();
      return results || [];
    } catch (error) {
      console.error('获取最近Telegram通知失败:', error);
      return [];
    }
  }

  // 获取所有后端最新状态
  async getAllBackendStatus() {
    try {
      const { results } = await this.db
        .prepare('SELECT * FROM backend_status ORDER BY updated_at_beijing DESC')
        .all();
      return results || [];
    } catch (error) {
      console.error('获取后端状态失败:', error);
      return [];
    }
  }

  // 获取后端统计信息（增强版）
  async getBackendStats(backendUrl) {
    try {
      // 获取最近一段时间内的请求统计
      const stats = await this.db
        .prepare(`
          SELECT 
            backend_url,
            COUNT(CASE WHEN success = 1 THEN 1 END) as success_count,
            COUNT(*) as total_count,
            AVG(response_time) as avg_response_time,
            MAX(response_time) as max_response_time,
            MIN(response_time) as min_response_time,
            AVG(backend_weight) as avg_weight
          FROM request_results 
          WHERE backend_url = ? 
          AND timestamp > datetime('now', '-30 minutes')
        `)
        .bind(backendUrl)
        .first();
      
      if (stats && stats.total_count > 0) {
        return {
          success_rate: stats.success_count / stats.total_count,
          avg_response_time: stats.avg_response_time,
          max_response_time: stats.max_response_time,
          min_response_time: stats.min_response_time,
          total_requests: stats.total_count,
          successful_requests: stats.success_count,
          avg_weight: stats.avg_weight
        };
      }
      
      // 如果最近30分钟没有数据，尝试从后端状态表获取
      const backendStatus = await this.getBackendStatus(backendUrl);
      if (backendStatus) {
        return {
          success_rate: backendStatus.success_rate || 0,
          avg_response_time: backendStatus.avg_response_time || 0,
          total_requests: backendStatus.request_count || 0,
          successful_requests: backendStatus.success_count || 0
        };
      }
      
      return null;
    } catch (error) {
      console.error('获取后端统计失败:', error);
      return null;
    }
  }

  // 获取D1写入统计
  async getD1WriteStats() {
    try {
      const today = getBeijingDateString(new Date());
      
      // 使用北京时间的日期部分进行匹配
      const todayStr = today + '%'; // 使用LIKE匹配
      
      const todayHealthChecks = await this.db
        .prepare('SELECT COUNT(*) as count FROM health_check_results WHERE beijing_time LIKE ?')
        .bind(todayStr)
        .first();
        
      const todayRequests = await this.db
        .prepare('SELECT COUNT(*) as count FROM request_results WHERE beijing_time LIKE ?')
        .bind(todayStr)
        .first();
        
      const todayNotifications = await this.db
        .prepare('SELECT COUNT(*) as count FROM telegram_notifications WHERE beijing_time LIKE ?')
        .bind(todayStr)
        .first();
      
      const totalHealthChecks = await this.db
        .prepare('SELECT COUNT(*) as count FROM health_check_results')
        .first();
      
      const totalRequests = await this.db
        .prepare('SELECT COUNT(*) as count FROM request_results')
        .first();
      
      const totalBackendStatus = await this.db
        .prepare('SELECT COUNT(*) as count FROM backend_status')
        .first();
      
      const totalTelegramNotifications = await this.db
        .prepare('SELECT COUNT(*) as count FROM telegram_notifications')
        .first();
      
      return {
        today: {
          health_checks: todayHealthChecks?.count || 0,
          request_results: todayRequests?.count || 0,
          telegram_notifications: todayNotifications?.count || 0,
          total: (todayHealthChecks?.count || 0) + (todayRequests?.count || 0) + (todayNotifications?.count || 0)
        },
        total: {
          health_checks: totalHealthChecks?.count || 0,
          request_results: totalRequests?.count || 0,
          backend_status: totalBackendStatus?.count || 0,
          telegram_notifications: totalTelegramNotifications?.count || 0,
          total: (totalHealthChecks?.count || 0) + (totalRequests?.count || 0) + 
                 (totalBackendStatus?.count || 0) + (totalTelegramNotifications?.count || 0)
        },
        beijing_date: today
      };
    } catch (error) {
      console.error('获取D1写入统计失败:', error);
      return {
        today: { health_checks: 0, request_results: 0, telegram_notifications: 0, total: 0 },
        total: { health_checks: 0, request_results: 0, backend_status: 0, telegram_notifications: 0, total: 0 },
        beijing_date: getBeijingDateString(),
        error: '获取统计失败'
      };
    }
  }
  
  // 获取增强的D1写入统计
  async getD1WriteStatsEnhanced() {
    try {
      const today = getBeijingDateString(new Date());
      const yesterday = getBeijingDateString(new Date(Date.now() - 24 * 60 * 60 * 1000));
      
      console.log(`统计查询: 今天日期 ${today}, 昨天日期 ${yesterday}`);
      
      // 方法1：使用 SUBSTR 函数提取日期部分进行匹配
      const todayHealthChecks = await this.db
        .prepare('SELECT COUNT(*) as count FROM health_check_results WHERE SUBSTR(beijing_time, 1, 10) = ?')
        .bind(today)
        .first();
        
      const todayRequests = await this.db
        .prepare('SELECT COUNT(*) as count FROM request_results WHERE SUBSTR(beijing_time, 1, 10) = ?')
        .bind(today)
        .first();
        
      const todayNotifications = await this.db
        .prepare('SELECT COUNT(*) as count FROM telegram_notifications WHERE SUBSTR(beijing_time, 1, 10) = ?')
        .bind(today)
        .first();
      
      console.log(`今日数据统计: 健康检查=${todayHealthChecks?.count || 0}, 请求=${todayRequests?.count || 0}, 通知=${todayNotifications?.count || 0}`);
      
      // 获取今日平均响应时间
      const avgResponseTimeResult = await this.db
        .prepare(`
          SELECT 
            AVG(response_time) as avg_response_time,
            COUNT(*) as total_requests,
            SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_requests
          FROM request_results 
          WHERE SUBSTR(beijing_time, 1, 10) = ?
        `)
        .bind(today)
        .first();
      
      const avgResponseTime = avgResponseTimeResult?.avg_response_time || 0;
      const todayRequestCount = avgResponseTimeResult?.total_requests || 0;
      const todaySuccessfulRequests = avgResponseTimeResult?.successful_requests || 0;
      
      // 获取各表总数
      const totalHealthChecks = await this.db
        .prepare('SELECT COUNT(*) as count FROM health_check_results')
        .first();
      
      const totalRequests = await this.db
        .prepare('SELECT COUNT(*) as count FROM request_results')
        .first();
      
      const totalBackendStatus = await this.db
        .prepare('SELECT COUNT(*) as count FROM backend_status')
        .first();
      
      const totalTelegramNotifications = await this.db
        .prepare('SELECT COUNT(*) as count FROM telegram_notifications')
        .first();
      
      // 获取后端权重统计
      const weightStats = await this.db
        .prepare(`
          SELECT 
            AVG(weight) as avg_weight,
            MIN(weight) as min_weight,
            MAX(weight) as max_weight,
            COUNT(*) as backend_count
          FROM backend_status
        `)
        .first();
      
      // 获取最近7天的趋势数据
      const sevenDaysAgo = getBeijingDateString(new Date(Date.now() - 7 * 24 * 60 * 60 * 1000));
      const weeklyTrend = await this.db
        .prepare(`
          SELECT 
            SUBSTR(beijing_time, 1, 10) as date,
            COUNT(*) as count
          FROM health_check_results 
          WHERE SUBSTR(beijing_time, 1, 10) >= ?
          GROUP BY SUBSTR(beijing_time, 1, 10)
          ORDER BY date
        `)
        .bind(sevenDaysAgo)
        .all();
      
      // 计算昨日数据用于对比
      const yesterdayHealthChecks = await this.db
        .prepare('SELECT COUNT(*) as count FROM health_check_results WHERE SUBSTR(beijing_time, 1, 10) = ?')
        .bind(yesterday)
        .first();
      
      const todayTotal = (todayHealthChecks?.count || 0) + 
                        (todayRequests?.count || 0) + 
                        (todayNotifications?.count || 0);
      
      const yesterdayTotal = (yesterdayHealthChecks?.count || 0);
      
      const changePercentage = yesterdayTotal > 0 ? 
        (((todayTotal - yesterdayTotal) / yesterdayTotal) * 100).toFixed(1) : 
        (todayTotal > 0 ? '+100.0' : '0.0');
      
      return {
        today: {
          health_checks: todayHealthChecks?.count || 0,
          request_results: todayRequests?.count || 0,
          telegram_notifications: todayNotifications?.count || 0,
          total: todayTotal,
          avg_response_time: Math.round(avgResponseTime),
          successful_requests: todaySuccessfulRequests,
          total_requests: todayRequestCount
        },
        comparison: {
          today_total: todayTotal,
          yesterday_total: yesterdayTotal,
          change_percentage: `${changePercentage}%`
        },
        total: {
          health_checks: totalHealthChecks?.count || 0,
          request_results: totalRequests?.count || 0,
          backend_status: totalBackendStatus?.count || 0,
          telegram_notifications: totalTelegramNotifications?.count || 0,
          total: (totalHealthChecks?.count || 0) + (totalRequests?.count || 0) + 
                 (totalBackendStatus?.count || 0) + (totalTelegramNotifications?.count || 0)
        },
        weight_statistics: {
          avg_weight: weightStats?.avg_weight || 0,
          min_weight: weightStats?.min_weight || 0,
          max_weight: weightStats?.max_weight || 0,
          backend_count: weightStats?.backend_count || 0
        },
        weekly_trend: weeklyTrend.results || [],
        beijing_date: today,
        last_updated: getBeijingTimeString(),
        debug_info: {
          today_query: today,
          yesterday_query: yesterday,
          query_time: new Date().toISOString()
        }
      };
    } catch (error) {
      console.error('获取增强D1写入统计失败:', error);
      return {
        today: {
          health_checks: 0,
          request_results: 0,
          telegram_notifications: 0,
          total: 0,
          avg_response_time: 0
        },
        comparison: {
          today_total: 0,
          yesterday_total: 0,
          change_percentage: 'N/A'
        },
        total: {
          health_checks: 0,
          request_results: 0,
          backend_status: 0,
          telegram_notifications: 0,
          total: 0
        },
        beijing_date: getBeijingDateString(),
        last_updated: getBeijingTimeString(),
        error: error.message
      };
    }
  }

  // 获取状态页面数据
  async getStatusPageData(env) {
    try {
      // 获取所有后端状态（从 backend_status 表）
      const backendStatus = await this.getAllBackendStatus();
      
      // 获取订阅转换请求数据
      const recentRequests = await this.getRecentRequests(100);
      
      // 获取D1统计
      const d1Stats = await this.getD1WriteStatsEnhanced();
      
      // 获取Telegram通知
      const telegramNotifications = await this.getRecentTelegramNotifications(10);
      
      // 获取Telegram通知总数
      let totalTelegramSent = 0;
      try {
        const totalNotifications = await this.db
          .prepare('SELECT COUNT(*) as count FROM telegram_notifications')
          .first();
        totalTelegramSent = totalNotifications?.count || 0;
      } catch (error) {
        console.log('获取Telegram通知总数失败:', error.message);
      }
      
      // 统计请求数据
      let totalRequests = 0;
      let successfulRequests = 0;
      let failedRequests = 0;
      let totalResponseTime = 0;
      let avgBackendWeight = 0;
      
      if (recentRequests.length > 0) {
        totalRequests = recentRequests.length;
        successfulRequests = recentRequests.filter(req => req.success === 1).length;
        failedRequests = totalRequests - successfulRequests;
        totalResponseTime = recentRequests.reduce((sum, req) => sum + (req.response_time || 0), 0);
        
        // 计算平均后端权重
        const weightedRequests = recentRequests.filter(req => req.backend_weight > 0);
        if (weightedRequests.length > 0) {
          avgBackendWeight = weightedRequests.reduce((sum, req) => sum + (req.backend_weight || 0), 0) / weightedRequests.length;
        }
      }
      
      // 统计后端健康状态 - 直接从 backend_status 表获取
      const backendUrls = JSON.parse(env.BACKEND_URLS || '[]');
      const totalBackends = backendUrls.length;
      let healthyBackends = 0;
      let totalBackendWeight = 0;
      let fastestBackend = null;
      let fastestResponseTime = Infinity;
      
      // 【新增】从健康后端中挑选权重最大的作为可用后端
      let currentAvailableBackend = null;
      let maxWeight = 0;
      
      // 使用 backend_status 表的数据
      if (backendStatus.length > 0) {
        // 计算健康后端数量和总权重
        healthyBackends = backendStatus.filter(b => b.healthy === 1).length;
        totalBackendWeight = backendStatus.reduce((sum, b) => sum + (b.weight || 0), 0);
        
        // 查找最快健康后端
        for (const backend of backendStatus) {
          if (backend.healthy === 1 && backend.avg_response_time && backend.avg_response_time < fastestResponseTime) {
            fastestResponseTime = backend.avg_response_time;
            fastestBackend = backend.backend_url;
          }
        }
        
        // 【新增】从健康后端中挑选权重最大的作为可用后端
        for (const backend of backendStatus) {
          if (backend.healthy === 1 && backend.weight > maxWeight) {
            maxWeight = backend.weight;
            currentAvailableBackend = backend.backend_url;
          }
        }
        
        // 如果权重最大的后端没找到，使用最快后端
        if (!currentAvailableBackend && fastestBackend) {
          currentAvailableBackend = fastestBackend;
        }
      }
      
      const avgWeight = backendStatus.length > 0 ? totalBackendWeight / backendStatus.length : 0;
      const concurrentStats = healthCheckController.getStats();
      
      return {
        latestHealthCheck: null, // 不再使用 health_check_results 表
        recentRequests: recentRequests,
        backendStatus: backendStatus, // 使用 backend_status 表数据
        telegramNotifications: telegramNotifications,
        telegramTotalSent: totalTelegramSent,
        d1Stats: d1Stats,
        requestStats: {
          total: totalRequests,
          successful: successfulRequests,
          failed: failedRequests,
          successRate: totalRequests > 0 ? (successfulRequests / totalRequests * 100).toFixed(1) + '%' : '0%',
          avgResponseTime: totalRequests > 0 ? Math.round(totalResponseTime / totalRequests) : 0,
          avgBackendWeight: Math.round(avgBackendWeight)
        },
        healthStats: {
          totalBackends: totalBackends,
          healthyBackends: healthyBackends,
          unhealthyBackends: totalBackends - healthyBackends,
          healthRate: totalBackends > 0 ? (healthyBackends / totalBackends * 100).toFixed(1) + '%' : '0%',
          avgWeight: Math.round(avgWeight)
        },
        concurrentStats: concurrentStats,
        // 【修改】使用权重最大的健康后端作为可用后端
        availableBackend: currentAvailableBackend,
        fastestResponseTime: fastestResponseTime === Infinity ? 0 : fastestResponseTime,
        backendUrls: backendUrls,
        timestamp: Date.now(),
        beijingTime: getBeijingTimeString(),
        dataStatus: 'ok'
      };
    } catch (error) {
      console.error('获取状态页面数据失败:', error);
      return {
        latestHealthCheck: null,
        recentRequests: [],
        backendStatus: [],
        telegramNotifications: [],
        telegramTotalSent: 0,
        d1Stats: null,
        requestStats: {
          total: 0,
          successful: 0,
          failed: 0,
          successRate: '0%',
          avgResponseTime: 0,
          avgBackendWeight: 0
        },
        healthStats: {
          totalBackends: 0,
          healthyBackends: 0,
          unhealthyBackends: 0,
          healthRate: '0%',
          avgWeight: 0
        },
        concurrentStats: healthCheckController.getStats(),
        availableBackend: null,
        fastestResponseTime: 0,
        backendUrls: [],
        timestamp: Date.now(),
        beijingTime: getBeijingTimeString(),
        dataStatus: 'error',
        errorMessage: error.message
      };
    }
  }

  // 清理旧数据
  async cleanupOldData(daysToKeep = 7) {
    try {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);
      const cutoffStr = cutoffDate.toISOString();
      
      const healthCheckResult = await this.db
        .prepare('DELETE FROM health_check_results WHERE timestamp < ?')
        .bind(cutoffStr)
        .run();
      
      const requestResult = await this.db
        .prepare('DELETE FROM request_results WHERE timestamp < ?')
        .bind(cutoffStr)
        .run();
      
      const telegramNotificationResult = await this.db
        .prepare('DELETE FROM telegram_notifications WHERE sent_time < ?')
        .bind(cutoffStr)
        .run();
      
      console.log(`数据清理完成: 删除了 ${healthCheckResult.changes} 条健康检查记录, ${requestResult.changes} 条请求记录, ${telegramNotificationResult.changes} 条Telegram通知记录`);
      
      return {
        health_checks_deleted: healthCheckResult.changes,
        requests_deleted: requestResult.changes,
        telegram_notifications_deleted: telegramNotificationResult.changes
      };
    } catch (error) {
      console.error('清理旧数据失败:', error);
      return null;
    }
  }

  // 保存错误日志
  async saveErrorLog(errorData, requestId) {
    try {
      const stmt = this.db.prepare(`
        INSERT INTO error_logs 
        (request_id, context, error_message, stack_trace, timestamp, beijing_time)
        VALUES (?, ?, ?, ?, ?, ?)
      `);
      
      const result = await stmt.bind(
        errorData.request_id || requestId,
        errorData.context || 'unknown',
        errorData.error_message || 'Unknown error',
        errorData.stack_trace || '',
        errorData.timestamp || new Date().toISOString(),
        getBeijingTimeString()
      ).run();
      
      return result;
    } catch (error) {
      console.warn(`[${requestId}] 保存错误日志失败:`, error.message);
      return null;
    }
  }
}