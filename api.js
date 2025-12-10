import { 
  getBackendsFromEnv, 
  getConfig, 
  getBeijingTimeString, 
  getBeijingDateString,
  logError
} from './utils.js';
import { healthCheckController } from './concurrency.js';
import { SmartWeightedLoadBalancer } from './load-balancer.js';
import { ResilientTelegramNotifier } from './notifier.js';
import { SafeD1Database } from './database.js';
import { performFullHealthCheck } from './core.js';

// API端点处理
export async function handleApiRequest(request, env, requestId) {
  const url = new URL(request.url);
  const db = env.DB ? new SafeD1Database(env.DB, env) : null;
  
  // 数据库初始化API
  if (url.pathname === '/api/init-database') {
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
      
      // 如果是GET请求，返回初始化表单
      if (request.method === 'GET') {
        const html = `
          <!DOCTYPE html>
          <html>
          <head>
            <title>初始化数据库</title>
            <style>
              body { font-family: Arial, sans-serif; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; }
              .container { max-width: 600px; margin: 0 auto; background: white; padding: 30px; border-radius: 12px; box-shadow: 0 8px 32px rgba(0,0,0,0.1); }
              h1 { color: #333; text-align: center; margin-bottom: 20px; }
              button { padding: 12px 24px; background: #28a745; color: white; border: none; border-radius: 6px; cursor: pointer; font-size: 16px; width: 100%; margin-top: 20px; }
              button:hover { background: #218838; }
              .result { margin-top: 20px; padding: 15px; background: #f8f9fa; border-radius: 8px; border: 1px solid #e9ecef; }
              .success { color: #28a745; }
              .error { color: #dc3545; }
            </style>
          </head>
          <body>
            <div class="container">
              <h1>🚀 初始化数据库</h1>
              <p>点击按钮初始化数据库表结构，将创建以下表：</p>
              <ul>
                <li>health_check_results - 健康检查结果表</li>
                <li>backend_status - 后端状态表</li>
                <li>request_results - 请求结果表</li>
                <li>telegram_notifications - Telegram通知表</li>
                <li>error_logs - 错误日志表</li>
              </ul>
              <button onclick="initDatabase()">🚀 开始初始化数据库</button>
              <div id="result" class="result"></div>
              <script>
                async function initDatabase() {
                  const result = document.getElementById('result');
                  const button = document.querySelector('button');
                  
                  button.disabled = true;
                  button.innerHTML = '🔄 正在初始化...';
                  result.innerHTML = '<div class="info">正在执行数据库初始化，请稍候...</div>';
                  
                  try {
                    const response = await fetch('/api/init-database', {
                      method: 'POST'
                    });
                    const data = await response.json();
                    
                    if (data.success) {
                      result.innerHTML = '<div class="success"><strong>✅ 数据库初始化成功!</strong><br><pre>' + JSON.stringify(data.result, null, 2) + '</pre></div>';
                    } else {
                      result.innerHTML = '<div class="error"><strong>❌ 数据库初始化失败!</strong><br><pre>' + JSON.stringify(data, null, 2) + '</pre></div>';
                    }
                  } catch (error) {
                    result.innerHTML = '<div class="error"><strong>❌ 请求失败:</strong><br>' + error.message + '</div>';
                  } finally {
                    button.disabled = false;
                    button.innerHTML = '🔄 重新初始化数据库';
                  }
                }
              </script>
            </div>
          </body>
          </html>
        `;
        
        return new Response(html, {
          headers: { 'Content-Type': 'text/html; charset=utf-8' }
        });
      }
      
      // POST请求直接执行初始化
      if (request.method === 'POST') {
        const { initDatabase } = await import('./init-database.js');
        const result = await initDatabase(env.DB);
        
        return new Response(JSON.stringify({
          success: result.success,
          message: result.success ? '数据库初始化完成' : '数据库初始化失败',
          result: result,
          request_id: requestId,
          timestamp: new Date().toISOString(),
          beijing_time: getBeijingTimeString()
        }), {
          headers: { 'Content-Type': 'application/json; charset=utf-8' }
        });
      }
      
      return new Response(JSON.stringify({ 
        error: '方法不允许，请使用 GET 或 POST',
        request_id: requestId
      }), {
        status: 405,
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
  
  // 健康检查API
  if (url.pathname === '/api/health' && request.method === 'GET') {
    try {
      const backends = getBackendsFromEnv(env);
      
      let d1Stats = null;
      let recentHealthChecks = [];
      let recentRequests = [];
      let backendStatus = [];
      let telegramNotifications = [];
      
      if (db) {
        try {
          d1Stats = await db.getD1WriteStatsEnhanced();
          recentHealthChecks = await db.getRecentHealthChecks(5);
          recentRequests = await db.getRecentRequests(10);
          backendStatus = await db.getAllBackendStatus();
          telegramNotifications = await db.getRecentTelegramNotifications(5);
        } catch (dbError) {
          logError('获取D1数据失败', dbError, requestId);
        }
      }
      
      const totalCount = backends.length;
      const lbAlgorithm = getConfig(env, 'LB_ALGORITHM', 'weighted_round_robin');
      const concurrentStats = healthCheckController.getStats();
      
      // 获取负载均衡器统计
      const loadBalancer = new SmartWeightedLoadBalancer(env);
      const weightStats = loadBalancer.getWeightStatistics();
      
      return new Response(JSON.stringify({
        status: 'ok',
        timestamp: new Date().toISOString(),
        beijing_time: getBeijingTimeString(),
        backends_count: totalCount,
        backends: backends,
        d1_stats: d1Stats,
        streaming_enabled: getConfig(env, 'ENABLE_STREAMING_PROXY', true),
        d1_data_available: {
          recent_health_checks_count: recentHealthChecks.length,
          recent_requests_count: recentRequests.length,
          backend_status_count: backendStatus.length,
          telegram_notifications_count: telegramNotifications.length
        },
        concurrent_stats: concurrentStats,
        weight_statistics: weightStats,
        load_balancer_config: {
          algorithm: lbAlgorithm,
          weight_adjustment_factor: getConfig(env, 'WEIGHT_ADJUSTMENT_FACTOR', 0.3),
          weight_recovery_rate: getConfig(env, 'WEIGHT_RECOVERY_RATE', 2),
          base_weight: getConfig(env, 'BASE_WEIGHT', 50),
          max_weight: getConfig(env, 'MAX_WEIGHT', 100),
          min_weight: getConfig(env, 'MIN_WEIGHT', 10)
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
      
      const d1Stats = await db.getD1WriteStatsEnhanced();
      const recentHealthChecks = await db.getRecentHealthChecks(20);
      const recentRequests = await db.getRecentRequests(50);
      const backendStatus = await db.getAllBackendStatus();
      const telegramNotifications = await db.getRecentTelegramNotifications(20);
      const concurrentStats = healthCheckController.getStats();
      
      // 获取负载均衡器统计
      const loadBalancer = new SmartWeightedLoadBalancer(env);
      const weightStats = loadBalancer.getWeightStatistics();
      
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        stats: {
          d1_write_stats: d1Stats,
          recent_health_checks: recentHealthChecks,
          recent_requests: recentRequests.slice(0, 20),
          backend_status: backendStatus,
          telegram_notifications: telegramNotifications,
          weight_statistics: weightStats,
          table_counts: {
            health_check_results: recentHealthChecks.length,
            request_results: recentRequests.length,
            backend_status: backendStatus.length,
            telegram_notifications: telegramNotifications.length
          },
          concurrent_stats: concurrentStats
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
        weight_statistics: checkResults.weightStatistics,
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
      const checkResults = await performFullHealthCheck(db, requestId, env);
    
      let d1Stats = null;
      if (db) {
        d1Stats = await db.getD1WriteStatsEnhanced();
      }
    
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        message: '健康检查已完成并写入D1数据库',
        results: checkResults.results,
        available_backend: checkResults.availableBackend,
        fastest_response_time: checkResults.fastestResponseTime,
        backend_changed: checkResults.backendChanged,
        weight_statistics: checkResults.weightStatistics,
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
      const backends = getBackendsFromEnv(env);
      
      return new Response(JSON.stringify({ 
        backends,
        config: {
          cache_ttl: getConfig(env, 'CACHE_TTL', 60000),
          health_check_timeout: getConfig(env, 'HEALTH_CHECK_TIMEOUT', 2000),
          concurrent_health_checks: getConfig(env, 'CONCURRENT_HEALTH_CHECKS', 5),
          fast_check_timeout: getConfig(env, 'FAST_CHECK_TIMEOUT', 800),
          fast_check_cache_ttl: getConfig(env, 'FAST_CHECK_CACHE_TTL', 2000),
          notify_on_request: getConfig(env, 'NOTIFY_ON_REQUEST', true),
          notify_on_health_change: getConfig(env, 'NOTIFY_ON_HEALTH_CHANGE', true),
          notify_on_error: getConfig(env, 'NOTIFY_ON_ERROR', true),
          lb_algorithm: getConfig(env, 'LB_ALGORITHM', 'weighted_round_robin'),
          enable_streaming_proxy: getConfig(env, 'ENABLE_STREAMING_PROXY', true),
          weight_adjustment_factor: getConfig(env, 'WEIGHT_ADJUSTMENT_FACTOR', 0.3),
          weight_recovery_rate: getConfig(env, 'WEIGHT_RECOVERY_RATE', 2),
          base_weight: getConfig(env, 'BASE_WEIGHT', 50),
          max_weight: getConfig(env, 'MAX_WEIGHT', 100),
          min_weight: getConfig(env, 'MIN_WEIGHT', 10),
          response_time_window: getConfig(env, 'RESPONSE_TIME_WINDOW', 10),
          health_threshold: getConfig(env, 'HEALTH_THRESHOLD', 0.7),
          failure_penalty: getConfig(env, 'FAILURE_PENALTY', 15),
          success_boost: getConfig(env, 'SUCCESS_BOOST', 8)
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
      healthCheckController.reset();
      
      // 重置负载均衡器权重
      const loadBalancer = new SmartWeightedLoadBalancer(env);
      loadBalancer.resetAllWeights();
      
      return new Response(JSON.stringify({
        success: true,
        message: '并发控制器和负载均衡器权重已重置',
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
      const backends = getBackendsFromEnv(env);
      const results = {};
      const loadBalancer = new SmartWeightedLoadBalancer(env);
      
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
          const responseTimeScore = calculateResponseTimeScore(responseTime, env);
          
          let version = '未知版本';
          if (response.status === 200) {
            const text = await response.text();
            version = text.trim() || '未知版本';
          }
          
          // 计算权重
          const healthResult = {
            healthy: response.status === 200,
            responseTime,
            responseTimeScore,
            status: response.status,
            version: version
          };
          
          const weight = await loadBalancer.calculateBackendWeight(url, healthResult, db, requestId);
          
          results[url] = {
            status: response.status,
            responseTime,
            responseTimeScore,
            weight,
            healthy: response.status === 200,
            version: version
          };
        } catch (error) {
          const responseTime = Date.now() - startTime;
          const healthResult = {
            healthy: false,
            responseTime,
            responseTimeScore: 0,
            status: 0,
            version: '未知版本',
            error: error.name
          };
          
          const weight = await loadBalancer.calculateBackendWeight(url, healthResult, db, requestId);
          
          results[url] = {
            status: 0,
            responseTime,
            responseTimeScore: 0,
            weight,
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
  
  // 错误日志API
  if (url.pathname === '/api/error-logs' && request.method === 'GET') {
    try {
      let errorLogs = [];
      if (db) {
        try {
          const { results } = await db.db
            .prepare('SELECT * FROM error_logs ORDER BY id DESC LIMIT 50')
            .all();
          errorLogs = results || [];
        } catch (e) {
          // 如果error_logs表不存在，忽略
        }
      }
      
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        error_logs: errorLogs,
        total_errors: errorLogs.length,
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
  
  // Telegram通知记录API
  if (url.pathname === '/api/telegram-notifications' && request.method === 'GET') {
    try {
      let telegramNotifications = [];
      
      if (db) {
        telegramNotifications = await db.getRecentTelegramNotifications(50);
      }
      
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        telegram_notifications: telegramNotifications,
        total_notifications: telegramNotifications.length,
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
  
  // 测试Telegram通知API
  if (url.pathname === '/api/test-telegram-notification' && request.method === 'POST') {
    try {
      const notifier = new ResilientTelegramNotifier(env);
      const notificationData = {
        type: 'request',
        request_id: requestId,
        client_ip: '127.0.0.1',
        backend_url: 'https://test-backend.example.com',
        backend_selection_time: 50,
        response_time: 200,
        status_code: 200,
        success: true,
        total_time: 250,
        backend_weight: 75,
        error: '',
        env: env
      };
      
      const sent = await notifier.sendNotification(notificationData, requestId, { waitUntil: (promise) => promise });
      
      return new Response(JSON.stringify({
        success: sent.success,
        message: sent.success ? 'Telegram通知测试发送成功' : 'Telegram通知测试发送失败',
        request_id: requestId,
        timestamp: new Date().toISOString(),
        beijing_time: getBeijingTimeString(),
        details: sent
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
  
  // 负载均衡算法测试API
  if (url.pathname === '/api/lb-test' && request.method === 'GET') {
    try {
      const backends = getBackendsFromEnv(env);
      const lbAlgorithm = getConfig(env, 'LB_ALGORITHM', 'weighted_round_robin');
      const loadBalancer = new SmartWeightedLoadBalancer(env);
      
      const mockBackends = new Map();
      backends.forEach((url, index) => {
        const responseTime = 100 + Math.random() * 400;
        const responseTimeScore = calculateResponseTimeScore(responseTime, env);
        
        mockBackends.set(url, {
          healthy: index % 3 !== 0,
          responseTime: responseTime,
          responseTimeScore: responseTimeScore,
          version: 'subconverter v1.0.0'
        });
      });
      
      const selectedBackend = await loadBalancer.selectOptimalBackend(mockBackends, requestId, env);
      const weightStats = loadBalancer.getWeightStatistics();
      
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        lb_algorithm: lbAlgorithm,
        available_backends: backends.length,
        healthy_backends: Array.from(mockBackends.values()).filter(b => b.healthy).length,
        selected_backend: selectedBackend,
        backend_details: Object.fromEntries(mockBackends),
        weight_statistics: weightStats,
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
  
  // 诊断API
  if (url.pathname === '/api/diagnose' && request.method === 'GET') {
    try {
      const db = env.DB ? new SafeD1Database(env.DB, env) : null;
      
      const dbCheck = db ? '✅ 正常' : '❌ 未配置';
      
      let tablesExist = { health_check_results: false, backend_status: false };
      if (db) {
        try {
          const healthCheckTable = await db.db
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='health_check_results'")
            .first();
          tablesExist.health_check_results = !!healthCheckTable;
          
          const backendStatusTable = await db.db
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='backend_status'")
            .first();
          tablesExist.backend_status = !!backendStatusTable;
        } catch (e) {
          tablesExist = { error: e.message };
        }
      }
      
      const backends = getBackendsFromEnv(env);
      
      let latestCheck = null;
      let recordCount = 0;
      if (db && tablesExist.health_check_results) {
        try {
          const { results } = await db.db
            .prepare('SELECT * FROM health_check_results ORDER BY id DESC LIMIT 1')
            .all();
          latestCheck = results[0] || null;
          recordCount = results.length;
        } catch (e) {
          latestCheck = { error: e.message };
        }
      }
      
      const concurrentStats = healthCheckController.getStats();
      const loadBalancer = new SmartWeightedLoadBalancer(env);
      const weightStats = loadBalancer.getWeightStatistics();
      
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        diagnosis: {
          timestamp: new Date().toISOString(),
          beijing_time: getBeijingTimeString(),
          database: {
            status: dbCheck,
            tables: tablesExist,
            total_records: recordCount
          },
          backends: {
            configured: backends.length,
            list: backends
          },
          health_check: {
            latest: latestCheck,
            concurrent_stats: concurrentStats
          },
          load_balancer: {
            algorithm: getConfig(env, 'LB_ALGORITHM', 'weighted_round_robin'),
            weight_statistics: weightStats,
            backend_count: weightStats.length
          },
          environment: {
            LB_ALGORITHM: getConfig(env, 'LB_ALGORITHM', '未配置'),
            NOTIFY_ON_REQUEST: getConfig(env, 'NOTIFY_ON_REQUEST', false),
            CONCURRENT_HEALTH_CHECKS: getConfig(env, 'CONCURRENT_HEALTH_CHECKS', 5),
            WEIGHT_ADJUSTMENT_FACTOR: getConfig(env, 'WEIGHT_ADJUSTMENT_FACTOR', 0.3),
            BASE_WEIGHT: getConfig(env, 'BASE_WEIGHT', 50)
          }
        }
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
  
  // 重置后端权重API
  if (url.pathname === '/api/reset-weights' && request.method === 'POST') {
    try {
      const loadBalancer = new SmartWeightedLoadBalancer(env);
      const backends = getBackendsFromEnv(env);
      
      for (const backendUrl of backends) {
        loadBalancer.resetBackendWeight(backendUrl);
      }
      
      return new Response(JSON.stringify({
        success: true,
        message: `已重置 ${backends.length} 个后端的权重`,
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
  
  // 获取后端权重统计API
  if (url.pathname === '/api/weight-stats' && request.method === 'GET') {
    try {
      const loadBalancer = new SmartWeightedLoadBalancer(env);
      const weightStats = loadBalancer.getWeightStatistics();
      
      return new Response(JSON.stringify({
        success: true,
        request_id: requestId,
        weight_statistics: weightStats,
        total_backends: weightStats.length,
        avg_weight: weightStats.length > 0 ? 
          weightStats.reduce((sum, stat) => sum + stat.weight, 0) / weightStats.length : 0,
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
  
  return new Response(JSON.stringify({ error: '未找到API端点' }), {
    status: 404,
    headers: { 'Content-Type': 'application/json; charset=utf-8' }
  });
}

// 计算响应时间得分
function calculateResponseTimeScore(responseTime, env) {
  const MAX_RESPONSE_TIME = getConfig(env, 'HEALTH_CHECK_TIMEOUT', 2000);
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