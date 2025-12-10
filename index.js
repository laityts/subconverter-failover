import { 
  generateRequestId, 
  validateConfig, 
  logError 
} from './utils.js';
import { ResilientTelegramNotifier } from './notifier.js';

// 全局错误处理
function handleGlobalError(error, requestId, env, ctx) {
  logError('全局错误处理', error, requestId);
  
  const notifier = new ResilientTelegramNotifier(env);
  const errorData = {
    type: 'error',
    request_id: requestId,
    error_type: 'global_error',
    error_message: error.message,
    env: env
  };
  
  ctx.waitUntil(notifier.sendNotification(errorData, requestId, ctx));
  
  return new Response('服务器内部错误，请稍后重试', {
    status: 500,
    headers: { 
      'Content-Type': 'text/plain; charset=utf-8',
      'X-Request-ID': requestId
    }
  });
}

// 主请求处理逻辑
async function handleSubconverterRequestMain(request, env, ctx, requestId) {
  const url = new URL(request.url);
  const backends = JSON.parse(env.BACKEND_URLS || '[]');
  
  if (backends.length === 0) {
    return new Response('未配置后端服务器，请在Cloudflare Dashboard中配置BACKEND_URLS', {
      status: 503,
      headers: { 
        'Content-Type': 'text/plain; charset=utf-8',
        'X-Request-ID': requestId
      }
    });
  }
  
  // 创建数据库实例（如果可用）
  let safeDB = null;
  if (env.DB) {
    const { SafeD1Database } = await import('./database.js');
    safeDB = new SafeD1Database(env.DB, env);
  }
  
  // 导入核心函数
  const { 
    smartFindAvailableBackend,
    streamProxyRequest,
    handleSubconverterRequest
  } = await import('./core.js');
  
  const { 
    backend: backendUrl, 
    selectionTime: backendSelectionTime, 
    algorithm 
  } = await smartFindAvailableBackend(safeDB, requestId, env, request);
  
  if (!backendUrl) {
    console.log(`[${requestId}] 无可用后端，返回503`);
    
    const notifier = new ResilientTelegramNotifier(env);
    const clientIp = request.headers.get('cf-connecting-ip') || 'unknown';
    const errorData = {
      type: 'error',
      request_id: requestId,
      error_type: 'no_available_backend',
      error_message: '所有后端服务均不可用',
      client_ip: clientIp,
      env: env
    };
    
    ctx.waitUntil(notifier.sendNotification(errorData, requestId, ctx));
    
    return new Response('所有后端服务均不可用，请稍后重试', {
      status: 503,
      headers: { 
        'Content-Type': 'text/plain; charset=utf-8',
        'X-Request-ID': requestId,
        'X-Backend-Selection-Time': `${backendSelectionTime}ms`
      }
    });
  }
  
  console.log(`[${requestId}] 使用后端: ${backendUrl}, 选择算法: ${algorithm}`);
  
  // 获取后端权重
  let backendWeight = 0;
  try {
    if (safeDB) {
      const backendStatus = await safeDB.getBackendStatus(backendUrl);
      backendWeight = backendStatus?.weight || 50;
    } else {
      backendWeight = 50;
    }
  } catch (error) {
    console.warn(`[${requestId}] 获取后端权重失败: ${error.message}`);
    backendWeight = 50;
  }
  
  const enableStreaming = env.ENABLE_STREAMING_PROXY === 'true' || true;
  
  if (enableStreaming) {
    return await streamProxyRequest(
      request, backendUrl, backendSelectionTime, requestId, env, ctx, backendWeight
    );
  } else {
    return await handleSubconverterRequest(
      request, backendUrl, backendSelectionTime, requestId, env, ctx, backendWeight
    );
  }
}

// ==================== 主处理函数 ====================
export default {
  async fetch(request, env, ctx) {
    const requestId = generateRequestId();
    const url = new URL(request.url);
    
    console.log(`[${requestId}] 收到请求: ${request.method} ${url.pathname}`);
    
    try {
      validateConfig(env, requestId);
      
      // 首页和状态页面
      if (url.pathname === '/' || url.pathname === '/status') {
        try {
          const { createEnhancedStatusPage } = await import('./status-page.js');
          // 状态页面需要数据库，所以我们在这里创建
          let safeDB = null;
          if (env.DB) {
            const { SafeD1Database } = await import('./database.js');
            safeDB = new SafeD1Database(env.DB, env);
          }
          return await createEnhancedStatusPage(requestId, env, safeDB);
        } catch (error) {
          logError('创建状态页面失败', error, requestId);
          return new Response('状态页面暂时不可用，请稍后重试', {
            status: 500,
            headers: { 'Content-Type': 'text/html; charset=utf-8' }
          });
        }
      }
      
      // API处理
      if (url.pathname.startsWith('/api/')) {
        try {
          const { handleApiRequest } = await import('./api.js');
          return await handleApiRequest(request, env, requestId);
        } catch (error) {
          logError('API处理失败', error, requestId);
          return new Response(JSON.stringify({ 
            error: 'API处理失败: ' + error.message,
            request_id: requestId
          }), {
            status: 500,
            headers: { 'Content-Type': 'application/json; charset=utf-8' }
          });
        }
      }
      
      // 订阅转换请求处理
      return await handleSubconverterRequestMain(request, env, ctx, requestId);
      
    } catch (error) {
      return handleGlobalError(error, requestId, env, ctx);
    }
  },
  
  async scheduled(event, env, ctx) {
    const requestId = generateRequestId();
    console.log(`[${requestId}] Cron触发，开始执行健康检查`);
    
    try {
      // 在定时任务中创建数据库实例
      let safeDB = null;
      if (env.DB) {
        const { SafeD1Database } = await import('./database.js');
        safeDB = new SafeD1Database(env.DB, env);
      }
      
      const { performFullHealthCheck } = await import('./core.js');
      const checkResults = await performFullHealthCheck(safeDB, requestId, env, ctx);
      
      console.log(`[${requestId}] Cron健康检查完成，检查了 ${checkResults.totalBackends} 个后端，${checkResults.healthyBackends} 个健康`);
      
      // 周日执行D1数据清理
      const now = new Date();
      if (now.getUTCDay() === 0 && safeDB) {
        console.log(`[${requestId}] 周日执行D1数据清理`);
        ctx.waitUntil(safeDB.cleanupOldData(7));
      }
      
      // 如果发现后端状态变化，发送Telegram通知
      if (safeDB && checkResults.healthyBackends === 0) {
        const notifier = new ResilientTelegramNotifier(env);
        const notificationData = {
          type: 'health_change',
          change_type: '所有后端不可用',
          current_backend: null,
          healthy_backends: 0,
          total_backends: checkResults.totalBackends,
          response_time: 0,
          reason: '定时健康检查发现所有后端均不可用',
          weight_statistics: checkResults.weightStatistics,
          env: env
        };
        
        ctx.waitUntil(notifier.sendNotification(notificationData, requestId, ctx));
      }
      
    } catch (error) {
      logError('Cron健康检查失败', error, requestId);
      
      const notifier = new ResilientTelegramNotifier(env);
      const errorData = {
        type: 'error',
        request_id: requestId,
        error_type: 'cron_health_check_failed',
        error_message: error.message,
        env: env
      };
      
      ctx.waitUntil(notifier.sendNotification(errorData, requestId, ctx));
    }
  }
};