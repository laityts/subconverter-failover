// core.js - 核心业务逻辑
import { 
  getBackendsFromEnv, 
  getConfig, 
  logError,
  getBeijingTimeString 
} from './utils.js';
import { healthCheckController } from './concurrency.js';
import { PriorityHealthCheck } from './health-check.js';
import { SmartWeightedLoadBalancer } from './load-balancer.js';
import { ResilientTelegramNotifier } from './notifier.js';
import { SafeD1Database } from './database.js';

// 获取后端列表
export async function getBackends(env, requestId) {
  try {
    const backends = getBackendsFromEnv(env);
    return backends;
  } catch (error) {
    logError('获取后端列表失败', error, requestId);
    return [];
  }
}

// 获取上次可用后端
export async function getLastAvailableBackend(db, requestId) {
  try {
    if (db) {
      try {
        // 从 backend_status 表获取健康的后端
        const backendStatus = await db.getAllBackendStatus();
        const healthyBackends = backendStatus.filter(b => b.healthy === 1);
        
        if (healthyBackends.length > 0) {
          // 按权重排序，返回权重最高的健康后端
          healthyBackends.sort((a, b) => (b.weight || 0) - (a.weight || 0));
          return healthyBackends[0].backend_url;
        }
      } catch (dbError) {
        logError('从backend_status获取上次可用后端失败', dbError, requestId);
      }
    }
    
    return null;
  } catch (error) {
    logError('获取上次可用后端失败', error, requestId);
    return null;
  }
}

// 获取健康高权重后端（新函数）
export async function getHealthyHighWeightBackend(db, requestId) {
  try {
    if (db) {
      try {
        // 从 backend_status 表获取健康的后端，按权重排序
        const backendStatus = await db.getAllBackendStatus();
        const healthyBackends = backendStatus
          .filter(b => b.healthy === 1)
          .sort((a, b) => (b.weight || 0) - (a.weight || 0));
        
        if (healthyBackends.length > 0) {
          return healthyBackends[0].backend_url;
        }
      } catch (dbError) {
        logError('获取健康高权重后端失败', dbError, requestId);
      }
    }
    
    return null;
  } catch (error) {
    logError('获取健康高权重后端失败', error, requestId);
    return null;
  }
}

// 批量健康检查 - 修复：增加 skipNormalPriority 参数
export async function batchHealthChecks(urls, requestId, env, skipNormalPriority = false) {
  const controller = healthCheckController;
  const priorityChecker = new PriorityHealthCheck(env);
  
  const groups = {
    highPriority: [],
    normalPriority: []
  };
  
  const db = env.DB ? new SafeD1Database(env.DB, env) : null;
  const lastBackend = db ? await getLastAvailableBackend(db, requestId) : null;
  
  urls.forEach(url => {
    if (url === lastBackend) {
      groups.highPriority.push(url);
    } else {
      groups.normalPriority.push(url);
    }
  });
  
  const results = new Map();
  
  console.log(`[${requestId}] 开始批量健康检查，高优先级: ${groups.highPriority.length} 个，普通优先级: ${groups.normalPriority.length} 个，跳过普通检查: ${skipNormalPriority}`);
  
  const highPriorityPromises = groups.highPriority.map(async url => {
    try {
      const result = await priorityChecker.priorityCheck(url, requestId);
      results.set(url, result);
    } catch (error) {
      results.set(url, {
        healthy: false,
        error: error.name,
        responseTime: null,
        responseTimeScore: 0,
        priority: 'high'
      });
    }
  });
  
  await Promise.allSettled(highPriorityPromises);
  
  // 修复：只有当 skipNormalPriority 为 true 且找到了健康后端时才跳过普通检查
  const healthyBackends = Array.from(results.entries())
    .filter(([_, result]) => result.healthy);
  
  if (skipNormalPriority && healthyBackends.length > 0) {
    console.log(`[${requestId}] 高优先级检查中找到健康后端，跳过普通优先级检查`);
    return results;
  }
  
  console.log(`[${requestId}] 执行普通优先级检查，数量: ${groups.normalPriority.length}`);
  
  const normalPriorityPromises = groups.normalPriority.map(async url => {
    try {
      const result = await priorityChecker.fullCheck(url, requestId);
      results.set(url, result);
    } catch (error) {
      results.set(url, {
        healthy: false,
        error: error.name,
        responseTime: null,
        responseTimeScore: 0,
        priority: 'normal'
      });
    }
  });
  
  await Promise.allSettled(normalPriorityPromises);
  
  console.log(`[${requestId}] 健康检查并发统计:`, controller.getStats());
  
  return results;
}

// 智能查找可用后端（修改：先使用高权重后端，失败则执行完整健康检查）
export async function smartFindAvailableBackend(db, requestId, env, request = null) {
  const backends = await getBackends(env, requestId);
  
  if (backends.length === 0) {
    return { backend: null, selectionTime: 0, algorithm: 'none' };
  }
  
  const selectionStartTime = Date.now();
  const loadBalancer = new SmartWeightedLoadBalancer(env);
  
  // 1. 首先尝试使用数据库中高权重的健康后端
  let highWeightBackend = null;
  if (db) {
    highWeightBackend = await getHealthyHighWeightBackend(db, requestId);
  }
  
  if (highWeightBackend) {
    try {
      const priorityChecker = new PriorityHealthCheck(env);
      const fastCheck = await priorityChecker.priorityCheck(
        highWeightBackend, 
        `${requestId}-highweight-check`
      );
      
      if (fastCheck.healthy) {
        const selectionTime = Date.now() - selectionStartTime;
        console.log(`[${requestId}] 使用高权重后端: ${highWeightBackend}, 响应时间: ${fastCheck.responseTime}ms, 得分: ${fastCheck.responseTimeScore}`);
        
        // 更新该后端状态
        const targetWeight = await loadBalancer.calculateBackendWeight(highWeightBackend, fastCheck, db, requestId);
        await db.updateBackendStatusWithWeight(highWeightBackend, fastCheck, targetWeight, requestId);
        
        return { 
          backend: highWeightBackend, 
          selectionTime,
          algorithm: 'high_weight_cached',
          weight: targetWeight
        };
      } else {
        console.log(`[${requestId}] 高权重后端检查失败: ${highWeightBackend}, 开始完整健康检查`);
      }
    } catch (error) {
      console.log(`[${requestId}] 高权重后端检查异常: ${error.message}, 开始完整健康检查`);
    }
  }
  
  // 2. 高权重后端不可用，执行完整健康检查
  console.log(`[${requestId}] 开始完整健康检查 ${backends.length} 个后端`);
  
  // 修复：请求处理时跳过普通优先级检查以提高响应速度
  const checkResults = await batchHealthChecks(backends, requestId, env, true);
  
  const healthyBackends = new Map(
    Array.from(checkResults.entries())
      .filter(([_, result]) => result.healthy)
  );
  
  let selectedBackend = null;
  let algorithm = 'weighted_round_robin';
  
  if (healthyBackends.size > 0) {
    selectedBackend = await loadBalancer.selectOptimalBackend(
      healthyBackends, 
      requestId, 
      env, 
      db
    );
    algorithm = 'smart_weighted_round_robin';
  } else {
    console.log(`[${requestId}] 无健康后端，尝试降级策略`);
    
    const backendsWithResponse = Array.from(checkResults.entries())
      .filter(([_, result]) => result.responseTime !== null);
    
    if (backendsWithResponse.length > 0) {
      backendsWithResponse.sort((a, b) => a[1].responseTime - b[1].responseTime);
      selectedBackend = backendsWithResponse[0][0];
      algorithm = 'degraded_fastest';
    }
  }
  
  const selectionTime = Date.now() - selectionStartTime;
  
  if (selectedBackend) {
    console.log(`[${requestId}] 选择后端: ${selectedBackend}, 算法: ${algorithm}, 选择耗时: ${selectionTime}ms`);
  } else {
    console.log(`[${requestId}] 所有后端均不可用`);
  }
  
  // 3. 【修复】无论是否选择到后端，都要更新所有后端状态
  if (db && checkResults.size > 0) {
    const updatePromises = [];
    
    for (const [url, health] of checkResults.entries()) {
      const targetWeight = await loadBalancer.calculateBackendWeight(url, health, db, requestId);
      const updatePromise = db.updateBackendStatusWithWeight(url, health, targetWeight, requestId)
        .catch(error => {
          console.error(`[${requestId}] 更新后端状态失败 ${url}: ${error.message}`);
        });
      
      updatePromises.push(updatePromise);
    }
    
    // 等待所有后端状态更新完成
    await Promise.allSettled(updatePromises);
    console.log(`[${requestId}] 所有后端状态更新完成，共更新 ${updatePromises.length} 个后端`);
  }
  
  return { 
    backend: selectedBackend, 
    selectionTime,
    algorithm,
    healthyCount: healthyBackends.size,
    totalChecked: checkResults.size,
    loadBalancer: loadBalancer
  };
}

// 流式代理请求
export async function streamProxyRequest(request, backendUrl, backendSelectionTime, requestId, env, ctx, backendWeight) {
  const url = new URL(request.url);
  const backendPath = url.pathname + url.search;
  const backendFullUrl = `${backendUrl}${backendPath}`;
  
  console.log(`[${requestId}] 流式转发请求到后端: ${backendFullUrl}, 权重: ${backendWeight}`);
  
  try {
    const requestStartTime = Date.now();
    
    const backendRequest = new Request(backendFullUrl, {
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
    
    backendRequest.headers.delete('host');
    backendRequest.headers.set('host', new URL(backendUrl).host);
    
    backendRequest.headers.set('X-Request-ID', requestId);
    backendRequest.headers.set('X-Forwarded-By', 'subconverter-failover-worker');
    
    const response = await fetch(backendRequest);
    const responseTime = Date.now() - requestStartTime;
    const success = response.ok;
    const totalTime = responseTime + backendSelectionTime;
    
    console.log(`[${requestId}] 后端响应时间: ${responseTime}ms, 状态码: ${response.status}, 成功: ${success}`);
    
    if (env.DB) {
      const db = new SafeD1Database(env.DB, env);
      const clientIp = request.headers.get('cf-connecting-ip') || 
                       request.headers.get('x-forwarded-for') || 
                       'unknown';
      
      const requestData = {
        backend_url: backendUrl,
        backend_selection_time: backendSelectionTime,
        response_time: responseTime,
        status_code: response.status,
        success: success,
        client_ip: clientIp,
        total_time: totalTime,
        backend_weight: backendWeight
      };
      
      ctx.waitUntil(db.saveRequestResult(requestData, requestId));
      
      // 更新后端状态（每次请求后更新）
      const healthResult = {
        healthy: success && response.status >= 200 && response.status < 300,
        responseTime: responseTime,
        responseTimeScore: 0,
        status: response.status,
        version: 'subconverter'
      };
      
      const loadBalancer = new SmartWeightedLoadBalancer(env);
      const newWeight = await loadBalancer.calculateBackendWeight(backendUrl, healthResult, db, requestId);
      ctx.waitUntil(db.updateBackendStatusWithWeight(backendUrl, healthResult, newWeight, requestId));
      
      if (getConfig(env, 'NOTIFY_ON_REQUEST', true)) {
        const notifier = new ResilientTelegramNotifier(env);
        const notificationData = {
          type: 'request',
          request_id: requestId,
          client_ip: clientIp,
          backend_url: backendUrl,
          backend_selection_time: backendSelectionTime,
          response_time: responseTime,
          status_code: response.status,
          success: success,
          total_time: totalTime,
          backend_weight: newWeight,
          error: success ? '' : `HTTP ${response.status}`,
          env: env
        };
        
        ctx.waitUntil(notifier.sendNotification(notificationData, requestId, ctx));
      }
    }
    
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    
    (async () => {
      try {
        const reader = response.body.getReader();
        
        const chunkSize = getConfig(env, 'STREAMING_CHUNK_SIZE', 8192);
        
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          
          await writer.write(value);
        }
        
        await writer.close();
      } catch (error) {
        console.error(`[${requestId}] 流式传输错误:`, error);
        try {
          await writer.abort(error);
        } catch (e) {
          // 忽略中止错误
        }
      }
    })();
    
    const responseHeaders = new Headers();
    
    for (const [key, value] of response.headers.entries()) {
      if (!key.startsWith('cf-') && key !== 'server') {
        responseHeaders.set(key, value);
      }
    }
    
    responseHeaders.set('X-Backend-Server', backendUrl);
    responseHeaders.set('X-Response-Time', `${responseTime}ms`);
    responseHeaders.set('X-Backend-Selection-Time', `${backendSelectionTime}ms`);
    responseHeaders.set('X-Total-Time', `${totalTime}ms`);
    responseHeaders.set('X-Request-ID', requestId);
    responseHeaders.set('X-Streaming-Proxy', 'true');
    responseHeaders.set('X-Backend-Weight', `${backendWeight}`);
    
    if (!responseHeaders.has('Cache-Control')) {
      responseHeaders.set('Cache-Control', 'no-store, max-age=0');
    }
    
    return new Response(readable, {
      status: response.status,
      statusText: response.statusText,
      headers: responseHeaders
    });
  } catch (error) {
    logError('流式转发请求失败', error, requestId);
    
    if (env.DB) {
      try {
        const db = new SafeD1Database(env.DB, env);
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
          error: error.message,
          backend_weight: backendWeight
        };
        
        ctx.waitUntil(db.saveRequestResult(requestData, `${requestId}-failed`));
        
        // 更新后端状态为失败
        const healthResult = {
          healthy: false,
          responseTime: 0,
          responseTimeScore: 0,
          status: 0,
          version: 'subconverter',
          error: error.name
        };
        
        const loadBalancer = new SmartWeightedLoadBalancer(env);
        const newWeight = await loadBalancer.calculateBackendWeight(backendUrl, healthResult, db, requestId);
        ctx.waitUntil(db.updateBackendStatusWithWeight(backendUrl, healthResult, newWeight, requestId));
        
        if (getConfig(env, 'NOTIFY_ON_ERROR', true)) {
          const notifier = new ResilientTelegramNotifier(env);
          const errorData = {
            type: 'error',
            request_id: requestId,
            error_type: 'request_failed',
            error_message: error.message,
            backend_url: backendUrl,
            client_ip: clientIp,
            backend_weight: newWeight,
            env: env
          };
          
          ctx.waitUntil(notifier.sendNotification(errorData, requestId, ctx));
        }
      } catch (dbError) {
        // 忽略D1写入错误
      }
    }
    
    throw error;
  }
}

// 传统请求处理
export async function handleSubconverterRequest(request, backendUrl, backendSelectionTime, requestId, env, ctx, backendWeight) {
  const url = new URL(request.url);
  const backendPath = url.pathname + url.search;
  
  console.log(`[${requestId}] 传统方式转发请求到后端: ${backendUrl}${backendPath}, 权重: ${backendWeight}`);
  
  try {
    const requestStartTime = Date.now();
    
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
    
    backendRequest.headers.delete('host');
    backendRequest.headers.set('host', new URL(backendUrl).host);
    
    backendRequest.headers.set('X-Request-ID', requestId);
    backendRequest.headers.set('X-Forwarded-By', 'subconverter-failover-worker');
    
    const response = await fetch(backendRequest);
    const responseTime = Date.now() - requestStartTime;
    const success = response.ok;
    const totalTime = responseTime + backendSelectionTime;
    
    console.log(`[${requestId}] 后端响应时间: ${responseTime}ms, 状态码: ${response.status}, 成功: ${success}`);
    
    if (env.DB) {
      const db = new SafeD1Database(env.DB, env);
      const clientIp = request.headers.get('cf-connecting-ip') || 
                       request.headers.get('x-forwarded-for') || 
                       'unknown';
      
      const requestData = {
        backend_url: backendUrl,
        backend_selection_time: backendSelectionTime,
        response_time: responseTime,
        status_code: response.status,
        success: success,
        client_ip: clientIp,
        total_time: totalTime,
        backend_weight: backendWeight
      };
      
      ctx.waitUntil(db.saveRequestResult(requestData, requestId));
      
      // 更新后端状态（每次请求后更新）
      const healthResult = {
        healthy: success && response.status >= 200 && response.status < 300,
        responseTime: responseTime,
        responseTimeScore: 0,
        status: response.status,
        version: 'subconverter'
      };
      
      const loadBalancer = new SmartWeightedLoadBalancer(env);
      const newWeight = await loadBalancer.calculateBackendWeight(backendUrl, healthResult, db, requestId);
      ctx.waitUntil(db.updateBackendStatusWithWeight(backendUrl, healthResult, newWeight, requestId));
      
      if (getConfig(env, 'NOTIFY_ON_REQUEST', true)) {
        const notifier = new ResilientTelegramNotifier(env);
        const notificationData = {
          type: 'request',
          request_id: requestId,
          client_ip: clientIp,
          backend_url: backendUrl,
          backend_selection_time: backendSelectionTime,
          response_time: responseTime,
          status_code: response.status,
          success: success,
          total_time: totalTime,
          backend_weight: newWeight,
          error: success ? '' : `HTTP ${response.status}`,
          env: env
        };
        
        ctx.waitUntil(notifier.sendNotification(notificationData, requestId, ctx));
      }
    }
    
    const responseHeaders = new Headers();
    
    for (const [key, value] of response.headers.entries()) {
      if (!key.startsWith('cf-') && key !== 'server') {
        responseHeaders.set(key, value);
      }
    }
    
    responseHeaders.set('X-Backend-Server', backendUrl);
    responseHeaders.set('X-Response-Time', `${responseTime}ms`);
    responseHeaders.set('X-Backend-Selection-Time', `${backendSelectionTime}ms`);
    responseHeaders.set('X-Total-Time', `${totalTime}ms`);
    responseHeaders.set('X-Request-ID', requestId);
    responseHeaders.set('X-Backend-Weight', `${backendWeight}`);
    
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
    
    if (env.DB) {
      try {
        const db = new SafeD1Database(env.DB, env);
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
          error: error.message,
          backend_weight: backendWeight
        };
        
        ctx.waitUntil(db.saveRequestResult(requestData, `${requestId}-failed`));
        
        // 更新后端状态为失败
        const healthResult = {
          healthy: false,
          responseTime: 0,
          responseTimeScore: 0,
          status: 0,
          version: 'subconverter',
          error: error.name
        };
        
        const loadBalancer = new SmartWeightedLoadBalancer(env);
        const newWeight = await loadBalancer.calculateBackendWeight(backendUrl, healthResult, db, requestId);
        ctx.waitUntil(db.updateBackendStatusWithWeight(backendUrl, healthResult, newWeight, requestId));
        
        if (getConfig(env, 'NOTIFY_ON_ERROR', true)) {
          const notifier = new ResilientTelegramNotifier(env);
          const errorData = {
            type: 'error',
            request_id: requestId,
            error_type: 'request_failed',
            error_message: error.message,
            backend_url: backendUrl,
            client_ip: clientIp,
            backend_weight: newWeight,
            env: env
          };
          
          ctx.waitUntil(notifier.sendNotification(errorData, requestId, ctx));
        }
      } catch (dbError) {
        // 忽略D1写入错误
      }
    }
    
    throw error;
  }
}

// 【修复】执行完整健康检查（定时任务：检查所有后端并更新backend_status）
// 修复：1. 定时任务不跳过普通优先级检查 2. 只有当可用后端发生改变时才发送通知
export async function performFullHealthCheck(db, requestId, env, ctx = null) {
  const backends = await getBackends(env, requestId);
  
  if (backends.length === 0) {
    console.log(`[${requestId}] 无配置的后端地址，跳过健康检查`);
    return {
      results: {},
      availableBackend: null,
      timestamp: new Date().toISOString()
    };
  }
  
  console.log(`[${requestId}] 定时任务：开始检查所有 ${backends.length} 个后端地址`);
  
  // 修复：定时任务不跳过普通优先级检查，所以要传入 false
  const results = await batchHealthChecks(backends, requestId, env, false);
  
  let fastestBackend = null;
  let fastestTime = Infinity;
  let healthyBackends = 0;
  let loadBalancer = new SmartWeightedLoadBalancer(env);
  let weightStatistics = [];
  
  // 【修复】收集所有后端（无论健康与否）的权重并更新到数据库
  const updatePromises = [];
  
  for (const [url, health] of results.entries()) {
    if (health.healthy) {
      healthyBackends++;
      if (health.responseTime && health.responseTime < fastestTime) {
        fastestBackend = url;
        fastestTime = health.responseTime;
      }
    }
    
    // 【修复】所有后端都要计算权重并更新到 backend_status 表
    if (db) {
      const targetWeight = await loadBalancer.calculateBackendWeight(url, health, db, requestId);
      
      // 记录权重统计
      weightStatistics.push({
        url,
        weight: targetWeight,
        healthy: health.healthy,
        responseTime: health.responseTime,
        responseTimeScore: health.responseTimeScore,
        success: health.healthy
      });
      
      const updatePromise = db.updateBackendStatusWithWeight(url, health, targetWeight, requestId)
        .then(() => {
          console.log(`[${requestId}] 后端状态更新成功: ${url}, 权重: ${targetWeight}, 健康: ${health.healthy}`);
        })
        .catch(error => {
          console.error(`[${requestId}] 更新后端状态失败 ${url}: ${error.message}`);
        });
      
      updatePromises.push(updatePromise);
    }
  }
  
  // 【修复】等待所有后端（无论健康与否）状态更新完成
  try {
    if (updatePromises.length > 0) {
      const updateResults = await Promise.allSettled(updatePromises);
      
      const successfulUpdates = updateResults.filter(r => r.status === 'fulfilled').length;
      const failedUpdates = updateResults.filter(r => r.status === 'rejected').length;
      
      console.log(`[${requestId}] 定时任务：后端状态更新完成，成功: ${successfulUpdates}, 失败: ${failedUpdates}, 总计: ${updatePromises.length} 个后端`);
    }
  } catch (error) {
    console.error(`[${requestId}] 定时任务：后端状态更新失败:`, error);
  }
  
  // 修复：只有当可用后端发生改变时才发送通知
  let backendChanged = false;
  let previousBackend = null;
  
  if (db) {
    try {
      // 获取上一次健康检查的最快后端
      const previousCheck = await db.getLastHealthCheck();
      if (previousCheck) {
        previousBackend = previousCheck.available_backend;
        
        // 比较当前最快后端和上一次的最快后端
        // 需要考虑两种情况：1. 都是null 2. 都是相同地址 3. 不同地址
        if ((!fastestBackend && !previousBackend) || (fastestBackend === previousBackend)) {
          backendChanged = false;
          console.log(`[${requestId}] 定时任务：可用后端未改变，当前: ${fastestBackend}, 上次: ${previousBackend}`);
        } else {
          backendChanged = true;
          console.log(`[${requestId}] 定时任务：可用后端发生改变！当前: ${fastestBackend}, 上次: ${previousBackend}`);
        }
      } else {
        // 第一次检查，视为有改变
        backendChanged = true;
        console.log(`[${requestId}] 定时任务：第一次检查，视为可用后端有改变`);
      }
    } catch (error) {
      console.error(`[${requestId}] 定时任务：获取上一次健康检查失败:`, error);
      // 如果获取失败，默认发送通知
      backendChanged = true;
    }
  }
  
  // 【修复】保存健康检查结果到 health_check_results 表（历史记录）
  if (db) {
    try {
      const healthCheckData = {
        results: Object.fromEntries(results),
        available_backend: fastestBackend,
        fastest_response_time: fastestTime,
        backend_changed: backendChanged,
        weight_statistics: weightStatistics
      };
      
      await db.saveHealthCheckResult(healthCheckData, requestId);
      
      console.log(`[${requestId}] 定时任务：健康检查结果已保存到D1，最快后端: ${fastestBackend}，健康后端数: ${healthyBackends}/${backends.length}，后端改变: ${backendChanged}`);
      
    } catch (error) {
      logError('保存健康检查结果到D1失败', error, requestId);
    }
  }
  
  console.log(`[${requestId}] 定时健康检查完成，发现最快后端: ${fastestBackend}, 响应时间: ${fastestTime}ms，健康后端: ${healthyBackends}/${backends.length}，后端改变: ${backendChanged}`);
  
  // 【修复】只有当后端发生改变且配置允许时才发送Telegram通知
  if (db && ctx && backendChanged && getConfig(env, 'NOTIFY_ON_HEALTH_CHANGE', true)) {
    const notifier = new ResilientTelegramNotifier(env);
    
    let changeType = '';
    if (fastestBackend && previousBackend) {
      changeType = '后端切换';
    } else if (fastestBackend && !previousBackend) {
      changeType = '后端恢复';
    } else if (!fastestBackend && previousBackend) {
      changeType = '所有后端不可用';
    } else {
      changeType = '健康状态变化';
    }
    
    const notificationData = {
      type: 'health_change',
      change_type: changeType,
      current_backend: fastestBackend,
      previous_backend: previousBackend,
      healthy_backends: healthyBackends,
      total_backends: backends.length,
      response_time: fastestTime,
      reason: `定时健康检查: ${healthyBackends}个健康/${backends.length}个总数`,
      weight_statistics: weightStatistics,
      env: env
    };
    
    ctx.waitUntil(notifier.sendNotification(notificationData, requestId, ctx));
    
    console.log(`[${requestId}] 定时任务：发送健康状态变化通知，类型: ${changeType}`);
  } else if (ctx && !backendChanged) {
    console.log(`[${requestId}] 定时任务：可用后端未改变，跳过发送通知`);
  }
  
  return {
    results: Object.fromEntries(results),
    availableBackend: fastestBackend,
    fastestResponseTime: fastestTime,
    timestamp: new Date().toISOString(),
    backendChanged: backendChanged,
    healthyBackends: healthyBackends,
    totalBackends: backends.length,
    weightStatistics: weightStatistics,
    loadBalancer: loadBalancer
  };
}

// 辅助函数：获取之前的健康后端数量
async function getPreviousHealthyCount(db, backends) {
  if (!db) return 0;
  
  try {
    const backendStatus = await db.getAllBackendStatus();
    const previousHealthy = backendStatus.filter(b => b.healthy === 1).length;
    return previousHealthy;
  } catch (error) {
    console.error('获取之前健康后端数量失败:', error);
    return 0;
  }
}