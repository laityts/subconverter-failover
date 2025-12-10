import { 
  getConfig, 
  getBeijingTimeString, 
  getBeijingDateString,
  getBeijingTimeShort,
  logError
} from './utils.js';
import { healthCheckController } from './concurrency.js';

// 创建增强状态页面
export async function createEnhancedStatusPage(requestId, env, db) {
  if (!db) {
    return new Response('D1数据库未配置，无法显示状态页面', {
      status: 503,
      headers: { 'Content-Type': 'text/html; charset=utf-8' }
    });
  }
  
  try {
    const statusData = await db.getStatusPageData(env);
    
    let showRefreshWarning = false;
    if (statusData.dataStatus === 'error' || statusData.backendStatus.length === 0) {
      showRefreshWarning = true;
    }
    
    const beijingNowStr = getBeijingTimeString();
    
    // 从 backend_status 表获取数据
    const backendStatus = statusData.backendStatus || [];
    const totalBackends = statusData.backendUrls?.length || 0;
    const healthyBackends = statusData.healthStats?.healthyBackends || 0;
    const availableBackend = statusData.availableBackend;
    const fastestResponseTime = statusData.fastestResponseTime || 0;
    const avgBackendWeight = statusData.healthStats?.avgWeight || 0;
    const avgRequestWeight = statusData.requestStats?.avgBackendWeight || 0;
    
    // 从 d1Stats 中获取今日写入数据
    const d1DailyWrites = statusData.d1Stats?.today?.total || 0;
    const d1TotalWrites = statusData.d1Stats?.total?.total || 0;
    const tgTotalSent = statusData.telegramTotalSent || 0;
    
    // 获取今日平均响应时间
    const todayAvgResponseTime = statusData.d1Stats?.today?.avg_response_time || 0;
    
    const totalRequests = statusData.requestStats.total || 0;
    const successfulRequests = statusData.requestStats.successful || 0;
    const failedRequests = statusData.requestStats.failed || 0;
    const avgResponseTime = statusData.requestStats.avgResponseTime || 0;
    
    // 使用今日平均响应时间，如果没有则使用历史平均
    const displayAvgResponseTime = todayAvgResponseTime > 0 ? todayAvgResponseTime : avgResponseTime;
    
    const lbAlgorithm = getConfig(env, 'LB_ALGORITHM', 'weighted_round_robin');
    const lbAlgorithmName = {
      'weighted_round_robin': '智能加权轮询'
    }[lbAlgorithm] || '智能加权轮询';
    
    const streamingEnabled = getConfig(env, 'ENABLE_STREAMING_PROXY', true);
    
    // 获取负载均衡器配置
    const weightAdjustmentFactor = getConfig(env, 'WEIGHT_ADJUSTMENT_FACTOR', 0.3);
    const weightRecoveryRate = getConfig(env, 'WEIGHT_RECOVERY_RATE', 2);
    const baseWeight = getConfig(env, 'BASE_WEIGHT', 50);
    const maxWeight = getConfig(env, 'MAX_WEIGHT', 100);
    const minWeight = getConfig(env, 'MIN_WEIGHT', 10);
    
    // 获取今日请求统计
    const todayRequestCount = statusData.d1Stats?.today?.request_results || 0;
    const todaySuccessfulRequests = statusData.d1Stats?.today?.successful_requests || 0;
    
    // 获取最后更新时间（从 backend_status 表）
    let lastUpdateTime = beijingNowStr;
    if (backendStatus.length > 0) {
      const latestBackend = backendStatus.sort((a, b) => {
        const timeA = a.updated_at_beijing || a.last_checked_beijing || '';
        const timeB = b.updated_at_beijing || b.last_checked_beijing || '';
        return timeB.localeCompare(timeA);
      })[0];
      
      if (latestBackend?.updated_at_beijing || latestBackend?.last_checked_beijing) {
        lastUpdateTime = latestBackend.updated_at_beijing || latestBackend.last_checked_beijing;
      }
    }
    
    const html = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>订阅转换服务状态</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
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
        h1 { color: #333; margin-bottom: 16px; text-align: center; font-size: 20px; font-weight: 600; line-height: 1.3; }
        .time-info { text-align: center; color: #495057; margin-bottom: 16px; font-size: 13px; line-height: 1.4; }
        .status-header { text-align: center; margin-bottom: 20px; }
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
        .status-healthy { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .status-unhealthy { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .refresh-warning {
            background: #fff3cd;
            border: 1px solid #ffeaa7;
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 16px;
            color: #856404;
        }
        .refresh-warning h4 { margin: 0 0 8px 0; color: #856404; font-size: 14px; }
        .immediate-check-btn {
            background: #28a745;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
            margin-top: 8px;
        }
        .immediate-check-btn:hover { background: #218838; }
        .immediate-check-btn:disabled { background: #6c757d; cursor: not-allowed; }
        #checkStatus { margin-top: 8px; font-size: 12px; }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 10px;
            margin-bottom: 20px;
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
        .stat-value { font-size: 22px; font-weight: 700; color: #2c3e50; margin-bottom: 4px; line-height: 1.2; }
        .stat-label { font-size: 12px; color: #6c757d; line-height: 1.3; }
        .current-backend {
            background: #e7f5ff;
            border: 1px solid #bbdefb;
            padding: 14px;
            border-radius: 10px;
            margin-bottom: 20px;
        }
        .current-backend h3 { color: #1971c2; margin-bottom: 8px; font-size: 16px; font-weight: 600; }
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
        .backend-meta { font-size: 11px; color: #6c757d; display: flex; flex-wrap: wrap; gap: 6px; margin-top: 6px; }
        .meta-item { background: #f8f9fa; padding: 3px 6px; border-radius: 4px; white-space: nowrap; }
        .request-stats, .lb-info {
            background: #e7f5ff;
            border: 1px solid #bbdefb;
            padding: 12px;
            border-radius: 8px;
            margin-top: 16px;
        }
        .request-stats h3, .lb-info h3 {
            color: #1971c2;
            margin-bottom: 8px;
            font-size: 16px;
            font-weight: 600;
        }
        
        /* ==== 优化后的Telegram通知卡片样式 ==== */
        .telegram-stats-section {
            margin-bottom: 20px;
        }
        
        .telegram-stats-section h3 {
            color: #0088cc;
            margin-bottom: 12px;
            font-size: 16px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .telegram-stats-section h3:before {
            content: "📱";
            font-size: 18px;
        }
        
        /* 统计卡片 */
        .telegram-stats-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 10px;
            margin-bottom: 16px;
        }
        
        .telegram-stat-card {
            background: linear-gradient(135deg, #0088cc 0%, #006699 100%);
            border-radius: 10px;
            padding: 14px;
            color: white;
            text-align: center;
            box-shadow: 0 4px 12px rgba(0, 136, 204, 0.2);
            transition: transform 0.2s ease;
        }
        
        .telegram-stat-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 16px rgba(0, 136, 204, 0.3);
        }
        
        .telegram-stat-value {
            font-size: 28px;
            font-weight: 700;
            margin-bottom: 5px;
            text-shadow: 0 1px 2px rgba(0, 0, 0, 0.1);
        }
        
        .telegram-stat-label {
            font-size: 12px;
            opacity: 0.9;
            margin-bottom: 8px;
        }
        
        .telegram-stat-breakdown {
            display: flex;
            justify-content: space-around;
            font-size: 10px;
            border-top: 1px solid rgba(255, 255, 255, 0.2);
            padding-top: 8px;
        }
        
        .telegram-stat-item {
            text-align: center;
        }
        
        .telegram-stat-item-value {
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 2px;
        }
        
        .telegram-stat-item-label {
            opacity: 0.8;
            font-size: 10px;
        }
        
        /* 通知列表容器 */
        .telegram-notifications-container {
            margin-top: 20px;
        }
        
        .telegram-notifications-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
            flex-wrap: wrap;
            gap: 8px;
        }
        
        .telegram-notifications-header h4 {
            color: #495057;
            font-size: 14px;
            font-weight: 600;
            margin: 0;
        }
        
        .notification-filter {
            display: flex;
            gap: 6px;
            flex-wrap: wrap;
        }
        
        .filter-btn {
            padding: 4px 10px;
            border-radius: 15px;
            font-size: 11px;
            font-weight: 500;
            cursor: pointer;
            border: 1px solid #dee2e6;
            background: white;
            color: #6c757d;
            transition: all 0.2s ease;
        }
        
        .filter-btn:hover, .filter-btn.active {
            background: #0088cc;
            color: white;
            border-color: #0088cc;
        }
        
        /* 通知列表 */
        .telegram-notifications-list {
            display: flex;
            flex-direction: column;
            gap: 10px;
        }
        
        /* 优化的通知卡片 */
        .notification-card {
            background: white;
            border: 1px solid #dee2e6;
            border-radius: 10px;
            padding: 14px;
            position: relative;
            overflow: hidden;
            transition: all 0.2s ease;
            cursor: pointer;
        }
        
        .notification-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(0, 0, 0, 0.1);
            border-color: #0088cc;
        }
        
        .notification-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 4px;
            height: 100%;
            background: #0088cc;
        }
        
        .notification-card.success::before {
            background: #28a745;
        }
        
        .notification-card.error::before {
            background: #dc3545;
        }
        
        .notification-card.health-change::before {
            background: #ffc107;
        }
        
        /* 通知卡片头部 */
        .notification-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 10px;
            flex-wrap: wrap;
            gap: 8px;
        }
        
        .notification-type-badge {
            display: inline-flex;
            align-items: center;
            gap: 5px;
            padding: 4px 10px;
            border-radius: 15px;
            font-size: 11px;
            font-weight: 600;
            white-space: nowrap;
        }
        
        .notification-type-badge.request {
            background: rgba(0, 136, 204, 0.1);
            color: #0088cc;
            border: 1px solid rgba(0, 136, 204, 0.3);
        }
        
        .notification-type-badge.health-change {
            background: rgba(255, 193, 7, 0.1);
            color: #856404;
            border: 1px solid rgba(255, 193, 7, 0.3);
        }
        
        .notification-type-badge.error {
            background: rgba(220, 53, 69, 0.1);
            color: #dc3545;
            border: 1px solid rgba(220, 53, 69, 0.3);
        }
        
        .notification-time {
            font-size: 11px;
            color: #6c757d;
            white-space: nowrap;
        }
        
        /* 通知消息内容 */
        .notification-message {
            font-size: 13px;
            line-height: 1.5;
            color: #495057;
            margin-bottom: 12px;
            word-break: break-word;
            max-height: 60px;
            overflow: hidden;
            text-overflow: ellipsis;
            display: -webkit-box;
            -webkit-line-clamp: 3;
            -webkit-box-orient: vertical;
        }
        
        .notification-message.expanded {
            max-height: none;
            -webkit-line-clamp: unset;
        }
        
        /* 通知详情 */
        .notification-details {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            font-size: 11px;
            color: #6c757d;
        }
        
        .notification-detail {
            display: inline-flex;
            align-items: center;
            gap: 4px;
            background: #f8f9fa;
            padding: 3px 8px;
            border-radius: 4px;
            max-width: 100%;
            overflow: hidden;
        }
        
        .notification-detail .icon {
            font-size: 10px;
            opacity: 0.7;
        }
        
        .notification-detail .text {
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        
        /* 展开/收起按钮 */
        .notification-expand-btn {
            margin-top: 10px;
            padding: 4px 10px;
            background: #f8f9fa;
            border: 1px solid #dee2e6;
            border-radius: 4px;
            font-size: 11px;
            color: #6c757d;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 4px;
            transition: all 0.2s ease;
        }
        
        .notification-expand-btn:hover {
            background: #e9ecef;
            color: #495057;
        }
        
        /* 空状态 */
        .telegram-empty-state {
            text-align: center;
            padding: 30px 20px;
            background: rgba(0, 136, 204, 0.05);
            border: 2px dashed rgba(0, 136, 204, 0.3);
            border-radius: 10px;
            color: #0088cc;
        }
        
        .telegram-empty-icon {
            font-size: 36px;
            margin-bottom: 12px;
            opacity: 0.5;
        }
        
        .telegram-empty-text {
            font-size: 14px;
            font-weight: 500;
            margin-bottom: 8px;
        }
        
        .telegram-empty-subtext {
            font-size: 12px;
            opacity: 0.7;
        }
        
        /* 后端卡片样式 */
        .backends-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
            gap: 12px;
            margin-top: 8px;
        }

        .backend-card {
            background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
            border: 1px solid #dee2e6;
            border-radius: 10px;
            padding: 14px;
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }

        .backend-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(0,0,0,0.1);
        }

        .backend-card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
        }

        .health-indicator {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            display: inline-block;
            margin-right: 8px;
            flex-shrink: 0;
        }

        .health-up {
            background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
            box-shadow: 0 0 0 3px rgba(40, 167, 69, 0.2);
        }

        .health-down {
            background: linear-gradient(135deg, #dc3545 0%, #fd7e14 100%);
            box-shadow: 0 0 0 3px rgba(220, 53, 69, 0.2);
        }

        .backend-status-badge {
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            display: inline-block;
        }

        .status-healthy {
            background: rgba(40, 167, 69, 0.1);
            color: #28a745;
            border: 1px solid rgba(40, 167, 69, 0.3);
        }

        .status-unhealthy {
            background: rgba(220, 53, 69, 0.1);
            color: #dc3545;
            border: 1px solid rgba(220, 53, 69, 0.3);
        }

        .weight-display {
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .weight-bar {
            width: 60px;
            height: 6px;
            background: #e9ecef;
            border-radius: 3px;
            overflow: hidden;
        }

        .weight-fill {
            height: 100%;
            background: linear-gradient(90deg, #28a745, #17a2b8);
            border-radius: 3px;
            transition: width 0.5s ease;
        }

        .weight-value {
            font-size: 13px;
            font-weight: 700;
            color: #495057;
            min-width: 25px;
        }

        .backend-url-container {
            margin-bottom: 12px;
            background: rgba(0, 123, 255, 0.05);
            padding: 8px;
            border-radius: 6px;
            border: 1px solid rgba(0, 123, 255, 0.1);
        }

        .backend-url {
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
            font-size: 13px;
            color: #495057;
            word-break: break-all;
            line-height: 1.4;
            margin-bottom: 4px;
            font-weight: 500;
        }

        .backend-version {
            font-size: 11px;
            color: #6c757d;
            background: rgba(108, 117, 125, 0.1);
            padding: 2px 6px;
            border-radius: 4px;
            display: inline-block;
        }

        .backend-stats-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 8px;
            margin-bottom: 12px;
        }

        .stat-item {
            background: #f8f9fa;
            padding: 6px;
            border-radius: 6px;
            text-align: center;
        }

        .stat-label {
            display: block;
            font-size: 11px;
            color: #6c757d;
            margin-bottom: 2px;
        }

        .stat-value {
            display: block;
            font-size: 14px;
            font-weight: 700;
            color: #2c3e50;
        }

        .stat-good {
            color: #28a745;
        }

        .stat-warning {
            color: #ffc107;
        }

        .stat-bad {
            color: #dc3545;
        }

        .backend-meta-details {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            font-size: 11px;
            color: #6c757d;
            padding-top: 8px;
            border-top: 1px solid #e9ecef;
        }

        .meta-detail {
            background: rgba(108, 117, 125, 0.05);
            padding: 3px 6px;
            border-radius: 4px;
            display: flex;
            align-items: center;
            gap: 4px;
        }

        .meta-label {
            font-weight: 500;
        }

        .meta-value {
            color: #495057;
        }

        .backend-error-info {
            background: rgba(220, 53, 69, 0.1);
            border: 1px solid rgba(220, 53, 69, 0.2);
            padding: 6px;
            border-radius: 4px;
            margin-top: 8px;
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .error-icon {
            color: #dc3545;
            font-size: 12px;
        }

        .error-text {
            color: #dc3545;
            font-size: 11px;
            font-weight: 500;
        }

        .backends-list {
            margin-bottom: 20px;
            margin-top: 40px; /* 增加与上方内容的间距 */
        }
        
        .backends-list h3 {
            margin-bottom: 12px;
            color: #495057;
            font-size: 16px;
            font-weight: 600;
        }
        
        .info-section {
            background: #f8f9fa;
            border: 1px solid #e9ecef;
            padding: 14px;
            border-radius: 10px;
            margin-top: 16px;
            font-size: 14px;
        }
        .info-section h3 { color: #495057; margin-bottom: 12px; font-size: 16px; font-weight: 600; }
        .info-section ul { margin-left: 16px; color: #6c757d; line-height: 1.5; }
        .info-section li { margin-bottom: 6px; }
        .feature-badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 600;
            margin-right: 6px;
            margin-bottom: 6px;
        }
        .feature-enabled { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .feature-disabled { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
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
        .action-btn:hover, .action-btn:active { background: #0056b3; transform: translateY(-1px); }
        .action-btn-secondary { background: #28a745; }
        .action-btn-secondary:hover, .action-btn-secondary:active { background: #1e7e34; }
        .action-btn-danger { background: #dc3545; }
        .action-btn-danger:hover, .action-btn-danger:active { background: #c82333; }
        .action-btn-telegram { background: #0088cc; }
        .action-btn-telegram:hover, .action-btn-telegram:active { background: #006699; }
        .action-btn-info { background: #17a2b8; }
        .action-btn-info:hover, .action-btn-info:active { background: #138496; }
        .action-btn-warning { background: #ffc107; color: #212529; }
        .action-btn-warning:hover, .action-btn-warning:active { background: #e0a800; }
        .footer {
            text-align: center;
            color: #6c757d;
            font-size: 11px;
            margin-top: 20px;
            line-height: 1.5;
            padding: 12px 0 0 0;
            border-top: 1px solid #e9ecef;
        }
        
        /* 响应式调整 */
        @media (max-width: 640px) {
            .backends-grid {
                grid-template-columns: 1fr;
            }
            
            .backend-card {
                padding: 12px;
            }
            
            .backend-stats-grid {
                grid-template-columns: repeat(4, 1fr);
            }
            
            .stat-item {
                padding: 4px;
            }
            
            .stat-label {
                font-size: 10px;
            }
            
            .stat-value {
                font-size: 12px;
            }
            
            /* 移动端优化Telegram统计卡片 */
            .telegram-stats-cards {
                grid-template-columns: 1fr 1fr;
            }
            
            .telegram-stat-card {
                padding: 12px;
            }
            
            .telegram-stat-value {
                font-size: 24px;
            }
            
            .telegram-stat-label {
                font-size: 11px;
            }
            
            .notification-card {
                padding: 12px;
            }
            
            .notification-message {
                font-size: 12px;
                max-height: 48px;
                -webkit-line-clamp: 2;
            }
            
            .notification-details {
                font-size: 10px;
            }
            
            .notification-detail {
                padding: 2px 6px;
            }
        }

        @media (min-width: 768px) {
            .backends-grid {
                grid-template-columns: repeat(2, 1fr);
            }
            
            .telegram-stats-cards {
                grid-template-columns: repeat(3, 1fr);
            }
            
            .telegram-stat-card {
                padding: 16px;
            }
        }

        @media (min-width: 1024px) {
            .backends-grid {
                grid-template-columns: repeat(3, 1fr);
            }
            
            .telegram-stats-cards {
                grid-template-columns: repeat(4, 1fr);
            }
        }
        
        @media (max-width: 360px) {
            .container { padding: 12px; }
            h1 { font-size: 18px; }
            .stat-card { padding: 10px 8px; min-height: 60px; }
            .stat-value { font-size: 20px; }
            .stat-label { font-size: 11px; }
            .action-btn { min-width: 110px; padding: 9px 12px; font-size: 12px; }
            .telegram-stats-cards {
                grid-template-columns: 1fr;
            }
            .notification-header {
                flex-direction: column;
                align-items: flex-start;
            }
            .notification-time {
                align-self: flex-start;
            }
        }
        
        @media (min-width: 640px) {
            .container { padding: 24px; max-width: 640px; }
            h1 { font-size: 24px; }
            .stats-grid { grid-template-columns: repeat(4, 1fr); gap: 15px; }
            .stat-card { padding: 18px 12px; }
            .stat-value { font-size: 24px; }
            .stat-label { font-size: 13px; }
            .backend-item { padding: 14px; }
            .action-buttons { gap: 10px; }
            .action-btn { flex: 0 1 auto; max-width: none; min-width: 140px; }
        }
        
        @media (prefers-color-scheme: dark) {
            body { background: linear-gradient(135deg, #4c51bf 0%, #6b21a8 100%); }
            .container { background: #1a1a1a; color: #e0e0e0; }
            h1 { color: #e0e0e0; }
            .stat-card { background: #2d2d2d; }
            .stat-value { color: #ffffff; }
            .stat-label { color: #a0a0a0; }
            .current-backend { background: #1e3a5f; border-color: #3b82f6; }
            .current-backend h3 { color: #93c5fd; }
            .backend-url { background: #2d2d2d; border-color: #404040; color: #d1d5db; }
            .backend-card { background: #2d2d2d; border-color: #404040; }
            .backend-url-container { background: rgba(0, 123, 255, 0.1); border-color: rgba(0, 123, 255, 0.2); }
            .backend-name { color: #d1d5db; }
            .meta-item { background: #3d3d3d; color: #b0b0b0; }
            .weight-bar { background: #3d3d3d; }
            .weight-fill { background: linear-gradient(90deg, #28a745, #17a2b8); }
            .weight-value { color: #d1d5db; }
            .info-section { background: #2d2d2d; border-color: #404040; }
            .info-section h3 { color: #d1d5db; }
            .info-section ul { color: #a0a0a0; }
            
            /* 深色模式下的Telegram卡片 */
            .telegram-stat-card { 
                background: linear-gradient(135deg, #006699 0%, #004466 100%); 
            }
            
            .notification-card {
                background: #2d2d2d;
                border-color: #404040;
                color: #e0e0e0;
            }
            
            .notification-card:hover {
                border-color: #0088cc;
            }
            
            .notification-type-badge.request {
                background: rgba(0, 136, 204, 0.2);
                color: #66b3ff;
                border-color: rgba(0, 136, 204, 0.4);
            }
            
            .notification-type-badge.health-change {
                background: rgba(255, 193, 7, 0.2);
                color: #ffd54f;
                border-color: rgba(255, 193, 7, 0.4);
            }
            
            .notification-type-badge.error {
                background: rgba(220, 53, 69, 0.2);
                color: #ff6b6b;
                border-color: rgba(220, 53, 69, 0.4);
            }
            
            .notification-time {
                color: #a0a0a0;
            }
            
            .notification-message {
                color: #d1d5db;
            }
            
            .notification-detail {
                background: #3d3d3d;
                color: #b0b0b0;
            }
            
            .notification-expand-btn {
                background: #3d3d3d;
                border-color: #404040;
                color: #b0b0b0;
            }
            
            .notification-expand-btn:hover {
                background: #4d4d4d;
                color: #d1d5db;
            }
            
            .telegram-empty-state {
                background: rgba(0, 136, 204, 0.1);
                border-color: rgba(0, 136, 204, 0.2);
                color: #66b3ff;
            }
            
            .filter-btn {
                background: #3d3d3d;
                border-color: #404040;
                color: #b0b0b0;
            }
            
            .filter-btn:hover, .filter-btn.active {
                background: #0088cc;
                color: white;
                border-color: #0088cc;
            }
            
            .feature-enabled { background: #1e453e; color: #34d399; border-color: #059669; }
            .feature-disabled { background: #3c1e1e; color: #f8d7da; border-color: #721c24; }
            .refresh-warning { background: #332701; border-color: #856404; color: #ffeaa7; }
            .refresh-warning h4 { color: #ffeaa7; }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 订阅转换服务状态 - 智能加权轮询</h1>
        
        <div class="time-info">
            页面生成时间: ${beijingNowStr}<br>
            数据更新时间: ${lastUpdateTime}<br>
            <small>数据来源: backend_status 表（北京时间）</small>
        </div>
        
        ${showRefreshWarning ? `
        <div class="refresh-warning">
            <h4>⚠️ 数据可能需要刷新</h4>
            <p>状态数据可能已过期，点击按钮立即执行健康检查并刷新页面。</p>
            <button class="immediate-check-btn" id="immediateCheckBtn">
                🔄 立即执行健康检查
            </button>
            <div id="checkStatus"></div>
        </div>
        ` : ''}
        
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
                <div class="stat-value">${tgTotalSent}</div>
                <div class="stat-label">TG通知</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">${d1DailyWrites}</div>
                <div class="stat-label">今日写入</div>
            </div>
        </div>
        
        ${availableBackend ? `
        <div class="current-backend">
            <h3>当前使用后端</h3>
            <div class="backend-url">${availableBackend}</div>
            <div class="backend-meta">
                <span class="meta-item">最快响应: ${fastestResponseTime > 0 ? fastestResponseTime + 'ms' : '未知'}</span>
                <span class="meta-item">最后更新: ${lastUpdateTime}</span>
                <span class="meta-item">负载均衡算法: ${lbAlgorithmName}</span>
                <span class="meta-item">平均后端权重: ${Math.round(avgBackendWeight)}</span>
                <span class="meta-item">平均请求权重: ${Math.round(avgRequestWeight)}</span>
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
        
        <div class="request-stats">
            <h3>📊 请求统计</h3>
            <div class="backend-meta">
                <span class="meta-item">今日请求: ${todayRequestCount}</span>
                <span class="meta-item">今日成功: ${todaySuccessfulRequests}</span>
                <span class="meta-item">今日成功率: ${todayRequestCount > 0 ? Math.round(todaySuccessfulRequests / todayRequestCount * 100) : 0}%</span>
                <span class="meta-item">今日平均响应: ${displayAvgResponseTime}ms</span>
                <span class="meta-item">总请求: ${totalRequests}</span>
                <span class="meta-item">历史平均响应: ${avgResponseTime}ms</span>
            </div>
        </div>
        
        <div class="lb-info">
            <h3>🚦 负载均衡配置</h3>
            <div class="backend-meta">
                <span class="feature-badge ${streamingEnabled ? 'feature-enabled' : 'feature-disabled'}">
                    ${streamingEnabled ? '✅ 流式代理' : '❌ 流式代理'}
                </span>
                <span class="meta-item">算法: ${lbAlgorithmName}</span>
                <span class="meta-item">权重调整因子: ${weightAdjustmentFactor}</span>
                <span class="meta-item">权重恢复速率: ${weightRecoveryRate}/分钟</span>
                <span class="meta-item">基准权重: ${baseWeight}</span>
                <span class="meta-item">权重范围: ${minWeight}-${maxWeight}</span>
            </div>
        </div>
        
        ${totalBackends > 0 ? `
        <div class="backends-list">
            <h3>🖥️ 后端状态详情</h3>
            <div class="backends-grid">
                ${backendStatus.map(backend => {
                  const url = backend.backend_url;
                  const healthy = backend.healthy === 1;
                  const weight = backend.weight || baseWeight;
                  const failureCount = backend.failure_count || 0;
                  const requestCount = backend.request_count || 0;
                  const successRate = (backend.success_rate || 0) * 100;
                  const avgResponseTime = backend.avg_response_time || 0;
                  const lastSuccessBeijing = backend.last_success_beijing || '从未';
                  const lastCheckedBeijing = backend.last_checked_beijing || '未知';
                  const updatedAtBeijing = backend.updated_at_beijing || '未知';
                  const version = backend.version || 'subconverter';
                  
                  const statusClass = healthy ? 'health-up' : 'health-down';
                  const statusText = healthy ? '正常' : '异常';
                  const weightPercentage = Math.round((weight / maxWeight) * 100);
                  
                  return `
                  <div class="backend-card ${healthy ? 'backend-healthy' : 'backend-unhealthy'}">
                      <div class="backend-card-header">
                          <span class="health-indicator ${statusClass}"></span>
                          <span class="backend-status-badge ${healthy ? 'status-healthy' : 'status-unhealthy'}">
                              ${statusText}
                          </span>
                          <div class="weight-display">
                              <div class="weight-bar" title="权重: ${weight}">
                                  <div class="weight-fill" style="width: ${weightPercentage}%"></div>
                              </div>
                              <span class="weight-value">${weight}</span>
                          </div>
                      </div>
                      
                      <div class="backend-url-container">
                          <div class="backend-url" title="${url}">
                              ${url.replace(/^https?:\/\//, '')}
                          </div>
                          <span class="backend-version">${version}</span>
                      </div>
                      
                      <div class="backend-stats-grid">
                          <div class="stat-item">
                              <span class="stat-label">成功率</span>
                              <span class="stat-value ${successRate >= 90 ? 'stat-good' : successRate >= 70 ? 'stat-warning' : 'stat-bad'}">
                                  ${successRate.toFixed(1)}%
                              </span>
                          </div>
                          <div class="stat-item">
                              <span class="stat-label">平均响应</span>
                              <span class="stat-value ${avgResponseTime < 300 ? 'stat-good' : avgResponseTime < 800 ? 'stat-warning' : 'stat-bad'}">
                                  ${avgResponseTime ? Math.round(avgResponseTime) + 'ms' : '未知'}
                              </span>
                          </div>
                          <div class="stat-item">
                              <span class="stat-label">请求数</span>
                              <span class="stat-value">${requestCount}</span>
                          </div>
                          <div class="stat-item">
                              <span class="stat-label">失败数</span>
                              <span class="stat-value">${failureCount}</span>
                          </div>
                      </div>
                      
                      <div class="backend-meta-details">
                          <div class="meta-detail">
                              <span class="meta-label">最后成功:</span>
                              <span class="meta-value">${lastSuccessBeijing}</span>
                          </div>
                          <div class="meta-detail">
                              <span class="meta-label">最后检查:</span>
                              <span class="meta-value">${lastCheckedBeijing}</span>
                          </div>
                          <div class="meta-detail">
                              <span class="meta-label">最后更新:</span>
                              <span class="meta-value">${updatedAtBeijing}</span>
                          </div>
                      </div>
                      
                      ${healthy ? '' : `
                      <div class="backend-error-info">
                          <span class="error-icon">⚠️</span>
                          <span class="error-text">后端异常，已降低权重</span>
                      </div>`}
                  </div>`;
                }).join('')}
            </div>
        </div>
        ` : ''}
        
        <div class="weight-stats">
            <h3>⚖️ 权重统计</h3>
            <div class="backend-meta">
                <span class="meta-item">平均后端权重: ${Math.round(avgBackendWeight)}</span>
                <span class="meta-item">平均请求权重: ${Math.round(avgRequestWeight)}</span>
                <span class="meta-item">基准权重: ${baseWeight}</span>
                <span class="meta-item">权重范围: ${minWeight}-${maxWeight}</span>
                <span class="meta-item">权重调整因子: ${weightAdjustmentFactor}</span>
                <span class="meta-item">权重恢复速率: ${weightRecoveryRate}/分钟</span>
            </div>
        </div>
        
        <!-- ==== 优化的Telegram通知部分 ==== -->
        <div class="telegram-stats-section">
            <h3>Telegram通知统计</h3>
            
            <div class="telegram-stats-cards">
                <div class="telegram-stat-card">
                    <div class="telegram-stat-value">${tgTotalSent}</div>
                    <div class="telegram-stat-label">通知总数</div>
                    <div class="telegram-stat-breakdown">
                        <div class="telegram-stat-item">
                            <div class="telegram-stat-item-value">${statusData.d1Stats?.today?.telegram_notifications || 0}</div>
                            <div class="telegram-stat-item-label">今日</div>
                        </div>
                        <div class="telegram-stat-item">
                            <div class="telegram-stat-item-value">${Math.min(statusData.telegramNotifications.length, 5)}</div>
                            <div class="telegram-stat-item-label">最近</div>
                        </div>
                    </div>
                </div>
                
                <div class="telegram-stat-card">
                    <div class="telegram-stat-value">${statusData.d1Stats?.total?.telegram_notifications || 0}</div>
                    <div class="telegram-stat-label">历史总计</div>
                    <div class="telegram-stat-breakdown">
                        <div class="telegram-stat-item">
                            <div class="telegram-stat-item-value">${statusData.d1Stats?.total?.telegram_notifications || 0}</div>
                            <div class="telegram-stat-item-label">全部</div>
                        </div>
                        <div class="telegram-stat-item">
                            <div class="telegram-stat-item-value">${Math.round((statusData.d1Stats?.today?.telegram_notifications || 0) / Math.max(tgTotalSent, 1) * 100)}%</div>
                            <div class="telegram-stat-item-label">今日占比</div>
                        </div>
                    </div>
                </div>
                
                <div class="telegram-stat-card">
                    <div class="telegram-stat-value">${todayRequestCount > 0 ? Math.round(((statusData.d1Stats?.today?.telegram_notifications || 0) / todayRequestCount) * 100) : 0}%</div>
                    <div class="telegram-stat-label">通知率</div>
                    <div class="telegram-stat-breakdown">
                        <div class="telegram-stat-item">
                            <div class="telegram-stat-item-value">${todayRequestCount}</div>
                            <div class="telegram-stat-item-label">今日请求</div>
                        </div>
                        <div class="telegram-stat-item">
                            <div class="telegram-stat-item-value">${statusData.d1Stats?.today?.telegram_notifications || 0}</div>
                            <div class="telegram-stat-item-label">今日通知</div>
                        </div>
                    </div>
                </div>
                
                <div class="telegram-stat-card">
                    <div class="telegram-stat-value">${getConfig(env, 'NOTIFY_ON_REQUEST', true) ? '✅' : '❌'}</div>
                    <div class="telegram-stat-label">通知状态</div>
                    <div class="telegram-stat-breakdown">
                        <div class="telegram-stat-item">
                            <div class="telegram-stat-item-value">${getConfig(env, 'NOTIFY_ON_REQUEST', true) ? '开' : '关'}</div>
                            <div class="telegram-stat-item-label">请求</div>
                        </div>
                        <div class="telegram-stat-item">
                            <div class="telegram-stat-item-value">${getConfig(env, 'NOTIFY_ON_HEALTH_CHANGE', true) ? '开' : '关'}</div>
                            <div class="telegram-stat-item-label">健康</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="telegram-notifications-container">
            <div class="telegram-notifications-header">
                <h4>最近Telegram通知</h4>
                <div class="notification-filter" id="notificationFilter">
                    <button class="filter-btn active" data-filter="all">全部</button>
                    <button class="filter-btn" data-filter="request">请求通知</button>
                    <button class="filter-btn" data-filter="health-change">健康变化</button>
                    <button class="filter-btn" data-filter="error">错误通知</button>
                </div>
            </div>
            
            ${statusData.telegramNotifications.length > 0 ? `
            <div class="telegram-notifications-list" id="notificationsList">
                ${statusData.telegramNotifications.slice(0, 5).map(notification => {
                    const isSuccess = notification.success === 1;
                    const type = notification.notification_type || 'unknown';
                    const typeClass = type === 'request' ? 'request' : 
                                     type === 'health_change' ? 'health-change' : 
                                     type === 'error' ? 'error' : 'unknown';
                    const typeBadgeText = type === 'request' ? '📡 请求' : 
                                         type === 'health_change' ? '🌡️ 健康变化' : 
                                         type === 'error' ? '⚠️ 错误' : '📢 通知';
                    const typeIcon = type === 'request' ? '📡' : 
                                    type === 'health_change' ? '🌡️' : 
                                    type === 'error' ? '⚠️' : '📢';
                    
                    const time = notification.beijing_time || notification.sent_time || '未知时间';
                    const shortTime = time.length > 16 ? time.substring(11, 16) : time;
                    const fullTime = time;
                    
                    const message = notification.message || '无消息内容';
                    const shortMessage = message.length > 80 ? message.substring(0, 80) + '...' : message;
                    const fullMessage = message;
                    
                    const requestId = notification.request_id || '';
                    const clientIp = notification.client_ip || '';
                    const backendUrl = notification.backend_url || '';
                    const statusCode = notification.status_code || '';
                    
                    const notificationId = notification.id || Math.random().toString(36).substr(2, 9);
                    
                    return `
                <div class="notification-card ${typeClass} ${isSuccess ? 'success' : 'error'}" data-type="${type}">
                    <div class="notification-header">
                        <span class="notification-type-badge ${typeClass}">
                            <span class="icon">${typeIcon}</span>
                            <span class="text">${typeBadgeText}</span>
                        </span>
                        <span class="notification-time" title="${fullTime}">${shortTime}</span>
                    </div>
                    
                    <div class="notification-message" id="message-${notificationId}">
                        ${shortMessage}
                    </div>
                    
                    <div class="notification-details">
                        ${requestId ? `
                        <div class="notification-detail" title="请求ID: ${requestId}">
                            <span class="icon">🆔</span>
                            <span class="text">${requestId.substring(0, 8)}${requestId.length > 8 ? '...' : ''}</span>
                        </div>` : ''}
                        
                        ${clientIp ? `
                        <div class="notification-detail" title="客户端IP: ${clientIp}">
                            <span class="icon">📍</span>
                            <span class="text">${clientIp}</span>
                        </div>` : ''}
                        
                        ${backendUrl ? `
                        <div class="notification-detail" title="后端地址: ${backendUrl}">
                            <span class="icon">🔗</span>
                            <span class="text">${backendUrl.replace(/^https?:\/\//, '').substring(0, 15)}${backendUrl.length > 15 ? '...' : ''}</span>
                        </div>` : ''}
                        
                        ${statusCode ? `
                        <div class="notification-detail" title="状态码: ${statusCode}">
                            <span class="icon">🔢</span>
                            <span class="text">${statusCode}</span>
                        </div>` : ''}
                    </div>
                    
                    ${message.length > 80 ? `
                    <button class="notification-expand-btn" data-id="${notificationId}" onclick="toggleNotificationMessage('${notificationId}')">
                        <span class="expand-icon">📖</span>
                        <span class="expand-text">展开详情</span>
                    </button>` : ''}
                </div>`;
                }).join('')}
            </div>
            ` : `
            <div class="telegram-empty-state">
                <div class="telegram-empty-icon">📭</div>
                <div class="telegram-empty-text">暂无Telegram通知记录</div>
                <div class="telegram-empty-subtext">点击下方"测试TG通知"按钮发送第一条通知</div>
            </div>
            `}
        </div>
        
        <div class="d1-stats">
            <h3>💾 D1数据库统计</h3>
            <div class="backend-meta">
                <span class="meta-item">今日写入: ${d1DailyWrites}次</span>
                <span class="meta-item">总写入: ${d1TotalWrites}次</span>
                <span class="meta-item">健康检查: ${statusData.d1Stats?.total?.health_checks || 0}条</span>
                <span class="meta-item">请求记录: ${statusData.d1Stats?.total?.request_results || 0}条</span>
                <span class="meta-item">TG通知: ${statusData.d1Stats?.total?.telegram_notifications || 0}条</span>
                ${statusData.d1Stats?.weight_statistics ? `
                <span class="meta-item">平均权重: ${Math.round(statusData.d1Stats.weight_statistics.avg_weight)}</span>
                <span class="meta-item">最小权重: ${statusData.d1Stats.weight_statistics.min_weight}</span>
                <span class="meta-item">最大权重: ${statusData.d1Stats.weight_statistics.max_weight}</span>
                ` : ''}
                ${statusData.d1Stats?.comparison ? `
                <span class="meta-item">昨日对比: ${statusData.d1Stats.comparison.change_percentage}</span>
                ` : ''}
            </div>
        </div>
        
        <div class="info-section">
            <h3>📋 系统信息</h3>
            <ul>
                <li><strong>数据来源:</strong> backend_status 表（实时北京时间）</li>
                <li><strong>后端状态更新:</strong> 定时任务（每2分钟）+ 每次请求后更新</li>
                <li><strong>负载均衡算法:</strong> ${lbAlgorithmName}（智能加权轮询）</li>
                <li><strong>流式代理:</strong> ${streamingEnabled ? '启用' : '禁用'}</li>
                <li><strong>权重计算:</strong> 基于健康状态、响应时间、成功率、稳定性等多维度动态调整</li>
                <li><strong>平滑调整:</strong> 使用指数加权移动平均，避免权重突变</li>
                <li><strong>权重恢复:</strong> 每分钟向基准权重恢复 ${weightRecoveryRate} 点</li>
                <li><strong>定时任务:</strong> 每2分钟执行一次健康检查并更新所有后端状态</li>
                <li><strong>订阅转换请求:</strong> 每次请求结果都更新后端状态并发送TG通知</li>
                <li><strong>TG通知:</strong> ${getConfig(env, 'NOTIFY_ON_REQUEST', true) ? '启用' : '禁用'}请求通知，${getConfig(env, 'NOTIFY_ON_HEALTH_CHANGE', true) ? '启用' : '禁用'}健康变化通知</li>
                <li><strong>并发控制:</strong> 最大 ${getConfig(env, 'CONCURRENT_HEALTH_CHECKS', 5)} 个并发检查</li>
                <li><strong>请求ID:</strong> ${requestId}</li>
            </ul>
        </div>
        
        <div class="action-buttons">
            <button class="action-btn" id="healthCheckBtn">🚀 手动健康检查</button>
            <button class="action-btn action-btn-telegram" id="testTelegramBtn">📱 测试TG通知</button>
            <a href="/api/d1-stats" class="action-btn" target="_blank">💾 D1统计</a>
            <a href="/api/config" class="action-btn" target="_blank">⚙️ 配置信息</a>
            <button class="action-btn action-btn-info" id="testLoadBalancerBtn">⚖️ 测试负载均衡</button>
            <a href="/api/weight-stats" class="action-btn action-btn-warning" target="_blank">📊 权重统计</a>
            <button class="action-btn action-btn-secondary" id="resetWeightsBtn">🔄 重置权重</button>
            <button class="action-btn action-btn-danger" id="cleanupD1Btn">🗑️ 清理旧数据</button>
            <a href="/api/diagnose" class="action-btn" target="_blank">🔍 系统诊断</a>
            <a href="/api/init-database" class="action-btn" target="_blank" style="background: #6f42c1;">🗃️ 初始化数据库</a>
        </div>
        
        <div class="footer">
            <div>🚀 智能加权轮询版本: 动态权重调整 + 平滑权重变化 + 多维健康评估</div>
            <div>📊 权重算法: 健康状态 × 响应时间 × 成功率 × 稳定性 × 时间衰减</div>
            <div>🔔 Telegram通知: 带重试机制的错误处理</div>
            <div>🔄 版本: 智能加权版 2.0.0 | 优化权重算法和响应时间评估</div>
        </div>
    </div>
    
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            // 按钮事件绑定
            const healthCheckBtn = document.getElementById('healthCheckBtn');
            const testTelegramBtn = document.getElementById('testTelegramBtn');
            const testLoadBalancerBtn = document.getElementById('testLoadBalancerBtn');
            const resetWeightsBtn = document.getElementById('resetWeightsBtn');
            const cleanupD1Btn = document.getElementById('cleanupD1Btn');
            
            if (healthCheckBtn) healthCheckBtn.addEventListener('click', performHealthCheck);
            if (testTelegramBtn) testTelegramBtn.addEventListener('click', testTelegramNotification);
            if (testLoadBalancerBtn) testLoadBalancerBtn.addEventListener('click', testLoadBalancer);
            if (resetWeightsBtn) resetWeightsBtn.addEventListener('click', resetWeights);
            if (cleanupD1Btn) cleanupD1Btn.addEventListener('click', cleanupD1Data);
            
            const buttons = document.querySelectorAll('.action-btn');
            buttons.forEach(btn => {
                btn.addEventListener('touchstart', function() {
                    this.style.opacity = '0.8';
                });
                
                btn.addEventListener('touchend', function() {
                    this.style.opacity = '1';
                });
            });
            
            let lastTouchEnd = 0;
            document.addEventListener('touchend', function(event) {
                const now = Date.now();
                if (now - lastTouchEnd <= 300) {
                    event.preventDefault();
                }
                lastTouchEnd = now;
            }, false);
            
            // 立即健康检查按钮功能
            const immediateCheckBtn = document.getElementById('immediateCheckBtn');
            const checkStatus = document.getElementById('checkStatus');
            
            if (immediateCheckBtn) {
                immediateCheckBtn.addEventListener('click', async function() {
                    immediateCheckBtn.disabled = true;
                    immediateCheckBtn.textContent = '检查中...';
                    if (checkStatus) {
                        checkStatus.textContent = '正在执行健康检查，请稍候...';
                        checkStatus.style.color = '#17a2b8';
                    }
                    
                    try {
                        const response = await fetch('/api/health-check-immediate', {
                            method: 'GET',
                            headers: {
                                'Content-Type': 'application/json'
                            }
                        });
                        
                        const data = await response.json();
                        
                        if (data.success) {
                            if (checkStatus) {
                                checkStatus.textContent = '✅ 健康检查完成！页面即将刷新...';
                                checkStatus.style.color = '#28a745';
                            }
                            
                            setTimeout(() => {
                                window.location.reload();
                            }, 3000);
                        } else {
                            if (checkStatus) {
                                checkStatus.textContent = '❌ 健康检查失败: ' + (data.error || '未知错误');
                                checkStatus.style.color = '#dc3545';
                            }
                            immediateCheckBtn.disabled = false;
                            immediateCheckBtn.textContent = '🔄 立即执行健康检查';
                        }
                    } catch (error) {
                        if (checkStatus) {
                            checkStatus.textContent = '❌ 请求失败: ' + error.message;
                            checkStatus.style.color = '#dc3545';
                        }
                        immediateCheckBtn.disabled = false;
                        immediateCheckBtn.textContent = '🔄 立即执行健康检查';
                    }
                });
            }
            
            // Telegram通知过滤功能
            const filterBtns = document.querySelectorAll('.filter-btn');
            filterBtns.forEach(btn => {
                btn.addEventListener('click', function() {
                    const filter = this.getAttribute('data-filter');
                    
                    // 更新按钮状态
                    filterBtns.forEach(b => b.classList.remove('active'));
                    this.classList.add('active');
                    
                    // 过滤通知
                    filterNotifications(filter);
                });
            });
            
            // 初始化消息展开状态
            initNotificationMessages();
        });
        
        // 过滤Telegram通知
        function filterNotifications(filter) {
            const notificationCards = document.querySelectorAll('.notification-card');
            
            notificationCards.forEach(card => {
                const type = card.getAttribute('data-type');
                
                if (filter === 'all' || filter === type) {
                    card.style.display = 'block';
                    // 添加淡入动画
                    setTimeout(() => {
                        card.style.opacity = '1';
                        card.style.transform = 'translateY(0)';
                    }, 10);
                } else {
                    card.style.display = 'none';
                }
            });
            
            // 如果没有显示任何通知，显示提示
            const visibleCards = Array.from(notificationCards).filter(card => 
                card.style.display !== 'none'
            );
            
            const emptyState = document.querySelector('.telegram-empty-state');
            if (visibleCards.length === 0 && emptyState) {
                emptyState.style.display = 'block';
            } else if (emptyState) {
                emptyState.style.display = 'none';
            }
        }
        
        // 初始化通知消息展开状态
        function initNotificationMessages() {
            const expandButtons = document.querySelectorAll('.notification-expand-btn');
            expandButtons.forEach(btn => {
                const messageId = btn.getAttribute('data-id');
                const messageElement = document.getElementById('message-' + messageId);
                if (messageElement) {
                    // 保存完整消息
                    const fullMessage = messageElement.innerHTML;
                    messageElement.setAttribute('data-full', fullMessage);
                    
                    // 设置初始状态
                    messageElement.classList.remove('expanded');
                }
            });
        }
        
        // 切换通知消息展开/收起
        function toggleNotificationMessage(id) {
            const messageElement = document.getElementById('message-' + id);
            const button = document.querySelector('[data-id="' + id + '"]');
            
            if (!messageElement || !button) return;
            
            const isExpanded = messageElement.classList.contains('expanded');
            const fullMessage = messageElement.getAttribute('data-full') || messageElement.innerHTML;
            
            if (isExpanded) {
                // 收起
                const shortMessage = fullMessage.length > 80 ? 
                    fullMessage.substring(0, 80) + '...' : 
                    fullMessage;
                messageElement.innerHTML = shortMessage;
                messageElement.classList.remove('expanded');
                
                const expandText = button.querySelector('.expand-text');
                if (expandText) expandText.textContent = '展开详情';
                
                const expandIcon = button.querySelector('.expand-icon');
                if (expandIcon) expandIcon.textContent = '📖';
            } else {
                // 展开
                messageElement.innerHTML = fullMessage;
                messageElement.classList.add('expanded');
                
                const expandText = button.querySelector('.expand-text');
                if (expandText) expandText.textContent = '收起详情';
                
                const expandIcon = button.querySelector('.expand-icon');
                if (expandIcon) expandIcon.textContent = '📘';
            }
        }
        
        function performHealthCheck() {
            const btn = document.getElementById('healthCheckBtn');
            const originalText = btn.textContent;
            
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
                    showToast('健康检查完成！页面即将刷新...', 'success');
                    setTimeout(() => {
                        window.location.reload();
                    }, 1500);
                } else {
                    showToast('健康检查失败：' + (data.error || '未知错误'), 'error');
                    btn.innerHTML = originalText;
                    btn.disabled = false;
                }
            })
            .catch(error => {
                showToast('请求失败：' + error.message, 'error');
                btn.innerHTML = originalText;
                btn.disabled = false;
            });
        }
        
        function testTelegramNotification() {
            const btn = document.getElementById('testTelegramBtn');
            const originalText = btn.textContent;
            
            btn.innerHTML = '📤 发送中...';
            btn.disabled = true;
            
            showToast('正在发送测试通知...', 'info');
            
            fetch('/api/test-telegram-notification', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                }
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showToast('测试通知发送成功！3秒后刷新页面查看记录', 'success');
                    setTimeout(() => {
                        window.location.reload();
                    }, 3000);
                } else {
                    showToast('测试通知发送失败：' + (data.message || '未知错误'), 'error');
                    btn.innerHTML = originalText;
                    btn.disabled = false;
                }
            })
            .catch(error) {
                showToast('请求失败：' + error.message, 'error');
                btn.innerHTML = originalText;
                btn.disabled = false;
            });
        }
        
        function testLoadBalancer() {
            const btn = document.getElementById('testLoadBalancerBtn');
            const originalText = btn.textContent;
            
            btn.innerHTML = '⚖️ 测试中...';
            btn.disabled = true;
            
            showToast('正在测试负载均衡算法...', 'info');
            
            fetch('/api/lb-test', {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                }
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showToast('负载均衡测试完成，页面即将刷新', 'success');
                    setTimeout(() => {
                        window.location.reload();
                    }, 1500);
                } else {
                    showToast('负载均衡测试失败：' + (data.error || '未知错误'), 'error');
                    btn.innerHTML = originalText;
                    btn.disabled = false;
                }
            })
            .catch(error) {
                showToast('请求失败：' + error.message, 'error');
                btn.innerHTML = originalText;
                btn.disabled = false;
            });
        }
        
        function resetWeights() {
            if (confirm('确定要重置所有后端权重吗？权重将恢复到基准值。')) {
                const btn = document.getElementById('resetWeightsBtn');
                const originalText = btn.textContent;
                
                btn.innerHTML = '🔄 重置中...';
                btn.disabled = true;
                
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
                        showToast('权重重置完成！页面即将刷新...', 'success');
                        setTimeout(() => {
                            window.location.reload();
                        }, 1000);
                    } else {
                        showToast('权重重置失败：' + (data.error || '未知错误'), 'error');
                        btn.innerHTML = originalText;
                        btn.disabled = false;
                    }
                })
                .catch(error) {
                    showToast('请求失败：' + error.message, 'error');
                    btn.innerHTML = originalText;
                    btn.disabled = false;
                });
            }
        }
        
        function cleanupD1Data() {
            if (confirm('确定要清理7天前的旧数据吗？此操作不可撤销。')) {
                const btn = document.getElementById('cleanupD1Btn');
                const originalText = btn.textContent;
                
                btn.innerHTML = '🗑️ 清理中...';
                btn.disabled = true;
                
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
                        btn.innerHTML = originalText;
                        btn.disabled = false;
                }
                })
                .catch(error) {
                    showToast('请求失败：' + error.message, 'error');
                    btn.innerHTML = originalText;
                    btn.disabled = false;
                });
            }
        }
        
        function showToast(message, type = 'info') {
            const existingToast = document.querySelector('.toast-notification');
            if (existingToast) {
                existingToast.remove();
            }
            
            const toast = document.createElement('div');
            toast.className = 'toast-notification';
            toast.innerHTML = message;
            
            toast.style.position = 'fixed';
            toast.style.bottom = '20px';
            toast.style.left = '50%';
            toast.style.transform = 'translateX(-50%)';
            toast.style.backgroundColor = type === 'success' ? '#28a745' : 
                                         type === 'error' ? '#dc3545' : 
                                         type === 'info' ? '#17a2b8' : 
                                         type === 'warning' ? '#ffc107' : '#007bff';
            toast.style.color = type === 'warning' ? '#212529' : 'white';
            toast.style.padding = '12px 20px';
            toast.style.borderRadius = '8px';
            toast.style.zIndex = '1000';
            toast.style.boxShadow = '0 4px 12px rgba(0,0,0,0.15)';
            toast.style.fontSize = '14px';
            toast.style.maxWidth = '90%';
            toast.style.textAlign = 'center';
            toast.style.animation = 'fadeIn 0.3s ease';
            
            document.body.appendChild(toast);
            
            setTimeout(() => {
                toast.style.animation = 'fadeOut 0.3s ease';
                setTimeout(() => {
                    if (toast.parentNode) {
                        toast.remove();
                    }
                }, 300);
            }, 3000);
        }
        
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
        
        let lastActivity = Date.now();
        const refreshInterval = 60000;
        
        ['click', 'touchstart', 'scroll', 'keydown'].forEach(event => {
            document.addEventListener(event, () => {
                lastActivity = Date.now();
            });
        });
        
        setInterval(() => {
            const now = Date.now();
            if (now - lastActivity > refreshInterval) {
                if (confirm('页面已加载60秒，是否刷新以获取最新数据？')) {
                    window.location.reload();
                } else {
                    lastActivity = now;
                }
            }
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
  } catch (error) {
    logError('创建状态页面失败', error, requestId);
    return new Response('状态页面暂时不可用，请稍后重试', {
      status: 500,
      headers: { 'Content-Type': 'text/html; charset=utf-8' }
    });
  }
}