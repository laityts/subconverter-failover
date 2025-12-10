import { 
  getConfig, 
  TG_API_URL, 
  DEFAULT_NOTIFY_ON_REQUEST, 
  DEFAULT_NOTIFY_ON_HEALTH_CHANGE, 
  DEFAULT_NOTIFY_ON_ERROR,
  formatTelegramMessage,
  getBeijingTimeString
} from './utils.js';
import { SafeD1Database } from './database.js';

export class ResilientTelegramNotifier {
  constructor(env) {
    this.env = env;
    this.maxRetries = 3;
    this.retryDelay = 1000;
    this.fallbackEnabled = true;
  }
  
  async sendNotification(notificationData, requestId, ctx) {
    const botToken = this.env.TG_BOT_TOKEN;
    const chatId = this.env.TG_CHAT_ID;
    
    if (!botToken || !chatId) {
      console.log(`[${requestId}] Telegram通知配置不完整，跳过发送`);
      return { success: false, reason: '配置不完整' };
    }
    
    const notificationSettings = {
      request: getConfig(this.env, 'NOTIFY_ON_REQUEST', DEFAULT_NOTIFY_ON_REQUEST),
      health_change: getConfig(this.env, 'NOTIFY_ON_HEALTH_CHANGE', DEFAULT_NOTIFY_ON_HEALTH_CHANGE),
      error: getConfig(this.env, 'NOTIFY_ON_ERROR', DEFAULT_NOTIFY_ON_ERROR)
    };
    
    if (!notificationSettings[notificationData.type]) {
      return { success: false, reason: '通知类型已禁用' };
    }
    
    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        const message = formatTelegramMessage(notificationData);
        
        const response = await fetch(`${TG_API_URL}${botToken}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text: message,
            parse_mode: 'HTML',
            disable_web_page_preview: true
          })
        });
        
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const result = await response.json();
        
        if (result.ok) {
          console.log(`[${requestId}] Telegram通知发送成功 (尝试 ${attempt}/${this.maxRetries})`);
          
          // 保存到数据库
          await this.saveNotificationToDB(notificationData, requestId, message, true);
          
          return { success: true, attempt };
        } else {
          throw new Error(`Telegram API错误: ${result.description || '未知错误'}`);
        }
      } catch (error) {
        console.error(`[${requestId}] Telegram通知尝试 ${attempt}/${this.maxRetries} 失败:`, error.message);
        
        if (attempt < this.maxRetries) {
          const delay = this.retryDelay * Math.pow(2, attempt - 1);
          await new Promise(resolve => setTimeout(resolve, delay));
        } else {
          if (this.fallbackEnabled) {
            await this.fallbackNotification(notificationData, requestId);
          }
          
          // 保存失败的记录到数据库
          await this.saveNotificationToDB(notificationData, requestId, `发送失败: ${error.message}`, false);
          
          return { 
            success: false, 
            attempt, 
            error: error.message,
            usedFallback: this.fallbackEnabled
          };
        }
      }
    }
  }
  
  async saveNotificationToDB(notificationData, requestId, message, success) {
    try {
      if (this.env.DB) {
        const db = new SafeD1Database(this.env.DB, this.env);
        
        await db.saveTelegramNotification({
          notification_type: notificationData.type || 'unknown',
          request_id: requestId,
          client_ip: notificationData.client_ip || 'unknown',
          backend_url: notificationData.backend_url || '',
          status_code: notificationData.status_code || 0,
          response_time: notificationData.response_time || 0,
          success: success,
          message: message.substring(0, 500)
        }, requestId);
        
        console.log(`[${requestId}] Telegram通知记录保存成功`);
      }
    } catch (error) {
      console.warn(`[${requestId}] 保存Telegram通知记录失败:`, error.message);
    }
  }
  
  async fallbackNotification(notificationData, requestId) {
    console.log(`[${requestId}] 使用备用通知方案`);
    
    const message = `[备用通知] ${JSON.stringify(notificationData, null, 2)}`;
    console.log(`[${requestId}] 备用通知内容:`, message.substring(0, 200));
    
    return { success: true, fallback: true };
  }
}