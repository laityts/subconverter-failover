// 数据库初始化脚本
import { getBeijingTimeString, getBeijingDateString } from './utils.js';

export async function initDatabase(db) {
  const startTime = Date.now();
  console.log(`开始初始化数据库...`);
  
  try {
    // 检查表结构
    const tables = [
      'health_check_results',
      'backend_status',
      'request_results',
      'telegram_notifications',
      'error_logs'
    ];
    
    let createdTables = 0;
    
    for (const table of tables) {
      try {
        const tableExists = await db
          .prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`)
          .bind(table)
          .first();
          
        if (!tableExists) {
          console.log(`表 ${table} 不存在，需要创建`);
          createdTables++;
        } else {
          console.log(`表 ${table} 已存在`);
        }
      } catch (error) {
        console.log(`检查表 ${table} 时出错:`, error.message);
      }
    }
    
    if (createdTables > 0) {
      console.log(`检测到 ${createdTables} 个表需要创建，执行动态创建...`);
      await createTablesDynamically(db);
    }
    
    // 插入默认后端配置
    const defaultBackends = [
      'https://url.v1.mk',
      'https://api.sub.zaoy.cn',
      'https://subapi.sosoorg.com',
      'https://subapi.cmliussss.net'
    ];
    
    let insertedBackends = 0;
    for (const backendUrl of defaultBackends) {
      try {
        const existing = await db
          .prepare('SELECT id FROM backend_status WHERE backend_url = ?')
          .bind(backendUrl)
          .first();
          
        if (!existing) {
          const beijingTime = getBeijingTimeString();
          await db
            .prepare(`
              INSERT INTO backend_status 
              (backend_url, healthy, weight, failure_count, request_count, success_count, 
               success_rate, avg_response_time, version, last_checked_beijing, 
               last_success_beijing, created_at_beijing, updated_at_beijing)
              VALUES (?, 0, 50, 0, 0, 0, 1.0, 0, 'subconverter', ?, ?, ?, ?)
            `)
            .bind(
              backendUrl, 
              beijingTime,              // last_checked_beijing (北京时间)
              null,                     // last_success_beijing (从未成功)
              beijingTime,              // created_at_beijing (北京时间)
              beijingTime               // updated_at_beijing (北京时间)
            )
            .run();
          insertedBackends++;
          console.log(`插入默认后端: ${backendUrl}, 时间: ${beijingTime}`);
        }
      } catch (error) {
        console.log(`插入后端 ${backendUrl} 时出错:`, error.message);
      }
    }
    
    const duration = Date.now() - startTime;
    console.log(`数据库初始化完成，用时 ${duration}ms，创建了 ${createdTables} 个表，插入了 ${insertedBackends} 个默认后端`);
    
    return {
      success: true,
      tables_checked: tables.length,
      tables_created: createdTables,
      default_backends_inserted: insertedBackends,
      duration_ms: duration,
      beijing_time: getBeijingTimeString()
    };
    
  } catch (error) {
    console.error('数据库初始化失败:', error);
    return {
      success: false,
      error: error.message,
      duration_ms: Date.now() - startTime,
      beijing_time: getBeijingTimeString()
    };
  }
}

// 动态创建表（修复表结构，只保留北京时间字段）
async function createTablesDynamically(db) {
  try {
    // 创建健康检查结果表
    await db.prepare(`
      CREATE TABLE IF NOT EXISTS health_check_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        beijing_time TEXT NOT NULL,
        results TEXT NOT NULL,
        available_backend TEXT,
        fastest_response_time INTEGER DEFAULT 0,
        backend_changed INTEGER DEFAULT 0,
        weight_statistics TEXT
      )
    `).run();
    
    // 创建后端状态表（只保留北京时间字段）
    await db.prepare(`
      CREATE TABLE IF NOT EXISTS backend_status (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        backend_url TEXT NOT NULL UNIQUE,
        healthy INTEGER DEFAULT 0,
        last_checked_beijing TEXT,
        weight INTEGER DEFAULT 50,
        failure_count INTEGER DEFAULT 0,
        request_count INTEGER DEFAULT 0,
        success_count INTEGER DEFAULT 0,
        success_rate REAL DEFAULT 1.0,
        avg_response_time INTEGER DEFAULT 0,
        last_success_beijing TEXT,
        version TEXT DEFAULT 'subconverter',
        response_time INTEGER DEFAULT 0,
        created_at_beijing TEXT,
        updated_at_beijing TEXT
      )
    `).run();
    
    // 创建请求结果表
    await db.prepare(`
      CREATE TABLE IF NOT EXISTS request_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT NOT NULL,
        client_ip TEXT,
        backend_url TEXT,
        backend_selection_time INTEGER DEFAULT 0,
        response_time INTEGER DEFAULT 0,
        status_code INTEGER DEFAULT 0,
        success INTEGER DEFAULT 0,
        timestamp TEXT NOT NULL,
        beijing_time TEXT NOT NULL,
        backend_weight INTEGER DEFAULT 50
      )
    `).run();
    
    // 创建Telegram通知记录表
    await db.prepare(`
      CREATE TABLE IF NOT EXISTS telegram_notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        notification_type TEXT NOT NULL,
        request_id TEXT,
        client_ip TEXT,
        backend_url TEXT,
        status_code INTEGER DEFAULT 0,
        response_time INTEGER DEFAULT 0,
        success INTEGER DEFAULT 0,
        message TEXT,
        sent_time TEXT NOT NULL,
        beijing_time TEXT NOT NULL
      )
    `).run();
    
    // 创建错误日志表
    await db.prepare(`
      CREATE TABLE IF NOT EXISTS error_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT,
        context TEXT,
        error_message TEXT,
        stack_trace TEXT,
        timestamp TEXT NOT NULL,
        beijing_time TEXT NOT NULL
      )
    `).run();
    
    // 创建索引
    const indexes = [
      'CREATE INDEX IF NOT EXISTS idx_health_check_timestamp ON health_check_results(timestamp)',
      'CREATE INDEX IF NOT EXISTS idx_backend_status_url ON backend_status(backend_url)',
      'CREATE INDEX IF NOT EXISTS idx_backend_status_healthy ON backend_status(healthy)',
      'CREATE INDEX IF NOT EXISTS idx_request_results_time ON request_results(timestamp)',
      'CREATE INDEX IF NOT EXISTS idx_request_results_backend ON request_results(backend_url)',
      'CREATE INDEX IF NOT EXISTS idx_telegram_notifications_time ON telegram_notifications(sent_time)',
      'CREATE INDEX IF NOT EXISTS idx_backend_status_beijing ON backend_status(updated_at_beijing)',
      'CREATE INDEX IF NOT EXISTS idx_beijing_time ON health_check_results(beijing_time)',
      'CREATE INDEX IF NOT EXISTS idx_request_beijing_time ON request_results(beijing_time)'
    ];
    
    for (const indexSql of indexes) {
      try {
        await db.prepare(indexSql).run();
      } catch (error) {
        console.log(`创建索引失败: ${indexSql}`, error.message);
      }
    }
    
    console.log('所有表创建完成');
    return true;
    
  } catch (error) {
    console.error('动态创建表失败:', error);
    throw error;
  }
}