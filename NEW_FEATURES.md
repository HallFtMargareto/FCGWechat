# 新功能说明

本次修改主要实现了以下功能：

## 1. 修改解密逻辑

- **只解密指定类型文件**：现在只解密 `contact.db` 和所有 `message_x.db` 文件
- **文件变化检测**：保存文件的更新时间，如果文件没有变化则跳过处理
- **限制message数据库数量**：通过配置文件设置最多处理几个最新的message数据库

## 2. 内存数据库处理

- **不保存本地文件**：解密后的数据库直接保存在内存中，不写入磁盘
- **缓存管理**：内存中的数据库会被缓存，避免重复解密

## 3. 增量数据处理

- **联系人数据**：记录最后处理的联系人ID，下次只处理新数据
- **消息数据**：针对每个消息表记录最后处理的local_id，支持增量处理

## 4. WebSocket数据发送

- **联系人数据发送**：通过WebSocket的SendContact方法发送联系人数据
- **消息数据发送**：通过WebSocket的SendFcgMessage方法发送消息数据

## 5. 任务锁机制

- **防重复处理**：定时器任务加锁，确保同一时间只有一个任务在处理
- **状态保存**：处理完成后保存状态到缓存

## 配置说明

创建 `config.json` 文件：

```json
{
  "database": {
    "max_message_db_count": 3
  },
  "websocket": {
    "server_host": "localhost",
    "server_port": "888",
    "path": "/websocket",
    "scheme": "ws",
    "require_auth": false,
    "reconnect": true,
    "max_reconnect": 10,
    "heartbeat_interval": "30s"
  }
}
```

## 使用方法

1. 启动WebSocket服务器（端口888）
2. 运行程序：`./chatlog.exe -debug`
3. 程序会自动：
   - 获取微信账户信息
   - 连接WebSocket服务器
   - 定期扫描并处理数据库文件
   - 增量发送新的联系人和消息数据

## 数据结构

### 联系人数据 (FcgContact)
```json
{
  "tenant_id": 1,
  "username": "user001",
  "nick_name": "张三",
  "alias": "小张",
  "local_type": 1,
  "pin_yin_initial": "ZS",
  "quan_pin": "zhangsan",
  "big_head_url": "https://example.com/avatar/big/001.jpg",
  "small_head_url": "https://example.com/avatar/small/001.jpg",
  "remark": "好朋友",
  "description": "我的好朋友张三",
  "hash": "hash001"
}
```

### 消息数据 (FcgMessage)
```json
{
  "tenant_id": 1,
  "user_name": "user001",
  "nick_name": "张三",
  "local_id": 1001,
  "sort_seq": 1,
  "server_id": 0,
  "local_type": 1,
  "create_time": 1642582800,
  "real_sender_id": 1001,
  "message_content": "大家好，我是张三！",
  "status": 1,
  "recognition_status": false,
  "message_no": "MSG_1642582800_1001",
  "task_list": ""
}
```

## 技术实现

### 主要模块分工

1. **main.go**: 
   - 数据库解密和内存管理
   - 文件状态跟踪
   - 全局任务调度

2. **sendmsg.go**: 
   - 数据库数据读取
   - WebSocket数据发送
   - 增量处理状态管理

### 关键特性

- **内存数据库**：使用SQLite的`:memory:`模式，避免磁盘IO
- **增量处理**：基于ID的增量处理，避免重复发送
- **状态持久化**：使用RBblot缓存处理状态
- **错误恢复**：支持WebSocket重连和错误处理
- **并发安全**：使用互斥锁保护共享状态