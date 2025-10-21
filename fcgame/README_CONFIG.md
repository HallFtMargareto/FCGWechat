# 配置文件说明

## 概述

本项目使用 JSON 格式的配置文件来管理应用程序的各种配置参数。配置文件位于 `fcgame/config.json`，程序启动时会自动加载。

## 配置文件结构

### 1. WebSocket 配置 (websocket)

```json
"websocket": {
  "server": "192.168.1.209",     // WebSocket服务器地址
  "port": "9050",                // WebSocket服务器端口
  "path": "/websocket",          // WebSocket路径
  "scheme": "ws"                 // WebSocket协议 (ws 或 wss)
}
```

**参数说明：**
- `server`: WebSocket服务器的IP地址或域名
- `port`: WebSocket服务器监听的端口号
- `path`: WebSocket连接的路径
- `scheme`: 协议类型，`ws` 表示普通WebSocket，`wss` 表示安全WebSocket

### 2. 认证配置 (auth)

```json
"auth": {
  "require_auth": true,          // 是否需要JWT认证
  "token": "eyJhbGciOiJIUzI1NiIs..."  // JWT Token
}
```

**参数说明：**
- `require_auth`: 布尔值，设置为true时启用JWT认证
- `token`: JWT认证令牌，用于身份验证

### 3. 客户端配置 (client)

```json
"client": {
  "version": "1.0.0",            // 客户端版本
  "heartbeat_interval": 30,      // 心跳间隔(秒)
  "reconnect_interval": 3,       // 重连间隔(秒)
  "max_reconnect_count": 100,    // 最大重连次数
  "connect_timeout": 100         // 连接超时时间(秒)
}
```

**参数说明：**
- `version`: 客户端版本号
- `heartbeat_interval`: WebSocket心跳检测间隔时间，单位为秒
- `reconnect_interval`: 连接断开后重连的间隔时间，单位为秒
- `max_reconnect_count`: 最大重连尝试次数
- `connect_timeout`: 建立连接的超时时间，单位为秒

### 4. 消息配置 (message)

```json
"message": {
  "max_message_size": 50120,     // 最大消息大小(字节)
  "send_timeout": 100,           // 发送超时时间(秒)
  "read_timeout": 120,           // 读取超时时间(秒)
  "max_queue": 1000              // 发送最大队列数量
}
```

**参数说明：**
- `max_message_size`: 单个消息的最大允许大小，单位为字节
- `send_timeout`: 发送消息的超时时间，单位为秒
- `read_timeout`: 读取消息的超时时间，单位为秒
- `max_queue`: 消息队列的最大容量

### 5. 基础配置 (basic)

```json
"basic": {
  "min_create_time": 1757370778, // 默认最小时间戳
  "max_message_db_count": 20,    // 默认处理最新的20个数据库
  "time_layout": "2006-01-02 15:04:05",  // 时间格式
  "contact_db": "fcgame_lxr.dat" // 联系人数据库文件名
}
```

**参数说明：**
- `min_create_time`: 消息创建时间的最小时间戳
- `max_message_db_count`: 同时处理的消息数据库最大数量
- `time_layout`: 时间显示格式，遵循Go语言的time.Format格式
- `contact_db`: 联系人数据库文件的名称

## 使用方法

### 1. 复制配置文件模板

```bash
cp fcgame/config.example.json fcgame/config.json
```

### 2. 修改配置

根据实际环境修改 `fcgame/config.json` 文件中的相应参数。

### 3. 启动程序

程序启动时会自动加载配置文件：

```bash
go run main.go
```

## 注意事项

1. **配置文件格式**: 必须使用标准的JSON格式，注意逗号和引号的正确使用
2. **时间单位**: 配置文件中的时间相关参数以秒为单位，程序内部会转换为 `time.Duration`
3. **文件路径**: 确保配置文件路径正确，默认为 `fcgame/config.json`
4. **安全性**: JWT Token等敏感信息请妥善保管，不要提交到版本控制系统
5. **备份**: 建议保留配置文件的备份，以防误修改

## 默认值

如果配置文件不存在或某个配置项缺失，程序会使用以下默认值：

- WebSocket服务器: 192.168.1.209:9050
- 认证: 启用JWT认证
- 心跳间隔: 30秒
- 重连间隔: 3秒
- 最大重连次数: 100次
- 连接超时: 100秒
- 最大消息大小: 50120字节
- 发送超时: 100秒
- 读取超时: 120秒
- 消息队列大小: 1000
- 最大数据库数量: 20个

## 故障排除

1. **配置文件加载失败**: 检查文件路径和JSON格式是否正确
2. **连接失败**: 检查WebSocket服务器配置是否正确
3. **认证失败**: 检查JWT Token是否有效且未过期
4. **性能问题**: 根据需要调整超时时间和队列大小参数
