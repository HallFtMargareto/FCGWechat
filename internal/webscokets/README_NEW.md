# FCGameServer WebSocket 客户端

一个功能完整的WebSocket客户端，支持JWT认证、自动重连、配置管理等功能。

## 功能特性

### 🔐 认证功能
- **JWT Token认证**: 支持Bearer Token认证
- **灵活认证配置**: 可启用/禁用认证
- **认证状态检查**: 自动验证Token有效性

### 🔄 连接管理
- **自动重连**: 连接断开时自动重连
- **连接状态监控**: 实时监控连接状态
- **优雅关闭**: 正确关闭WebSocket连接
- **连接超时控制**: 可配置连接超时时间

### 💗 心跳机制
- **自动心跳**: 定期发送心跳保持连接
- **心跳配置**: 可配置心跳间隔
- **心跳监控**: 监控心跳响应状态

### ⚙️ 配置管理
- **静态配置**: 关键参数作为常量，方便修改
- **配置文件**: 支持JSON配置文件
- **命令行参数**: 支持命令行参数覆盖
- **交互式配置**: 支持交互式配置向导

### 📊 监控和日志
- **结构化日志**: 详细的日志记录
- **统计信息**: 连接统计和性能监控
- **错误处理**: 完善的错误处理机制

### 🛡️ 安全特性
- **Origin检查**: 支持Origin头验证
- **子协议支持**: 支持WebSocket子协议
- **SSL/TLS**: 支持WSS加密连接

## 静态配置常量

客户端的关键参数已设置为静态常量，位于文件顶部，方便修改：

```go
const (
    // WebSocket服务器配置
    WSServer         = "localhost"  // 服务器地址
    WSPort           = "888"        // 服务器端口
    WSPath           = "/websocket" // WebSocket路径
    WSScheme         = "ws"         // 协议 (ws 或 wss)
    
    // 认证配置
    RequireAuth      = false // 是否需要JWT认证
    DefaultToken     = ""    // 默认JWT Token
    
    // 客户端配置
    ClientVersion    = "1.0.0"
    HeartbeatInterval = 30 * time.Second
    ReconnectInterval = 5 * time.Second
    MaxReconnectCount = 10
    // ... 更多配置
)
```

## 使用方法

### 1. 基本使用

直接运行（使用默认配置）：
```bash
go run main.go
```

### 2. 命令行参数

```bash
# 指定服务器和端口
go run main.go -h localhost -p 888

# 使用JWT认证
go run main.go -t "your_jwt_token_here" --auth

# 使用WSS加密连接
go run main.go --wss -h example.com -p 443

# 显示帮助信息
go run main.go --help
```

**支持的命令行参数：**
- `-h, --host <host>`: WebSocket服务器地址
- `-p, --port <port>`: WebSocket服务器端口
- `-t, --token <token>`: JWT Token（启用认证）
- `--auth`: 强制启用认证
- `--no-auth`: 禁用认证
- `--wss`: 使用WSS（加密）协议
- `--help`: 显示帮助信息

### 3. 配置文件

客户端会自动查找 `client-config.json` 配置文件。如果不存在，可以复制示例配置：

```bash
cp client-config-example.json client-config.json
```

然后编辑 `client-config.json`：

```json
{
  "server_host": "localhost",
  "server_port": "888",
  "path": "/websocket",
  "scheme": "ws",
  "token": "your_jwt_token_here",
  "require_auth": true,
  "version": "1.0.0",
  "heartbeat": "30s",
  "reconnect": true,
  "max_reconnect": 10
}
```

## JWT认证配置

### 服务器端配置

确保服务器端启用了JWT认证。在 `config.yaml` 中：

```yaml
websocket:
  require-auth: true
  # 其他配置...
```

### 获取JWT Token

1. **登录获取Token**：首先通过API登录获取JWT Token
2. **配置Token**：将Token配置到客户端
3. **自动认证**：客户端会自动在握手时发送认证头

### Token格式

客户端会在WebSocket握手时发送以下头部：
```
Authorization: Bearer <your_jwt_token>
```

## 测试数据

客户端内置了测试数据生成功能，会自动发送：

1. **联系人数据** (`FcgContact`)
   - 张三、李四、王五等示例联系人
   - 包含头像、拼音、备注等完整信息

2. **消息数据** (`FcgMessage`)
   - 模拟聊天消息
   - 包含用户信息、时间戳、消息内容等

### 自定义测试数据

可以修改 `generateSampleContacts()` 和 `generateSampleMessages()` 函数来自定义测试数据。

## 编程方式使用

```go
package main

import (
    "log"
    "time"
)

func main() {
    // 创建自定义配置
    config := ClientConfig{
        ServerHost:    "localhost",
        ServerPort:    "888",
        Path:          "/websocket",
        Scheme:        "ws",
        RequireAuth:   true,
        Token:         "your_jwt_token",
        Heartbeat:     30 * time.Second,
        Reconnect:     true,
        MaxReconnect:  10,
    }
    
    // 创建客户端
    client := NewWebSocketClient(config)
    
    // 连接到服务器
    err := client.ConnectWithRetry()
    if err != nil {
        log.Fatalf("连接失败: %v", err)
    }
    defer client.Close()
    
    // 启动消息监听
    go client.ListenMessages()
    
    // 启动心跳
    go client.StartHeartbeat()
    
    // 发送消息
    err = client.SendTextMessage("Hello, Server!")
    if err != nil {
        log.Printf("发送消息失败: %v", err)
    }
    
    // 发送自定义数据
    contact := FcgContact{
        Username: "test_user",
        NickName: "测试用户",
        // ... 其他字段
    }
    err = client.SendContact(contact)
    if err != nil {
        log.Printf("发送联系人失败: %v", err)
    }
    
    // 等待...
    time.Sleep(time.Minute)
}
```

## 错误处理

客户端提供了完善的错误处理：

- **连接错误**：自动重连机制
- **认证错误**：详细的认证失败信息
- **网络错误**：网络异常检测和恢复
- **协议错误**：WebSocket协议错误处理

## 监控和调试

### 日志级别

客户端使用结构化日志记录，包含：
- 连接状态变化
- 消息发送/接收
- 错误和异常
- 心跳和重连事件

### 统计信息

客户端提供实时统计信息：
```go
stats := client.GetStats()
fmt.Printf("统计信息: %+v\n", stats)
```

包含：
- 连接状态
- 消息计数
- 重连次数
- 连接时长等

## 性能优化

- **连接池**：复用WebSocket连接
- **缓冲区优化**：合理设置读写缓冲区大小
- **心跳优化**：智能心跳间隔调整
- **重连策略**：指数退避重连算法

## 安全注意事项

1. **Token安全**：妥善保管JWT Token，避免泄露
2. **WSS加密**：生产环境建议使用WSS加密连接
3. **Origin验证**：配置正确的Origin头
4. **权限控制**：确保服务器端有适当的权限验证

## 常见问题

### Q: 连接失败怎么办？
A: 检查服务器地址、端口是否正确，确认服务器是否正在运行。

### Q: 认证失败怎么办？
A: 检查JWT Token是否正确，是否过期，服务器是否启用了认证。

### Q: 为什么收不到消息？
A: 检查消息监听是否正常启动，网络连接是否稳定。

### Q: 如何自定义配置？
A: 可以通过配置文件、命令行参数或直接修改代码中的常量。

## 许可证

本项目采用 MIT 许可证。