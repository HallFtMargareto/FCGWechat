# 代理配置说明

本项目已添加代理配置功能，允许WebSocket连接通过代理服务器进行连接。

## 配置选项

在配置文件中添加了以下代理配置选项：

```json
{
  "proxy": {
    "enabled": false,        // 是否启用代理 (true/false)
    "host": "",              // 代理服务器地址 (例如: 127.0.0.1)
    "port": "",              // 代理服务器端口 (例如: 8080)
    "username": "",          // 代理认证用户名 (可选)
    "password": ""           // 代理认证密码 (可选)
  }
}
```

## 使用方法

### 1. 不使用代理（默认）

```json
{
  "proxy": {
    "enabled": false,
    "host": "",
    "port": "",
    "username": "",
    "password": ""
  }
}
```

### 2. 使用无认证的代理

```json
{
  "proxy": {
    "enabled": true,
    "host": "127.0.0.1",
    "port": "8080",
    "username": "",
    "password": ""
  }
}
```

### 3. 使用需要认证的代理

```json
{
  "proxy": {
    "enabled": true,
    "host": "proxy.example.com",
    "port": "8080",
    "username": "your_username",
    "password": "your_password"
  }
}
```

## 注意事项

1. 当 `enabled` 设置为 `false` 时，其他代理配置项将被忽略
2. 如果启用了代理但未提供 `host` 或 `port`，连接将失败
3. 代理支持HTTP代理协议
4. 用户名和密码如果包含特殊字符，会自动进行URL编码

## 示例文件

可以参考 `config-proxy-example.json` 文件查看完整的代理配置示例。

## 技术实现

代理功能通过以下方式实现：

1. 在配置结构体中添加了代理相关字段
2. 在WebSocket连接时检查代理配置
3. 如果启用了代理，使用 `golang.org/x/net/proxy` 包创建代理连接
4. 支持带认证和不带认证的代理服务器

当代理启用时，程序会在日志中输出代理信息，便于调试和确认代理配置是否正确。

## 故障排除

### 错误: "proxy: unknown scheme: http"

如果遇到此错误，请确保使用的是最新版本的代码。此问题已在最新版本中修复，现在使用 `net/http` 包的代理功能来支持 HTTP 代理，而不是 `golang.org/x/net/proxy` 包。

### 警告: "transport.Dial is deprecated"

如果看到此警告，表示代码使用了已弃用的方法。最新版本已修复此问题，现在使用 `DialContext` 方法替代 `Dial` 方法，这是 Go 官方推荐的做法，具有更好的取消机制和资源管理能力。

### 代理连接失败

1. 检查代理服务器地址和端口是否正确
2. 确认代理服务器是否正在运行
3. 如果使用认证代理，检查用户名和密码是否正确
4. 确认网络连接是否正常，可以访问代理服务器