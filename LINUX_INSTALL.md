# 🍎 Apple Stock Monitor - Linux部署指南

## 📦 下载和安装

### 1. 下载包
```bash
# 假设您已经将 apple_monitor_linux.tar.gz 上传到服务器
```

### 2. 解压安装包
```bash
tar -xzf apple_monitor_linux.tar.gz
cd apple_monitor_linux
```

### 3. 设置权限
```bash
chmod +x apple_monitor
chmod +x start.sh
```

## ⚙️ 配置

### 1. 配置Telegram通知
编辑配置文件：
```bash
nano apple_monitor.json
```

更新以下字段：
```json
{
  "telegram": {
    "enabled": true,
    "bot_token": "YOUR_BOT_TOKEN_HERE",
    "chat_id": "YOUR_CHAT_ID_HERE",
    "timeout": 30
  }
}
```

### 2. Telegram Bot设置
参考 `TELEGRAM_SETUP.md` 文件获取详细设置步骤。

## 🚀 启动服务

### 基本使用
```bash
# 启动监控
./start.sh start

# 检查状态  
./start.sh status

# 查看日志
./start.sh logs

# 停止监控
./start.sh stop

# 重启监控
./start.sh restart
```

### 后台运行
```bash
# 启动后可以安全断开SSH连接
./start.sh start

# 监控将在后台持续运行
```

## 📊 监控状态

### 查看实时日志
```bash
tail -f monitor.log
```

### 检查进程
```bash
ps aux | grep apple_monitor
```

### 查看网络连接
```bash
netstat -an | grep :443
```

## 🔧 作为系统服务运行 (可选)

### 1. 创建服务文件
```bash
sudo nano /etc/systemd/system/apple-monitor.service
```

内容：
```ini
[Unit]
Description=Apple Stock Monitor
After=network.target

[Service]
Type=simple
User=your_username
WorkingDirectory=/path/to/apple_monitor_linux
ExecStart=/path/to/apple_monitor_linux/apple_monitor
Restart=always
RestartSec=10
StandardOutput=append:/path/to/apple_monitor_linux/monitor.log
StandardError=append:/path/to/apple_monitor_linux/monitor.log

[Install]
WantedBy=multi-user.target
```

### 2. 启用和启动服务
```bash
# 重新加载systemd配置
sudo systemctl daemon-reload

# 启用服务（开机自启）
sudo systemctl enable apple-monitor

# 启动服务
sudo systemctl start apple-monitor

# 查看状态
sudo systemctl status apple-monitor

# 查看日志
sudo journalctl -u apple-monitor -f
```

### 3. 服务管理命令
```bash
# 停止服务
sudo systemctl stop apple-monitor

# 重启服务
sudo systemctl restart apple-monitor

# 禁用开机自启
sudo systemctl disable apple-monitor
```

## 🛠️ 故障排除

### 常见问题

#### 1. 权限错误
```bash
# 确保文件有执行权限
chmod +x apple_monitor
chmod +x start.sh

# 检查文件所有者
ls -la apple_monitor
```

#### 2. 端口占用
```bash
# 检查网络连接
netstat -tulpn | grep apple_monitor
```

#### 3. 配置文件错误
```bash
# 验证JSON格式
python3 -m json.tool apple_monitor.json

# 或使用jq
cat apple_monitor.json | jq .
```

#### 4. Telegram连接问题
```bash
# 测试网络连接
curl -s "https://api.telegram.org/bot${BOT_TOKEN}/getMe"

# 检查防火墙
sudo ufw status
```

#### 5. 内存不足
```bash
# 检查内存使用
free -h

# 检查进程内存占用
ps aux --sort=-%mem | head
```

### 日志分析

#### 查看错误日志
```bash
grep -i error monitor.log
grep -i failed monitor.log
```

#### 查看连接状态
```bash
grep -i "telegram message sent" monitor.log
grep -i "stock check completed" monitor.log
```

#### 统计检查次数
```bash
grep -c "Starting stock check" monitor.log
```

## 📈 性能优化

### 1. 调整检查间隔
编辑 `apple_monitor.json`：
```json
{
  "monitoring": {
    "default_interval": 30  // 增加到30秒减少服务器负载
  }
}
```

### 2. 限制并发请求
```json
{
  "monitoring": {
    "max_concurrent": 2  // 减少并发数
  }
}
```

### 3. 日志轮转
添加到 `/etc/logrotate.d/apple-monitor`：
```
/path/to/apple_monitor_linux/monitor.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    postrotate
        /path/to/apple_monitor_linux/start.sh restart > /dev/null 2>&1 || true
    endscript
}
```

## 🔒 安全建议

### 1. 文件权限
```bash
# 设置适当的文件权限
chmod 600 apple_monitor.json  # 只有所有者可读写
chmod 755 apple_monitor       # 可执行
chmod 755 start.sh           # 可执行
```

### 2. 防火墙配置
```bash
# 只允许HTTPS出站连接
sudo ufw allow out 443/tcp
```

### 3. 定期更新
```bash
# 定期检查和更新系统
sudo apt update && sudo apt upgrade  # Ubuntu/Debian
sudo yum update                       # CentOS/RHEL
```

## 📋 系统要求

- **操作系统**: Linux x86_64
- **内存**: 最少 128MB RAM
- **存储**: 最少 50MB 可用空间
- **网络**: 稳定的互联网连接
- **端口**: 需要访问HTTPS (443端口)

## 📞 技术支持

如遇问题：
1. 查看 `monitor.log` 详细日志
2. 检查 `README.md` 和 `TELEGRAM_SETUP.md`
3. 验证网络连接和配置文件
4. 确认系统资源充足

---

🎯 **祝您部署顺利，抢购成功！** 🍎