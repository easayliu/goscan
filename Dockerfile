# 多阶段构建，减小镜像大小
FROM golang:1.24.3-alpine AS builder

# 安装必要的构建工具
RUN apk add --no-cache git gcc musl-dev sqlite-dev

# 设置工作目录
WORKDIR /app

# 复制 go mod 文件
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 复制源代码
COPY . .

# 安装 swag 并生成文档
RUN go install github.com/swaggo/swag/cmd/swag@latest
RUN swag init -g cmd/server/main.go --output docs

# 构建应用
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags "-s -w" -o goscan cmd/server/main.go

# 最终运行镜像
FROM alpine:latest

# 安装运行时依赖
RUN apk --no-cache add ca-certificates sqlite tzdata

# 创建非root用户
RUN addgroup -g 1000 goscan && \
    adduser -D -s /bin/sh -u 1000 -G goscan goscan

# 设置工作目录
WORKDIR /app

# 从构建阶段复制二进制文件
COPY --from=builder /app/goscan .
COPY --from=builder /app/config.yaml .
COPY --from=builder /app/docs ./docs

# 创建数据目录
RUN mkdir -p /app/data && chown -R goscan:goscan /app

# 切换到非root用户
USER goscan

# 暴露端口
EXPOSE 8080

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# 启动应用
CMD ["./goscan"]