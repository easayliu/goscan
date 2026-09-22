# 构建阶段钉在构建机自己的架构上（BUILDPLATFORM），要哪个架构由 Go 交叉编译，
# 不走 QEMU —— 多架构镜像里 arm64 那一份是模拟执行编译器，慢十倍不止，
# 而 Go 交叉编译只是换两个环境变量。
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder
WORKDIR /src

# 依赖单独一层：go.mod / go.sum 没动就能命中 buildcache，改代码不用重下一遍依赖。
# 这一层在中间阶段里，所以 registry 缓存必须配 mode=max —— 默认的 min 只导出
# 最终阶段的层，压根不会把它带上。
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# swag 生成的 docs/ 已经在仓库里，镜像里不再跑一遍 swag：那要多下一整套依赖，
# 换来的只是一份和提交进去的一模一样的文件。改了注解就在本地 `make swagger`。
#
# CGO_ENABLED=0：sqlite 驱动只是 go.mod 里的间接依赖，没有代码 import 它，
# 关掉 cgo 出来的是静态二进制，不用再担心 alpine 和 glibc 的事。
# TARGETOS / TARGETARCH 由 buildx 按 --platform 自动注入
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/goscan ./cmd/server

FROM alpine:3.21
# ca-certificates：调云厂商的账单 API 走 HTTPS。
# tzdata：cron 的「凌晨 2 点」要按 TZ（见 deploy/ 里的 goscan-env）算，
#         没有时区库的话一律当 UTC
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -g 1000 goscan \
    && adduser -D -u 1000 -G goscan goscan

COPY --from=builder /out/goscan /usr/local/bin/goscan

# 出图的费用日报要中文字体，字体是按相对路径找的，所以工作目录必须是 /app
WORKDIR /app
COPY --chown=goscan:goscan assets/ /app/assets/

USER goscan
EXPOSE 8080

# 配置从 /etc/goscan 挂进来（ConfigMap），密钥走环境变量。
# ENTRYPOINT 是二进制本身，于是 args 直接就是子命令：
#   args: ["/etc/goscan/config.yaml"]            启动
#   args: ["--ddl", "/etc/goscan/config.yaml"]   打印建表语句
ENTRYPOINT ["goscan"]
CMD ["/etc/goscan/config.yaml"]
