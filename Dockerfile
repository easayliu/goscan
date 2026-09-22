# 这个 Dockerfile 只负责「把产物装进镜像」，不编译 —— 二进制由外面先编好放进
# dist/linux/amd64/goscan，CI 里就是 release.yml 的 build job 编出来的那一份，
# 镜像和 GitHub Release 里的二进制因此是同一个文件，而不是两次独立构建的结果。
#
# 本地构建先跑一次 `make image`（它会先编再构建），直接 `docker build .` 会因为
# 找不到 dist/ 而失败。
#
# 只出 linux/amd64。真要再加一个架构的话：dist/ 下按架构分目录（make dist 已经这么放），
# 把下面的路径换成 ${TARGETARCH} 并加上 ARG TARGETARCH，同时注意下面那条 RUN 会跑在
# 目标架构上、需要 docker/setup-qemu-action。
FROM alpine:3.21

# ca-certificates：调云厂商的账单 API 走 HTTPS。
# tzdata：cron 的「凌晨 2 点」要按 TZ（见 deploy/ 里的 goscan-env）算，
#         没有时区库的话一律当 UTC
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -g 1000 goscan \
    && adduser -D -u 1000 -G goscan goscan

COPY dist/linux/amd64/goscan /usr/local/bin/goscan

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
