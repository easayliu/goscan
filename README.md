# goscan

多云账单同步入库。和 [logpipe](../log)、[tracepipe](../trace)、[metricpipe](../metric) 同一套部署口径，
写进同一个 ClickHouse 库，[opdash](../opdash) 一个连接就能把日志、链路、指标和**花了多少钱**一起查：
**把火山引擎、阿里云的账单按账期拉下来，批量写进 ClickHouse。**

```text
  ┌──────────────┐        ┌──────────────────────┐        ┌────────────┐
  │   云厂商 API   │  分页  │      内置 cron        │ 批量写  │ ClickHouse │
  │ 火山 / 阿里云  ├───────▶│ 按账期拉 / 重试 / 补数 ├───────▶│  账单明细表 │
  └──────────────┘        └──────────────────────┘        └────────────┘
                                    │
                                    └──▶ 企业微信费用日报（可出图）
```

四兄弟的分工：logpipe 采日志、tracepipe 采 trace、metricpipe 采指标、goscan 拉账单，
opdash 负责查。前三个是「应用自己吐出来的数据」，goscan 是「去云厂商那儿拉回来的数据」——
所以它不是常驻监听端口的采集器，而是按账期定时去拉。

## 数据表

三张表，都在配置里的 `clickhouse.database`（部署用的是 `logs`，和另外三个项目同库）：

| 表 | 内容 | 时间列 | 金额列 |
| --- | --- | --- | --- |
| `volcengine_bill_details` | 火山引擎账单明细，一行一个计费项 | `BillPeriod`（账期 `2026-09`）/ `ExpenseDate` | `PayableAmount` 等 |
| `alicloud_bill_monthly` | 阿里云月度账单 | `billing_cycle`（`2026-09`） | `pretax_amount` / `payment_amount` |
| `alicloud_bill_daily` | 阿里云日度账单 | `billing_date`（`Date`） | 同上 |

两朵云的列名口径跟着各自的 API 走 —— 火山是 PascalCase、阿里云是 snake_case，
没有强行统一：账单字段有几十个且各家含义并不一一对应，硬映射成一套「通用列」，
对不上的那些只能丢掉或者塞进备注里，真要对账时反而查不回原始值。要跨云汇总就在
查询里对齐那几个关键列。

一个坑：**火山那张表的金额是 `String`**（API 原样返回，避免精度和空值问题），
按金额排序、求和要先转：

```sql
-- 上个月各产品花了多少（火山）
select ProductZh, sum(toFloat64OrZero(PayableAmount)) as amount
from logs.volcengine_bill_details
where BillPeriod = '2026-08'
group by ProductZh order by amount desc;

-- 上个月各产品花了多少（阿里云）
select product_name, sum(payment_amount) as amount
from logs.alicloud_bill_monthly
where billing_cycle = '2026-08'
group by product_name order by amount desc;
```

三张表都是 `ReplacingMergeTree`：同一个账期重复拉不会翻倍，但去重发生在后台 merge，
刚写完就查要加 `final` 或者等一次 merge。

表结构的唯一定义在 [`pkg/ddl`](pkg/ddl)，建表语句由它渲染，同步写入也用它 ——
加一列只改一个地方，两边不会各改各的。

## 启动

```bash
goscan                                 # 读默认路径的配置（见下）
goscan /etc/goscan/config.yaml         # 指定配置
goscan --check /etc/goscan/config.yaml # 只校验配置，不连库
goscan --ddl   /etc/goscan/config.yaml # 打印建表语句，不连库
goscan --version
```

不带配置文件时按 `./config.yaml` → `~/.goscan/config.yaml` → `/etc/goscan/config.yaml` 的顺序找。
**显式指定的路径找不到会直接报错退出** —— 容器里挂错了 ConfigMap 就该起不来，
而不是拿一份没有任何密钥的默认配置假装在跑。

`Ctrl-C` / `SIGTERM` 是优雅退出：在跑的同步任务先跑完，最多等 30 秒。

### 跑一次就退出

补历史账期、手工重拉某个月，用 `--once`，账期这些维度由参数指定，不用改配置：

```bash
goscan --once config.yaml --provider volcengine                      # 按配置的默认模式拉一次
goscan --once config.yaml --provider alicloud --period 2026-08       # 指定账期
goscan --once config.yaml --provider alicloud --start 2026-01 --end 2026-06 --granularity both
goscan --once config.yaml --provider notification                    # 只发一次企微日报
```

退出码反映任务本身的成败，脚本和 `kubectl wait` 都能直接判断。

| 参数 | 说明 |
| --- | --- |
| `--provider` | `volcengine` / `alicloud` / `notification` |
| `--mode` | `standard`（老老实实按账期拉）/ `sync-optimal`（按数据量比对，只补差的那部分） |
| `--granularity` | 阿里云专用：`monthly` / `daily` / `both` |
| `--period` | 单个账期，`YYYY-MM` 或 `YYYY-MM-DD` |
| `--start` `--end` | 账期区间 |
| `--limit` | 最多同步多少条，0 = 不限 |
| `--force` | 已有数据也重新拉 |

## 配置

完整带注释的样例在 [`configs/config.daemon.yaml`](configs/config.daemon.yaml)。要紧的是这几段：

```yaml
clickhouse:
  hosts: [clickhouse-log.logck.svc.cluster.local]
  port: 9000            # native 协议；protocol 改 http 时用 8123
  database: logs        # 和 logpipe / tracepipe / metricpipe 同库
  cluster: ""           # 填了就建「本地表 + Distributed 表」
  replicated: false     # 本地表用 Replicated* 引擎，需要 Keeper

scheduler:              # 调度是 goscan 自己的 cron，不是 K8s CronJob
  enabled: true
  jobs:
    - name: "alicloud_daily_sync"
      provider: "alicloud"
      cron: "0 3 * * *"
      config:
        sync_mode: "sync-optimal"
        granularity: "both"
```

**每一项都能用环境变量覆盖**，优先级是环境变量 > 配置文件：
`CLICKHOUSE_*`、`VOLCENGINE_*`、`ALICLOUD_*`、`WECHAT_*`、`SERVER_*`、`LOG_LEVEL`。
密钥因此不用写进配置文件，容器里由 Secret 注入就行（见 `deploy/`）。

Cron 表达式是 robfig/cron v3 的语法。**时区跟着进程的 `TZ` 走** —— 容器里不设
`TZ=Asia/Shanghai` 的话，"凌晨 2 点"是 UTC 的凌晨 2 点。

凭据全空的 provider 块视为「这朵云没接」，`--check` 会跳过它；只填了一半
（有 `access_key` 没有 `secret_key`）才报错。

## 建表

goscan **自己从不执行 DDL**，`--ddl` 只把建表语句打到 stdout：

```bash
goscan --ddl config.yaml | clickhouse-client --host <ck> --database logs --queries-file -
```

这样同步进程不需要建表权限，也不会在生产库里悄悄改表结构 —— 账单表一改就是几十列，
误改比没建更麻烦。K8s 上这一步是独立的 Job，见下。

DDL 里除了 `CREATE TABLE IF NOT EXISTS`，还带一段幂等的 `ALTER ... ADD COLUMN IF NOT EXISTS`：
云厂商给账单加了字段、goscan 跟着加了列之后，**重跑一遍 DDL 就把老表缺的列按原位补上**，
已经是最新的表则什么也不做，多跑无副作用。

配了 `clickhouse.cluster` 的话，一张表会渲染成两张：`<表名>_local` 存数据（`ON CLUSTER`
一次下发到所有节点），`<表名>` 是它上面的 `Distributed`，也就是同步实际写入、opdash
实际查询的那张。`replicated: true` 时本地表用 `ReplicatedReplacingMergeTree`，需要
ClickHouse Keeper；集群是一堆无副本分片就保持 `false`。

`--ddl` 不建库，`CREATE DATABASE` 由部署那一步负责（集群模式下建库也得 `ON CLUSTER`）。

## K8s 部署

```bash
kubectl apply -f deploy/goscan-ddl-job.yaml      # 先建库建表
kubectl apply -f deploy/goscan-deployment.yaml   # 再起同步
```

* [`deploy/goscan-ddl-job.yaml`](deploy/goscan-ddl-job.yaml)：两段式 Job。initContainer 用
  goscan 镜像把 DDL 渲染到 emptyDir，主容器用 ClickHouse 镜像执行（goscan 镜像里没有
  clickhouse-client）。配置来源和 Deployment 是同一个 ConfigMap，所以建出来的表一定
  和同步写入的那张一致。升级到带新列的版本后重跑一次。
* [`deploy/goscan-deployment.yaml`](deploy/goscan-deployment.yaml)：ConfigMap + Secret +
  Deployment + Service。**副本恒为 1、`strategy: Recreate`** —— 两个副本各自跑一份 cron
  会把同一个账期拉两遍，滚动更新时新旧 Pod 并存也一样，所以先停旧的再起新的。
  initContainer 跑 `--check`：配置错了（密钥没挂上、cron 写错）就别起来，
  省得半夜才发现没同步。

和 logpipe / tracepipe / opdash 放同一个 namespace（`logging`）、连同一个 ClickHouse。
探针只看进程和 HTTP：拉不到账单不该把 Pod 重启掉，重启只会让它从头再拉一遍；
同步成没成看 `/tasks`。

手动补数不用改 Deployment：常规做法是从 opdash 触发（见下面「手动同步」），
没有 opdash 或者集群外操作时再起个一次性 Pod 跑 `--once`。

## 手动同步（给 opdash 对接）

日常同步由内置 cron 跑，**手动补一次由 opdash 触发**：集群内直接调
`http://goscan.logging.svc.cluster.local:8080`，不用 kubectl、不用改配置。

触发一次同步，参数就是账期这些维度：

```bash
curl -X POST http://goscan.logging.svc.cluster.local:8080/sync \
  -H 'Content-Type: application/json' \
  -d '{"provider":"alicloud","sync_mode":"sync-optimal","granularity":"both",
       "start_period":"2026-01","end_period":"2026-06","force_update":true}'

{"task_id":"6f1c…","status":"started","provider":"alicloud","timestamp":"…"}
```

| 字段 | 说明 |
| --- | --- |
| `provider` | 必填，`volcengine` / `alicloud` |
| `sync_mode` | `standard` / `sync-optimal`，不填走配置里的默认 |
| `granularity` | 阿里云专用，`monthly` / `daily` / `both` |
| `bill_period` | 单个账期 |
| `start_period` `end_period` | 账期区间 |
| `force_update` | 已有数据也重拉 |
| `limit` | 最多同步多少条 |

**接口立刻返回，同步在后台跑**，拿 `task_id` 轮询 `GET /tasks/{task_id}` 看结果：
`status` 是 `running` / `completed` / `failed`，完成后 `result` 里有
`records_processed`、`duration`，失败的话 `error` 里是原因。

状态码就是 opdash 那边要分的几种情况：

| 状态码 | 含义 | 界面上该怎么办 |
| --- | --- | --- |
| `200` | 已受理 | 拿 task_id 轮询 |
| `409` | **这朵云已经有同步在跑** | 提示「正在同步中」，别重复发 |
| `429` | 并发任务数到上限 | 稍后再试 |
| `400` | 参数不对 | 报错信息直接显示 |

409 是服务端的去重：同一朵云同时只允许一个同步任务，重复点按钮、或者手工触发
撞上 cron 的那一次都会被挡下来 —— 两个任务并行拉同一批账期只是浪费 API 配额，
进度还互相看不懂。cron 遇到上一轮还没跑完时同样跳过这一次，日志里是
`Skipping scheduled job`。

opdash 是只读服务，这条写路径建议由它的后端代调（浏览器不直连 goscan），
认证沿用 opdash 自己的登录，goscan 这边不再单独做一套。

其余接口（运维看状态用）：

| 路径 | 干什么 |
| --- | --- |
| `GET /health` | 健康检查，探针用 |
| `GET /tasks`、`GET /tasks/:id` | 任务列表和单个任务的结果 |
| `POST /tasks` | 和 `POST /sync` 等价的通用入口，可自带 `id` 让重试幂等 |
| `GET /sync`、`GET /sync/history` | 同步状态与历史 |
| `GET /scheduler/status`、`GET /scheduler/jobs` | 调度器和任务计划 |
| `POST /scheduler/jobs/:id/trigger` | 手工触发某个已配置的 job |
| `POST /notifications/wechat`、`POST /notifications/wechat/test` | 手工发一次企微报告、测试 webhook |
| `GET /swagger/index.html` | 完整接口文档 |

## 发布

打 `v*` tag 由 CI 构建镜像推到 GHCR（`.github/workflows/release.yml`），同时出
linux amd64 / arm64 的二进制（纯静态，不依赖 GLIBC；macOS 包不再打，本地开发用 `make build`）。
镜像是 `linux/amd64` + `linux/arm64` 的 manifest，两个架构都由 Go 交叉编译出来，
不走 QEMU 模拟。镜像里的版本号来自构建参数 `VERSION`，`goscan --version` 报的就是 tag。

## 开发

```bash
make build        # 构建（CGO_ENABLED=0，和镜像里那份一致）
make check        # 校验 configs/config.daemon.yaml
make ddl          # 打印建表语句
make test         # 跑测试
make dev          # 直接 go run
make swagger      # 重新生成 Swagger 文档（需要 swag）
```

`make` 的 `CONFIG=` 可以换配置文件：`make ddl CONFIG=/etc/goscan/config.yaml`。

目录：

```text
cmd/server/      入口：命令行解析、常驻模式、--once
pkg/ddl/         三张表的列定义 + 建表语句渲染（表结构的唯一来源）
pkg/config/      配置结构、默认值、环境变量覆盖、校验
pkg/scheduler/   内置 cron
pkg/tasks/       任务编排：同步任务、通知任务
pkg/volcengine/  火山引擎账单 API 封装
pkg/alicloud/    阿里云账单 API 封装
pkg/clickhouse/  ClickHouse 客户端、表名解析（单机 / 集群）
pkg/analysis/    费用分析、日报出图
pkg/wechat/      企业微信通知
deploy/          K8s 清单
```

## 常见问题

**改了配置没生效** —— ConfigMap 更新不会自动重启 Pod，`kubectl -n logging rollout restart deploy/goscan`。

**同步没按点跑** —— 先看 `TZ`（容器默认 UTC），再看 `scheduler.enabled` 和 cron 表达式，
然后 `GET /scheduler/jobs` 看下一次触发时间。

**表不存在** —— 同步进程不建表，跑 `deploy/goscan-ddl-job.yaml` 或者把 `--ddl` 的输出
喂给 clickhouse-client。配置里写了 `create_table` 的老 job 会在启动时告警，那个字段
已经不起作用了。

**升级后少列** —— 重跑一次 DDL Job，ALTER 段会把缺的列补上。

**账单金额对不上** —— 火山那张表的金额列是 `String`，直接 `sum()` 得到 0，要
`sum(toFloat64OrZero(PayableAmount))`；另外 `ReplacingMergeTree` 的去重在后台 merge，
刚同步完就核对要加 `final`。

**触发同步返回 409** —— 这朵云已经有同步在跑（手动的或 cron 的），等它跑完再来。
`GET /sync` 看在跑几个，`GET /tasks` 看是哪一个。

**企微通知失败** —— webhook 是否还有效；`wechat.enabled` 开着但 webhook 为空时
`--check` 不过，Pod 会起不来。

## 许可证

MIT。
