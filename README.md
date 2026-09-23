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
| `volcengine_bill` | 火山引擎账单明细，一行一个计费项 | `BillPeriod`（账期 `2026-09`）/ `ExpenseDate` | `PayableAmount` 等 |
| `alicloud_bill_monthly` | 阿里云月度账单 | `billing_cycle`（`2026-09`） | `pretax_amount` / `payment_amount` |
| `alicloud_bill_daily` | 阿里云日度账单 | `billing_date`（`Date`） | 同上 |

两朵云的列名口径跟着各自的 API 走 —— 火山是 PascalCase、阿里云是 snake_case，
没有强行统一：账单字段有几十个且各家含义并不一一对应，硬映射成一套「通用列」，
对不上的那些只能丢掉或者塞进备注里，真要对账时反而查不回原始值。要跨云汇总就在
查询里对齐那几个关键列。

**金额一律是 `Decimal(20, 8)`**，直接求和即可：

```sql
-- 上个月各产品花了多少（火山）
select ProductZh, sum(PayableAmount) as amount
from logs.volcengine_bill
where BillPeriod = '2026-08'
group by ProductZh order by amount desc;

-- 上个月各产品花了多少（阿里云）
select product_name, sum(payment_amount) as amount
from logs.alicloud_bill_monthly
where billing_cycle = '2026-08'
group by product_name order by amount desc;
```

火山的金额过去按 API 原样存成 `String`，每次求和都要 `toFloat64OrZero`，排序与跳数索引都用不上；
阿里云的则是 `Float64`。2026-09 起两边都改为 `Decimal`：金额不能走二进制浮点，月度汇总差一分钱
就是一笔对不上的账。阿里云 SDK 交出来的金额已经是 `float64`，入库时取能还原成它的最短十进制串，
也就是接口原样打印的那个数；从那以后求和都在 `Decimal` 上做。

### 去重是怎么保证的

三张表都是 `ReplacingMergeTree(updated_at)`，去重键就是各自的排序键。这里要同时防住两件事：
同一行拉两遍留下两份（金额翻倍），以及两条不同的行撞上同一个键、合并时只剩一条（金额变少）。

* **排序键只放业务身份，不含任何金额**。云厂商会在月中修正账单（退款、优惠重算、发票折扣），
  金额若是键的一部分，修正后的行与旧行键不同，两行都会留下，账期直接算两遍。
* **身份要能把每一条不同的行分开**。火山用 API 自带的行 id `BillDetailId`。阿里云没有单行 id，
  身份是拼出来的：账号 + 产品 + 实例 + 付费方式 + 账单类型 + **`item`**（订单 / 后付账单 / 退款 / 调账）
  + 业务类型 + 地域 / 可用区 + 拆分项。`item` 是关键：同一实例的订单和它的退款其余各列完全一样，
  少了它两行撞键，合并后只剩一条。地域 / 可用区分开的是没有实例 id 的行（资源包、云市场、短信这类）。
* **兜底是 `line_seq`**。拼出来的身份总可能还有撞上的，所以同步时把「排序键其余各列都相同」的行
  按拉到的顺序编号 0、1、2…，写进键里。重拉同一个账期会从 0 重新编号，落回同样的位置、替换掉旧行；
  而两条真不同的行不再共用一个键。它有个前提，见下一条。
* **重拉前先清空该账期**。原地替换只能替换键还会回来的行：云厂商后来撤掉的行（冲掉的退款、
  少了一条因而再也编不到旧 `line_seq` 的那组）不会回来，会留在表里被多算一次。所以不管是
  「条数对不上」触发的重拉，还是勾了 `force_update` 的重拉，都先按分区清掉这个账期再写。
* **版本列是 `updated_at`**，后拉到的那一份胜出；没有版本列时引擎只会任取一行，重拉反而可能把旧金额留下。
* **集群上的分片键是 `cityHash64(<排序键>)`，不是 `rand()`**。`ReplacingMergeTree` 的去重只发生在分片内，
  `rand()` 会把同一行的两次写入丢到不同分片，后台 merge 和 `FINAL` 都收不掉——这正是账单金额翻倍最隐蔽的一条路。

去重仍然发生在后台 merge，**刚写完就查要加 `final` 或等一次 merge**。goscan 自己比对条数时
（决定一个账期要不要重拉）数的就是 `FINAL` 之后的行数 —— 不加的话，刚重拉完、还没合并的账期
会把新旧两份都算上，被误判成不一致，又清掉重拉一遍。

> 2026-09 的结构调整不能原地升级：引擎、排序键、分区键和列类型都是建表时定死的，
> `CREATE TABLE IF NOT EXISTS` 对已存在的表不起作用。老表要先 `DROP` 再按新 DDL 建，然后重新同步
> ——账单随时可以按账期重拉，代价只是一次同步。
>
> 阿里云两张表在同月又改了一次（金额改 `Decimal`，排序键加入 `item` / `line_seq`），在那之前建的
> 同样要 `DROP` 重建再重拉；只跑 `--ddl` 的补列语句会把两列加上，但它们不在键里，撞键的行照样被合并掉。
> 老表的金额列还是 `Float64`，新版本写进去会直接报错，不会悄悄写坏。火山那张表只是 `RoundAmount`
> 改成了 `Decimal`，用 `ALTER TABLE ... MODIFY COLUMN` 原地就能改。

表结构的唯一定义在 [`pkg/ddl`](pkg/ddl)，建表语句由它渲染，同步写入也用它 ——
加一列只改一个地方，两边不会各改各的。

## 启动

```bash
goscan                                 # 读默认路径的配置（见下）
goscan /etc/goscan/config.yaml         # 指定配置
goscan --check /etc/goscan/config.yaml # 只校验配置，不连库
goscan --ddl   /etc/goscan/config.yaml # 打印建表语句，不连库
goscan --drop-legacy /etc/goscan/config.yaml # 打印删除旧表名的迁移语句，不连库
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
（有 `access_key` 没有 `secret_key`）会报错。另外 `--check` 会拿 `scheduler.jobs`
反查：**配了某朵云的定时任务、那朵云却没有凭据，直接判失败** —— K8s 上 Secret 忘了填
正好就是「凭据全空」的样子，不这么查的话 Pod 照常起来，凌晨两点才发现一条都没同步。

`sync_mode` 现在的合法值是 `standard` / `sync-optimal`（通知任务用 `cost_report`）。
老配置里的 `all_periods` / `current_period` / `range` 仍然能通过校验，但会在加载时
告警并按 `standard` 执行 —— 那几个值执行器从来就没认过，配了的任务一直在运行时失败。

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
一次下发到所有节点），**`<表名>` 本身是它上面的 `Distributed`**，也就是同步实际写入、
opdash 实际查询的那张（和运行时的表名解析规则一致，有测试对拍住）。
`replicated: true` 时本地表用 `ReplicatedReplacingMergeTree`，需要
ClickHouse Keeper；集群是一堆无副本分片就保持 `false`。

这套命名和 logpipe / tracepipe / metricpipe 是同一个口径（`app_log` + `app_log_local`），
好处是**一个表名在单机和集群上都成立** —— opdash 那边配 `volcengine_bill` 就行，
不用再按部署形态猜后缀。

`--ddl` 不建库，`CREATE DATABASE` 由部署那一步负责（集群模式下建库也得 `ON CLUSTER`）。

### 从旧表名升级

旧版本里火山那张表叫 `volcengine_bill_details`，集群上的 `Distributed` 表还带
`_distributed` 后缀。升级上来的部署先跑一遍 `--ddl` 建好新表，再用 `--drop-legacy`
把旧的删掉：

```bash
goscan --ddl         config.yaml | clickhouse-client --host <ck> --database logs --queries-file -
goscan --drop-legacy config.yaml | clickhouse-client --host <ck> --database logs --queries-file -
```

`--drop-legacy` 同样只打印不执行，而且只列当前配置不再使用的表名 —— 配置里仍然
写着 `bill_table: volcengine_bill_details` 的部署，那张表不会被碰。

`*_distributed` 自己不存数据，删掉只是去掉一层空壳；**`volcengine_bill_details`
存着旧的火山账单行**，删了要等下次调度按 `max_historical_months` 重新拉回来。
想保住历史数据就照脚本头部注释里的 `RENAME TABLE` 改名，然后把对应那条 `DROP` 去掉。
阿里云两张表名字没变，集群上 `_local` 就是原来那张，数据不受影响。

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
| `granularity` | 阿里云专用，选写哪张表：`monthly` → `alicloud_bill_monthly`，`daily` → `alicloud_bill_daily`，`both` → 两张都写 |
| `bill_period` | 单个账期，`YYYY-MM` 或 `YYYY-MM-DD` |
| `start_period` `end_period` | 账期区间，闭区间 |
| `force_update` | 已有数据也重拉 |
| `limit` | 最多同步多少条，0 = 不限 |

火山引擎只有 `volcengine_bill` 一张表，`granularity` 对它不起作用。阿里云不传
`granularity` 时，粒度由账期格式决定：`2026-09` 走月表，`2026-09-15` 走日表 ——
所以按日补数有两种写法，传 `granularity: daily` 或者直接给一个 `YYYY-MM-DD` 的账期。

同一份定义在 `/swagger` 上也能看到（字段说明和取值枚举由 `pkg/models.SyncTriggerRequest`
生成，接口就是绑这个结构体，不会和文档对不上）。

`force_update` 管的是「已经有数据的账期要不要再拉一遍」：不勾时先比对该账期在库里的条数
和接口报的条数，一致就跳过、不一致就先清掉再拉；勾上则不做这次比对，要什么账期拉什么账期。
勾上它时 `sync-optimal` 按 `standard` 走 —— 那个模式本身就是「只补缺的」，和「都重拉」
凑在一起只能二选一，以显式勾上的那个为准。

账期区间和粒度是**乘起来**的：`2026-01` 至 `2026-06` 配 `granularity: both`，
一次任务要拉 12 趟 —— 6 个账期各拉一趟月表、一趟日表。日表按天逐日调用云厂商接口，
因此补一个月的日账单比补一个月的月账单慢得多，补历史时留足时间。

**接口立刻返回，同步在后台跑**，拿 `task_id` 轮询 `GET /tasks/{task_id}` 看结果：
`status` 是 `running` / `completed` / `failed`，完成后 `result` 里有
`records_processed`、`duration`，失败的话 `error` 里是原因。`progress` 报的是
「拉到第几趟」——`period` 是当前账期，`granularity` 是这一趟写哪张表，
`done` / `total` 的单位就是上面那个乘出来的趟数。

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

打 `v*` tag 触发 `.github/workflows/release.yml`：先编出 linux/amd64 的二进制
（纯静态，不依赖 GLIBC），Release 挂这个包，**镜像直接装同一份二进制** ——
不在镜像里重编一遍，所以镜像里的 `goscan` 和 Release 里的是同一个文件，
`goscan --version` 报的就是 tag。

只出 linux/amd64 这一种架构（部署目标就这一种），因此镜像构建不需要 QEMU；
macOS 包也不打，本地开发用 `make build`。要再加架构的话看 `Dockerfile` 顶上的说明。

`Dockerfile` 只负责装配、本身不编译：它要求 `dist/linux/amd64/goscan` 已经存在，
直接 `docker build .` 会失败，本地构建镜像用 `make image`（先编再构建）。

## 开发

```bash
make build        # 构建本地二进制（CGO_ENABLED=0）
make dist         # 交叉编译镜像要装的 linux/amd64 二进制
make image        # 先 dist 再构建容器镜像
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
