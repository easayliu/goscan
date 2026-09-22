// Command goscan syncs multi-cloud bills into ClickHouse.
//
//	goscan [配置文件]                启动服务：内置 cron 调度 + HTTP API
//	goscan --check [配置文件]        只校验配置，不连库
//	goscan --ddl   [配置文件]        打印 ClickHouse 建表语句，不连库
//	goscan --once  [配置文件] --provider volcengine
//	                                 跑一次同步就退出，账期等维度由参数指定
//
// 不带配置文件时按 ./config.yaml → ~/.goscan/ → /etc/goscan/ 的顺序找。
// 配置里的每一项都能用环境变量覆盖（CLICKHOUSE_* / VOLCENGINE_* / ALICLOUD_* …），
// 容器里密钥因此走 Secret 注入，不必写进 ConfigMap。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"goscan/pkg/config"
	"goscan/pkg/ddl"
	"goscan/pkg/logger"
	"goscan/pkg/scheduler"
	"goscan/pkg/server"
	"goscan/pkg/tasks"

	"go.uber.org/zap"
)

// @title Goscan API
// @version 1.0
// @description Cloud billing data synchronization service API documentation
// @description Supports automatic synchronization of billing data from multi-cloud platforms like VolcEngine and Alibaba Cloud to ClickHouse database
// @termsOfService http://swagger.io/terms/
// @contact.name API Support
// @contact.email support@goscan.com
// @license.name MIT
// @license.url https://opensource.org/licenses/MIT
// @host localhost:8080
// @BasePath /
// @schemes http https

const usage = `goscan —— 多云账单同步入库

用法:
    goscan [配置文件]                启动服务（内置 cron 调度 + HTTP API）
    goscan --check [配置文件]        只校验配置，不连库
    goscan --ddl   [配置文件]        打印 ClickHouse 建表语句，不连库
    goscan --once  [配置文件] --provider <云厂商> [任务参数]
                                     跑一次同步就退出

任务参数（只对 --once 生效，不写就用配置里的默认值）:
    --provider     volcengine | alicloud | notification
    --mode         standard | sync-optimal
    --granularity  monthly | daily | both（阿里云）
    --period       单个账期，YYYY-MM 或 YYYY-MM-DD
    --start --end  账期区间
    --limit        最多同步多少条，0 = 不限
    --force        已有数据也重新拉取

其他:
    --address --port   HTTP 监听地址和端口，覆盖配置文件
    --version          打印版本
`

// version 由构建时的 -ldflags "-X main.version=..." 注入。
var version = "dev"

type options struct {
	configPath string

	check       bool
	ddl         bool
	once        bool
	showVersion bool

	address string
	port    int

	provider    string
	mode        string
	granularity string
	period      string
	start       string
	end         string
	limit       int
	force       bool

	// set 记录哪些参数是命令行上真写了的，用来区分「显式指定的默认值」和「没写」。
	set map[string]bool
}

func main() {
	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n\n%s", err, usage)
		os.Exit(2)
	}

	if opts.showVersion {
		fmt.Printf("goscan %s\n", version)
		return
	}

	os.Exit(run(opts))
}

func run(opts *options) int {
	// LoadConfig 找不到文件时会退回默认配置。对着默认搜索路径这是对的，但显式指定
	// 的路径找不到几乎一定是挂载错了，这时静悄悄地用一份没有任何密钥的默认配置跑起来
	// 比直接报错糟得多。
	if opts.configPath != "" {
		if _, err := os.Stat(opts.configPath); err != nil {
			fmt.Fprintf(os.Stderr, "读取配置 %s 失败: %v\n", opts.configPath, err)
			return 1
		}
	}

	cfg, err := config.LoadConfig(opts.configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		return 1
	}

	// --ddl / --check 在 K8s 里由 Job 和 CI 跑，输出要能直接管道给 clickhouse-client，
	// 所以放在日志初始化之前：stdout 上只有 SQL，日志走 stderr。
	switch {
	case opts.ddl:
		fmt.Print(ddl.Render(ddl.Tables(cfg), ddl.OptionsFrom(cfg)))
		return 0
	case opts.check:
		if err := cfg.ValidateConfig(); err != nil {
			fmt.Fprintf(os.Stderr, "配置校验失败: %v\n", err)
			return 1
		}
		fmt.Printf("配置 %s 校验通过\n", describeConfigPath(opts.configPath))
		return 0
	}

	if err := logger.InitLogger(true, ""); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		return 1
	}
	defer logger.Sync()

	logger.Info("Starting goscan",
		zap.String("version", version),
		zap.String("config", describeConfigPath(opts.configPath)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if opts.once {
		return runOnce(ctx, cfg, opts)
	}

	app := &DaemonApp{cfg: cfg, ctx: ctx, cancel: cancel}
	address, port := opts.listenOn(cfg)
	if err := app.Start(address, port); err != nil {
		logger.Error("Failed to start goscan service", zap.Error(err))
		return 1
	}

	app.WaitForShutdown()
	logger.Info("Goscan service stopped")
	return 0
}

// parseFlags 解析命令行。配置文件既可以写在 --config 后面，也可以像 logpipe 那样
// 直接作为位置参数跟在子命令后（goscan --ddl /etc/goscan/config.yaml）。
func parseFlags(args []string) (*options, error) {
	opts := &options{set: map[string]bool{}}
	showVersion := false

	fs := flag.NewFlagSet("goscan", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }

	fs.StringVar(&opts.configPath, "config", "", "配置文件路径")
	fs.BoolVar(&opts.check, "check", false, "只校验配置")
	fs.BoolVar(&opts.ddl, "ddl", false, "打印建表语句")
	fs.BoolVar(&opts.once, "once", false, "跑一次任务就退出")
	fs.BoolVar(&showVersion, "version", false, "打印版本")

	fs.StringVar(&opts.address, "address", "0.0.0.0", "HTTP 监听地址")
	fs.IntVar(&opts.port, "port", 8080, "HTTP 监听端口")

	fs.StringVar(&opts.provider, "provider", "", "云厂商：volcengine | alicloud | notification")
	fs.StringVar(&opts.mode, "mode", "", "同步模式：standard | sync-optimal")
	fs.StringVar(&opts.granularity, "granularity", "", "粒度：monthly | daily | both")
	fs.StringVar(&opts.period, "period", "", "账期，YYYY-MM 或 YYYY-MM-DD")
	fs.StringVar(&opts.start, "start", "", "起始账期")
	fs.StringVar(&opts.end, "end", "", "结束账期")
	fs.IntVar(&opts.limit, "limit", 0, "最多同步多少条，0 = 不限")
	fs.BoolVar(&opts.force, "force", false, "已有数据也重新拉取")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	fs.Visit(func(f *flag.Flag) { opts.set[f.Name] = true })

	opts.showVersion = showVersion

	// 位置参数就是配置文件，写了就盖过 --config。后面还可以继续跟参数，
	// 这样 `goscan --once /etc/goscan/config.yaml --provider alicloud` 也认。
	seenPositional := false
	for rest := fs.Args(); len(rest) > 0; rest = fs.Args() {
		if seenPositional {
			return nil, fmt.Errorf("只能指定一个配置文件，多出来的是 %q", rest[0])
		}
		seenPositional = true
		opts.configPath = rest[0]
		if err := fs.Parse(rest[1:]); err != nil {
			return nil, err
		}
		fs.Visit(func(f *flag.Flag) { opts.set[f.Name] = true })
	}

	if count(opts.check, opts.ddl, opts.once) > 1 {
		return nil, fmt.Errorf("--check / --ddl / --once 只能用一个")
	}
	if opts.once && opts.provider == "" {
		return nil, fmt.Errorf("--once 需要 --provider 指定跑哪个云厂商")
	}
	if !opts.once && opts.provider != "" {
		return nil, fmt.Errorf("--provider 只对 --once 生效；常驻模式下的任务由配置里的 scheduler.jobs 决定")
	}

	return opts, nil
}

// listenOn 取 HTTP 监听地址：命令行显式写了就用命令行的，否则用配置文件的。
func (o *options) listenOn(cfg *config.Config) (string, int) {
	address, port := o.address, o.port
	if srv := cfg.Server; srv != nil {
		if !o.set["address"] && srv.Address != "" {
			address = srv.Address
		}
		if !o.set["port"] && srv.Port != 0 {
			port = srv.Port
		}
	}
	return address, port
}

// taskRequest 把命令行参数翻译成一次任务。没写的参数留空，由执行器按配置里的
// 默认值补齐 —— 命令行只负责「这一次跑什么」。
func (o *options) taskRequest() *tasks.TaskRequest {
	req := &tasks.TaskRequest{
		Type:     tasks.TaskTypeSync,
		Provider: o.provider,
		Config: tasks.TaskConfig{
			SyncMode:    o.mode,
			ForceUpdate: o.force,
			Granularity: o.granularity,
			BillPeriod:  o.period,
			StartPeriod: o.start,
			EndPeriod:   o.end,
			Limit:       o.limit,
		},
	}
	if o.provider == "notification" {
		req.Type = tasks.TaskTypeNotification
	}
	return req
}

// runOnce 跑一次任务就退出，用来补历史账期或者手工重跑一次同步。
// 退出码反映任务本身的成败，方便外面用 `kubectl wait` 或者脚本判断。
func runOnce(ctx context.Context, cfg *config.Config, opts *options) int {
	taskMgr, err := tasks.NewTaskManager(ctx, cfg)
	if err != nil {
		logger.Error("Failed to create task manager", zap.Error(err))
		return 1
	}

	req := opts.taskRequest()
	logger.Info("Running one-shot task",
		zap.String("provider", req.Provider),
		zap.String("type", string(req.Type)),
		zap.String("mode", req.Config.SyncMode),
		zap.String("period", req.Config.BillPeriod))

	result, err := taskMgr.ExecuteTaskSync(ctx, req)
	if err != nil {
		logger.Error("Task failed", zap.Error(err))
		return 1
	}

	// ExecuteTaskSync hands back whatever the executor produced, and a task
	// that succeeded without a result is not an error — the scheduler guards
	// the same call the same way.
	if result == nil {
		logger.Info("Task completed")
		return 0
	}

	logger.Info("Task completed",
		zap.Int("records_processed", result.RecordsProcessed),
		zap.Int("records_fetched", result.RecordsFetched),
		zap.Duration("duration", result.Duration))
	return 0
}

func count(flags ...bool) int {
	n := 0
	for _, f := range flags {
		if f {
			n++
		}
	}
	return n
}

// DaemonApp represents the main application
type DaemonApp struct {
	cfg       *config.Config
	ctx       context.Context
	cancel    context.CancelFunc
	server    *server.HTTPServer
	scheduler *scheduler.TaskScheduler
	wg        sync.WaitGroup
}

// Start initializes and starts all components of the application
func (app *DaemonApp) Start(address string, port int) error {
	logger.Info("Initializing goscan components...")

	// Initialize HTTP server
	serverConfig := &server.Config{
		Address: address,
		Port:    port,
		Config:  app.cfg,
	}

	httpServer, err := server.NewHTTPServer(app.ctx, serverConfig)
	if err != nil {
		return fmt.Errorf("failed to create HTTP server: %w", err)
	}
	app.server = httpServer

	// Initialize task scheduler
	schedulerConfig := &scheduler.Config{
		Config: app.cfg,
	}

	taskScheduler, err := scheduler.NewTaskScheduler(app.ctx, schedulerConfig)
	if err != nil {
		return fmt.Errorf("failed to create task scheduler: %w", err)
	}
	app.scheduler = taskScheduler

	// Set scheduler reference in handler service
	app.server.SetScheduler(taskScheduler)

	// Start HTTP server
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		logger.Info("Starting HTTP server", zap.String("address", address), zap.Int("port", port))
		if err := app.server.Start(); err != nil {
			logger.Error("HTTP server error", zap.Error(err))
			app.cancel() // Trigger graceful shutdown
		}
	}()

	// Start task scheduler
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		logger.Info("Starting task scheduler")
		if err := app.scheduler.Start(); err != nil {
			logger.Error("Task scheduler error", zap.Error(err))
			app.cancel() // Trigger graceful shutdown
		}
	}()

	// Wait a moment for services to start
	time.Sleep(100 * time.Millisecond)
	logger.Info("Goscan service started successfully")

	return nil
}

// WaitForShutdown waits for shutdown signals and performs graceful shutdown
func (app *DaemonApp) WaitForShutdown() {
	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal or context cancellation
	select {
	case sig := <-sigChan:
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
	case <-app.ctx.Done():
		logger.Info("Context cancelled, initiating shutdown")
	}

	// Start graceful shutdown
	app.Shutdown()
}

// Shutdown performs graceful shutdown of all components
func (app *DaemonApp) Shutdown() {
	logger.Info("Starting graceful shutdown...")

	// Cancel context to signal all components to stop
	app.cancel()

	// Create shutdown timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown components in parallel
	var shutdownWg sync.WaitGroup

	// Shutdown HTTP server
	if app.server != nil {
		shutdownWg.Add(1)
		go func() {
			defer shutdownWg.Done()
			logger.Info("Shutting down HTTP server...")
			if err := app.server.Shutdown(shutdownCtx); err != nil {
				logger.Error("Error shutting down HTTP server", zap.Error(err))
			} else {
				logger.Info("HTTP server shut down successfully")
			}
		}()
	}

	// Shutdown task scheduler
	if app.scheduler != nil {
		shutdownWg.Add(1)
		go func() {
			defer shutdownWg.Done()
			logger.Info("Shutting down task scheduler...")
			if err := app.scheduler.Shutdown(shutdownCtx); err != nil {
				logger.Error("Error shutting down task scheduler", zap.Error(err))
			} else {
				logger.Info("Task scheduler shut down successfully")
			}
		}()
	}

	// Wait for all components to shutdown with timeout
	shutdownComplete := make(chan struct{})
	go func() {
		shutdownWg.Wait()
		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		logger.Info("All components shut down successfully")
	case <-shutdownCtx.Done():
		logger.Warn("Shutdown timeout exceeded, some components may not have shut down gracefully")
	}

	// Wait for all goroutines to complete
	app.wg.Wait()
	logger.Info("Graceful shutdown completed")
}

// describeConfigPath 返回实际用到的配置文件路径，供日志和 --check 的输出使用。
func describeConfigPath(configPath string) string {
	if strings.TrimSpace(configPath) != "" {
		return configPath
	}
	return "默认搜索路径"
}
