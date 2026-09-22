package logger

import (
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap/zapcore"
)

// An empty path means stdout only. The config comment and the deployment both
// rely on it: the container has no writable directory, and the log collector
// reads stdout anyway.
func TestProductionLoggerWithoutAPathWritesNoFile(t *testing.T) {
	dir := t.TempDir()
	// Run from a directory we can see, so a stray "./logs/app.log" shows up.
	restore := chdir(t, dir)
	defer restore()

	log, err := NewProductionLogger("", zapcore.InfoLevel)
	if err != nil {
		t.Fatalf("building the logger failed: %v", err)
	}
	log.Info("hello")
	_ = log.Sync()

	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("an empty log path still created %v", entries)
	}
}

func TestProductionLoggerWritesTheConfiguredFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "app.log")

	log, err := NewProductionLogger(path, zapcore.InfoLevel)
	if err != nil {
		t.Fatalf("building the logger failed: %v", err)
	}
	log.Info("hello")
	_ = log.Sync()

	if _, err := os.Stat(path); err != nil {
		t.Errorf("the configured log file was not written: %v", err)
	}
}

// A log file that cannot be opened must not stop the process from starting:
// that turned a read-only working directory into CrashLoopBackOff.
func TestProductionLoggerFallsBackWhenTheDirectoryIsNotWritable(t *testing.T) {
	dir := t.TempDir()
	readonly := filepath.Join(dir, "readonly")
	if err := os.Mkdir(readonly, 0o500); err != nil {
		t.Fatalf("setting up the read-only directory failed: %v", err)
	}

	log, err := NewProductionLogger(filepath.Join(readonly, "sub", "app.log"), zapcore.InfoLevel)
	if err != nil {
		t.Fatalf("an unwritable log directory was fatal: %v", err)
	}
	if log == nil {
		t.Fatal("no logger was returned")
	}
	log.Info("still logging to stdout")
}

func chdir(t *testing.T, dir string) func() {
	t.Helper()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	return func() { _ = os.Chdir(previous) }
}

// Library code logs through the package-level helpers without knowing whether
// the process has set logging up yet; before this default existed, the first
// such call panicked on a nil Logger.
func TestPackageHelpersWorkBeforeInit(t *testing.T) {
	if Logger == nil {
		t.Fatal("Logger is nil before InitLogger")
	}
	Info("this must not panic")
	Warn("neither must this")
}
