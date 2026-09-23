package tasks

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Draining asks every running sync to stop after its pass, and returns only
// once they have all finished — not when the stop has merely been requested.
func TestDrainStopsSyncsAndWaitsForThem(t *testing.T) {
	tm := newTestManager()
	task := runningSync(tm, "t-1")

	drained := make(chan error, 1)
	go func() { drained <- tm.Drain(context.Background()) }()

	select {
	case <-task.stop:
	case <-time.After(time.Second):
		t.Fatal("the sync was not signalled to stop")
	}
	select {
	case err := <-drained:
		t.Fatalf("Drain returned (%v) while the sync was still on its pass", err)
	case <-time.After(50 * time.Millisecond):
	}

	tm.finishTask(task, tm.convertSyncResult(&SyncResult{Success: true, Cancelled: true}, task), nil)

	select {
	case err := <-drained:
		if err != nil {
			t.Errorf("Drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Drain did not return once the last task finished")
	}
	if got, _ := tm.GetTask("t-1"); got.Status != TaskStatusCancelled {
		t.Errorf("status = %s, want cancelled", got.Status)
	}
}

// With nothing running there is nothing to wait for.
func TestDrainWithNothingRunningReturnsAtOnce(t *testing.T) {
	tm := newTestManager()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := tm.Drain(ctx); err != nil {
		t.Errorf("Drain: %v", err)
	}
}

// A pass that outlasts the drain is reported, so the caller knows cancelling
// now cuts it off.
func TestDrainGivesUpAtItsDeadline(t *testing.T) {
	tm := newTestManager()
	runningSync(tm, "t-1")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := tm.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Drain: %v, want DeadlineExceeded", err)
	}
}

// Once draining, no task is started — a cron slot firing during shutdown
// would otherwise begin a pass that then gets cut off.
func TestNoTaskIsAcceptedWhileDraining(t *testing.T) {
	tm := newTestManager()
	if err := tm.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	task := tm.createTask(&TaskRequest{ID: "late", Type: TaskTypeSync, Provider: "volcengine"})
	if err := tm.addTask(task); !errors.Is(err, ErrShuttingDown) {
		t.Errorf("addTask while draining: %v, want ErrShuttingDown", err)
	}
}
