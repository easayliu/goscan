package tasks

import (
	"errors"
	"sync"
	"testing"
	"time"

	"goscan/pkg/cloudsync"
)

func mustSubscribe(t *testing.T, tm *TaskManagerImpl, id string) TaskSubscription {
	t.Helper()
	sub, err := tm.Subscribe(id)
	if err != nil {
		t.Fatalf("Subscribe(%q): %v", id, err)
	}
	t.Cleanup(sub.Close)
	return sub
}

// waitReady fails the test if nothing is announced within a second.
func waitReady(t *testing.T, sub TaskSubscription) []Task {
	t.Helper()
	select {
	case <-sub.Ready():
		return sub.Drain()
	case <-time.After(time.Second):
		t.Fatal("no change announced")
		return nil
	}
}

// Every step of a task's life reaches a subscriber: registered, running,
// progressing and finished.
func TestFeedCarriesATaskThroughItsLife(t *testing.T) {
	tm := newTestManager()
	sub := mustSubscribe(t, tm, "")

	task := &Task{ID: "t-1", Provider: "alicloud", Type: TaskTypeSync, Status: TaskStatusPending}
	if err := tm.addTask(task); err != nil {
		t.Fatal(err)
	}
	if got := waitReady(t, sub); len(got) != 1 || got[0].Status != TaskStatusPending {
		t.Fatalf("after addTask: %+v", got)
	}

	tm.updateTaskStatus(task, TaskStatusRunning)
	tm.updateTaskProgress(task, cloudsync.SyncProgress{Period: "2026-08", Granularity: "monthly", Done: 0, Total: 2, Records: 300, RecordsTotal: 1200})
	got := waitReady(t, sub)
	if len(got) != 1 {
		t.Fatalf("two changes to one task should drain as one snapshot, got %d", len(got))
	}
	if p := got[0].Progress; got[0].Status != TaskStatusRunning || p == nil || p.Records != 300 || p.RecordsTotal != 1200 {
		t.Fatalf("latest snapshot = %+v (progress %+v)", got[0], got[0].Progress)
	}

	tm.finishTask(task, &TaskResult{ID: "t-1", RecordsProcessed: 1200}, nil)
	got = waitReady(t, sub)
	if len(got) != 1 || !got[0].IsFinal() || got[0].Result == nil {
		t.Fatalf("after finishTask: %+v", got)
	}
}

// On a feed of every task, one task's update must not overwrite another's —
// above all not the other's final state.
func TestFeedKeepsTheLatestStatePerTask(t *testing.T) {
	tm := newTestManager()
	a := &Task{ID: "a", Provider: "alicloud", Type: TaskTypeSync}
	b := &Task{ID: "b", Provider: "volcengine", Type: TaskTypeSync}
	_ = tm.addTask(a)
	_ = tm.addTask(b)

	sub := mustSubscribe(t, tm, "")
	tm.finishTask(a, &TaskResult{ID: "a"}, nil)
	for i := 0; i < 50; i++ {
		tm.updateTaskProgress(b, cloudsync.SyncProgress{Records: int64(i)})
	}

	got := waitReady(t, sub)
	if len(got) != 2 {
		t.Fatalf("drained %d snapshots, want one per task: %+v", len(got), got)
	}
	if got[0].ID != "a" || got[0].Status != TaskStatusCompleted {
		t.Errorf("a's final state lost: %+v", got[0])
	}
	if got[1].ID != "b" || got[1].Progress.Records != 49 {
		t.Errorf("b's latest progress = %+v", got[1].Progress)
	}
}

// A subscriber that never reads must not hold up the sync: publishing only
// records the snapshot and moves on.
func TestPublishingNeverWaitsForASubscriber(t *testing.T) {
	tm := newTestManager()
	task := &Task{ID: "t-1", Type: TaskTypeSync}
	_ = tm.addTask(task)
	mustSubscribe(t, tm, "")
	mustSubscribe(t, tm, "t-1")

	done := make(chan struct{})
	go func() {
		for i := 0; i < 10000; i++ {
			tm.updateTaskProgress(task, cloudsync.SyncProgress{Records: int64(i)})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("updates blocked on subscribers that never read")
	}
}

// Subscribing to one task starts from its current state and sees nothing of
// the others.
func TestSingleTaskFeed(t *testing.T) {
	tm := newTestManager()
	mine := &Task{ID: "mine", Provider: "alicloud", Type: TaskTypeSync, Status: TaskStatusRunning}
	other := &Task{ID: "other", Provider: "volcengine", Type: TaskTypeSync}
	_ = tm.addTask(mine)
	_ = tm.addTask(other)

	sub := mustSubscribe(t, tm, "mine")
	if init := sub.Initial(); len(init) != 1 || init[0].ID != "mine" || init[0].Status != TaskStatusRunning {
		t.Fatalf("initial = %+v", init)
	}

	tm.updateTaskProgress(other, cloudsync.SyncProgress{Records: 1})
	select {
	case <-sub.Ready():
		t.Fatalf("another task's change reached a single-task feed: %+v", sub.Drain())
	case <-time.After(50 * time.Millisecond):
	}
}

// A task that finished before anyone subscribed is still answered — from
// history — so a page opened late shows the outcome instead of a 404.
func TestSubscribingToAFinishedTask(t *testing.T) {
	tm := newTestManager()
	task := &Task{ID: "t-1", Type: TaskTypeSync}
	_ = tm.addTask(task)
	tm.finishTask(task, nil, errors.New("boom"))

	sub := mustSubscribe(t, tm, "t-1")
	if init := sub.Initial(); len(init) != 1 || init[0].Status != TaskStatusFailed || init[0].Error != "boom" {
		t.Fatalf("initial = %+v", init)
	}

	if _, err := tm.Subscribe("nope"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("unknown task: err = %v, want ErrTaskNotFound", err)
	}
}

// Closing a feed unregisters it; later changes go nowhere near it.
func TestClosedFeedGetsNothing(t *testing.T) {
	tm := newTestManager()
	task := &Task{ID: "t-1", Type: TaskTypeSync}
	_ = tm.addTask(task)

	sub, _ := tm.Subscribe("")
	sub.Close()
	sub.Close() // idempotent
	tm.updateTaskProgress(task, cloudsync.SyncProgress{Records: 1})

	if len(tm.subs) != 0 {
		t.Errorf("%d feeds still registered after Close", len(tm.subs))
	}
	if got := sub.Drain(); len(got) != 0 {
		t.Errorf("closed feed received %+v", got)
	}
}

// GetTask hands out a copy: the handler serialises it after the lock is gone
// while the sync keeps writing the live task. Run with -race.
func TestGetTaskReturnsACopy(t *testing.T) {
	tm := newTestManager()
	task := &Task{ID: "t-1", Type: TaskTypeSync, Status: TaskStatusRunning}
	_ = tm.addTask(task)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			tm.updateTaskProgress(task, cloudsync.SyncProgress{Records: int64(i)})
		}
	}()
	for i := 0; i < 200; i++ {
		got, err := tm.GetTask("t-1")
		if err != nil {
			t.Fatal(err)
		}
		_ = got.Status
		_ = got.Progress
	}
	wg.Wait()

	got, _ := tm.GetTask("t-1")
	if got == task {
		t.Error("GetTask returned the live task")
	}
}
