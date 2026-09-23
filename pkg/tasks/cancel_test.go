package tasks

import (
	"errors"
	"testing"
)

func runningSync(tm *TaskManagerImpl, id string) *Task {
	task := tm.createTask(&TaskRequest{ID: id, Type: TaskTypeSync, Provider: "alicloud"})
	task.Status = TaskStatusRunning
	_ = tm.addTask(task)
	return task
}

// Cancelling records the request and signals the sync; it does not end the
// task. The task keeps running the pass it is on, and says it is stopping.
func TestCancelRequestsAStopWithoutEndingTheTask(t *testing.T) {
	tm := newTestManager()
	task := runningSync(tm, "t-1")
	sub := mustSubscribe(t, tm, "t-1")

	if err := tm.CancelTask("t-1"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}

	select {
	case <-task.stop:
	default:
		t.Fatal("the sync was not signalled to stop")
	}
	got := waitReady(t, sub)
	if len(got) != 1 || !got[0].CancelRequested || got[0].Status != TaskStatusRunning || got[0].IsFinal() {
		t.Fatalf("after cancel: %+v — want still running, cancel_requested", got)
	}

	// Asking again while it stops is harmless (and must not close stop twice).
	if err := tm.CancelTask("t-1"); err != nil {
		t.Errorf("second CancelTask: %v", err)
	}
}

// When the sync comes back cancelled the task ends as cancelled, keeps its
// result, and says which passes never ran.
func TestACancelledSyncEndsAsCancelled(t *testing.T) {
	tm := newTestManager()
	task := runningSync(tm, "t-1")
	_ = tm.CancelTask("t-1")

	result := tm.convertSyncResult(&SyncResult{Success: true, RecordsProcessed: 200, Cancelled: true, NotRun: []string{"2026-08 monthly"}}, task)
	tm.finishTask(task, result, nil)

	got, _ := tm.GetTask("t-1")
	if got.Status != TaskStatusCancelled || !got.IsFinal() {
		t.Fatalf("status = %s, want cancelled and final", got.Status)
	}
	if got.Result == nil || got.Result.RecordsProcessed != 200 || len(got.Result.NotRun) != 1 {
		t.Errorf("result = %+v", got.Result)
	}
}

// A cancel that arrives during the last pass changes nothing: every pass ran,
// so the task completed.
func TestCancelDuringTheLastPassStillCompletes(t *testing.T) {
	tm := newTestManager()
	task := runningSync(tm, "t-1")
	_ = tm.CancelTask("t-1")

	tm.finishTask(task, tm.convertSyncResult(&SyncResult{Success: true}, task), nil)

	if got, _ := tm.GetTask("t-1"); got.Status != TaskStatusCompleted {
		t.Errorf("status = %s, want completed", got.Status)
	}
}

func TestWhatCannotBeCancelled(t *testing.T) {
	tm := newTestManager()

	if err := tm.CancelTask("nope"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("unknown task: %v, want ErrTaskNotFound", err)
	}

	done := runningSync(tm, "done")
	tm.finishTask(done, &TaskResult{}, nil)
	if err := tm.CancelTask("done"); !errors.Is(err, ErrTaskNotCancellable) {
		t.Errorf("finished task: %v, want ErrTaskNotCancellable", err)
	}

	note := tm.createTask(&TaskRequest{ID: "note", Type: TaskTypeNotification, Provider: "notification"})
	_ = tm.addTask(note)
	if err := tm.CancelTask("note"); !errors.Is(err, ErrTaskNotCancellable) {
		t.Errorf("notification: %v, want ErrTaskNotCancellable", err)
	}
}
