package tasks

import (
	"encoding/json"
	"strings"
	"testing"

	"goscan/pkg/cloudsync"
)

// GetTask hands its caller the *Task itself and the HTTP handler serialises it
// after the manager's lock is gone. So every progress update has to publish a
// fresh value: editing the old one in place would let a reader watch its own
// snapshot change underneath it.
func TestProgressIsPublishedAsAnImmutableSnapshot(t *testing.T) {
	tm := &TaskManagerImpl{}
	task := &Task{ID: "t-1", Status: TaskStatusRunning}

	tm.updateTaskProgress(task, cloudsync.SyncProgress{Period: "2026-08", Done: 1, Total: 6})
	first := task.Progress
	tm.updateTaskProgress(task, cloudsync.SyncProgress{Period: "2026-09", Done: 2, Total: 6})

	if first == task.Progress {
		t.Fatal("progress was edited in place")
	}
	if first.Period != "2026-08" || first.Done != 1 {
		t.Errorf("the earlier snapshot changed: %+v", first)
	}
	if task.Progress.Period != "2026-09" || task.Progress.Done != 2 || task.Progress.Total != 6 {
		t.Errorf("latest progress = %+v", task.Progress)
	}
	if task.Progress.UpdatedAt.IsZero() {
		t.Error("progress has no timestamp; a caller cannot tell a stalled sync from a slow one")
	}
}

// The progress has to reach whoever polls /tasks/{id}, and a task that has not
// started must not carry an empty one.
func TestProgressIsSerialisedForTheAPI(t *testing.T) {
	task := &Task{ID: "t-1", Status: TaskStatusPending}
	if body, _ := json.Marshal(task); strings.Contains(string(body), "progress") {
		t.Errorf("a task without progress should omit the field: %s", body)
	}

	tm := &TaskManagerImpl{}
	tm.updateTaskProgress(task, cloudsync.SyncProgress{Period: "2026-09", Done: 2, Total: 6})
	body, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{`"progress"`, `"period":"2026-09"`, `"done":2`, `"total":6`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("task JSON missing %s: %s", want, body)
		}
	}
}
