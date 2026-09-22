package tasks

import (
	"errors"
	"strings"
	"testing"
)

func newTestManager() *TaskManagerImpl {
	return &TaskManagerImpl{tasks: make(map[string]*Task), maxTasks: 10}
}

// Two syncs of the same cloud at once pull and write the same periods twice.
// It is easy to cause: triggering a sync from the UI while the cron job runs,
// or double-clicking the button.
func TestAddTaskRefusesSecondRunOfSameProvider(t *testing.T) {
	tm := newTestManager()

	first := &Task{ID: "first", Type: TaskTypeSync, Provider: "volcengine", Status: TaskStatusRunning}
	if err := tm.addTask(first); err != nil {
		t.Fatalf("first task rejected: %v", err)
	}

	second := &Task{ID: "second", Type: TaskTypeSync, Provider: "volcengine", Status: TaskStatusPending}
	err := tm.addTask(second)
	if !errors.Is(err, ErrTaskAlreadyRunning) {
		t.Fatalf("second task error = %v, want ErrTaskAlreadyRunning", err)
	}
	if _, registered := tm.tasks["second"]; registered {
		t.Error("the refused task was registered anyway")
	}
	// The caller has to be able to tell which run is in the way.
	if !strings.Contains(err.Error(), "first") {
		t.Errorf("error does not name the task in flight: %v", err)
	}
}

// A task that has not started yet still blocks: it is queued to run.
func TestAddTaskRefusesWhilePending(t *testing.T) {
	tm := newTestManager()
	if err := tm.addTask(&Task{ID: "queued", Type: TaskTypeSync, Provider: "alicloud", Status: TaskStatusPending}); err != nil {
		t.Fatalf("first task rejected: %v", err)
	}
	if err := tm.addTask(&Task{ID: "next", Type: TaskTypeSync, Provider: "alicloud", Status: TaskStatusPending}); !errors.Is(err, ErrTaskAlreadyRunning) {
		t.Fatalf("error = %v, want ErrTaskAlreadyRunning", err)
	}
}

// The guard is per provider and per task type: syncing both clouds at once is
// normal, and the daily WeChat report must not be held up by a running sync.
func TestAddTaskAllowsOtherProvidersAndTypes(t *testing.T) {
	tm := newTestManager()
	if err := tm.addTask(&Task{ID: "volc", Type: TaskTypeSync, Provider: "volcengine", Status: TaskStatusRunning}); err != nil {
		t.Fatalf("volcengine sync rejected: %v", err)
	}
	if err := tm.addTask(&Task{ID: "ali", Type: TaskTypeSync, Provider: "alicloud", Status: TaskStatusRunning}); err != nil {
		t.Errorf("a different provider was refused: %v", err)
	}
	if err := tm.addTask(&Task{ID: "report", Type: TaskTypeNotification, Provider: "volcengine", Status: TaskStatusRunning}); err != nil {
		t.Errorf("a different task type was refused: %v", err)
	}
}

// Once a run is over the provider is free again. finishTask drops the task from
// the active map, which is what makes the next sync possible.
func TestAddTaskAllowsAnotherRunAfterTheFirstEnds(t *testing.T) {
	tm := newTestManager()
	if err := tm.addTask(&Task{ID: "done", Type: TaskTypeSync, Provider: "volcengine", Status: TaskStatusRunning}); err != nil {
		t.Fatalf("first task rejected: %v", err)
	}
	delete(tm.tasks, "done")

	if err := tm.addTask(&Task{ID: "again", Type: TaskTypeSync, Provider: "volcengine", Status: TaskStatusPending}); err != nil {
		t.Errorf("the provider stayed blocked after its task ended: %v", err)
	}
}
