package tasks

import (
	"fmt"
	"sync"
)

// TaskSubscription is one subscriber's view of task changes: Initial is the
// state at the moment of subscribing, and every change after that is announced
// on Ready and collected with Drain. Close it when done.
type TaskSubscription interface {
	Initial() []Task
	Ready() <-chan struct{}
	Drain() []Task
	Close()
}

// TaskFeed is the TaskManagerImpl's TaskSubscription.
//
// It keeps the latest snapshot per task, not a queue of events. A subscriber
// that falls behind — a slow browser, a stalled proxy — skips intermediate
// progress but never the state a task ends in, and the sync that publishes is
// never held up waiting for it. One event channel per subscriber could promise
// neither: a full buffer either blocks the sync or drops whichever event comes
// next, and on a feed of every task that can be another task's "completed".
type TaskFeed struct {
	// initial holds the tasks as they stood when the feed was opened: every
	// active task, or the one task asked for (from history if it has already
	// finished).
	initial []Task

	taskID string // empty: every task
	ready  chan struct{}

	mu      sync.Mutex
	pending map[string]Task
	order   []string // pending IDs in the order they first changed
	closed  bool

	unsubscribe func()
}

// Initial returns the tasks as they stood when the feed was opened.
func (f *TaskFeed) Initial() []Task {
	return f.initial
}

// Ready is signalled whenever there is something to Drain. It never closes.
func (f *TaskFeed) Ready() <-chan struct{} {
	return f.ready
}

// Drain hands over the latest snapshot of every task that changed since the
// last call, in the order the tasks first changed.
func (f *TaskFeed) Drain() []Task {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]Task, 0, len(f.order))
	for _, id := range f.order {
		out = append(out, f.pending[id])
	}
	f.pending = make(map[string]Task)
	f.order = f.order[:0]
	return out
}

// Close stops the feed. Safe to call more than once.
func (f *TaskFeed) Close() {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.closed = true
	f.mu.Unlock()
	f.unsubscribe()
}

// offer records a snapshot for the subscriber. It must not block: it runs on
// the goroutine that is changing the task, under the manager's lock.
func (f *TaskFeed) offer(t Task) {
	if f.taskID != "" && f.taskID != t.ID {
		return
	}

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	if _, seen := f.pending[t.ID]; !seen {
		f.order = append(f.order, t.ID)
	}
	f.pending[t.ID] = t
	f.mu.Unlock()

	select {
	case f.ready <- struct{}{}:
	default: // already signalled; the next Drain picks this up too
	}
}

// IsFinal reports whether a task has reached a state it will not leave.
// A task whose cancel was requested is not there yet: it is still running the
// pass in flight, and CancelRequested says so.
func (t Task) IsFinal() bool {
	switch t.Status {
	case TaskStatusCompleted, TaskStatusFailed, TaskStatusCancelled:
		return true
	}
	return false
}

// Subscribe opens a feed of task changes. taskID "" follows every task;
// otherwise only that one, and an unknown ID is ErrTaskNotFound.
//
// The initial snapshot and the registration happen under the same lock every
// change is published under, so no change can fall between the two: whatever
// is not in Initial arrives on the feed.
func (tm *TaskManagerImpl) Subscribe(taskID string) (TaskSubscription, error) {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	feed := &TaskFeed{
		taskID:  taskID,
		ready:   make(chan struct{}, 1),
		pending: make(map[string]Task),
	}

	if taskID == "" {
		for _, task := range tm.tasks {
			feed.initial = append(feed.initial, snapshot(task))
		}
	} else {
		task := tm.findTaskLocked(taskID)
		if task == nil {
			return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
		}
		feed.initial = []Task{snapshot(task)}
	}

	tm.subsMu.Lock()
	if tm.subs == nil {
		tm.subs = make(map[*TaskFeed]struct{})
	}
	tm.subs[feed] = struct{}{}
	tm.subsMu.Unlock()

	feed.unsubscribe = func() {
		tm.subsMu.Lock()
		delete(tm.subs, feed)
		tm.subsMu.Unlock()
	}
	return feed, nil
}

// publishLocked tells every subscriber about a task's new state. The caller
// holds tasksMutex for writing, which is what orders it against Subscribe.
func (tm *TaskManagerImpl) publishLocked(task *Task) {
	snap := snapshot(task)

	tm.subsMu.Lock()
	defer tm.subsMu.Unlock()
	for feed := range tm.subs {
		feed.offer(snap)
	}
}

// snapshot copies a task so it can be read after the lock is released. The
// copy is shallow on purpose: Progress is replaced wholesale on every update
// and Result is set once when the task finishes, so neither pointer's target
// is ever written again. The stop channel is left out; it is not the reader's.
func snapshot(task *Task) Task {
	t := *task
	t.stop = nil
	return t
}

// findTaskLocked looks a task up among the active ones and then the finished.
// The caller holds tasksMutex.
func (tm *TaskManagerImpl) findTaskLocked(taskID string) *Task {
	if task, exists := tm.tasks[taskID]; exists {
		return task
	}
	for _, task := range tm.taskHistory {
		if task.ID == taskID {
			return task
		}
	}
	return nil
}
