package handlers

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"goscan/pkg/tasks"

	"github.com/gin-gonic/gin"
)

// fakeSub is a TaskSubscription the test feeds by hand.
type fakeSub struct {
	initial []tasks.Task
	ready   chan struct{}

	mu      sync.Mutex
	pending []tasks.Task
	closed  chan struct{}
	once    sync.Once
}

func newFakeSub(initial ...tasks.Task) *fakeSub {
	return &fakeSub{initial: initial, ready: make(chan struct{}, 1), closed: make(chan struct{})}
}

func (s *fakeSub) Initial() []tasks.Task  { return s.initial }
func (s *fakeSub) Ready() <-chan struct{} { return s.ready }
func (s *fakeSub) Close()                 { s.once.Do(func() { close(s.closed) }) }
func (s *fakeSub) Drain() []tasks.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.pending
	s.pending = nil
	return out
}

func (s *fakeSub) push(t tasks.Task) {
	s.mu.Lock()
	s.pending = append(s.pending, t)
	s.mu.Unlock()
	select {
	case s.ready <- struct{}{}:
	default:
	}
}

// fakeTaskMgr answers Subscribe and nothing else.
type fakeTaskMgr struct {
	tasks.TaskManager
	sub *fakeSub
	err error
	got string
}

func (m *fakeTaskMgr) Subscribe(taskID string) (tasks.TaskSubscription, error) {
	m.got = taskID
	if m.err != nil {
		return nil, m.err
	}
	return m.sub, nil
}

// serve runs the stream handlers behind a real HTTP server, whose WriteTimeout
// is short enough that a stream which failed to lift it would be cut off.
func serve(t *testing.T, mgr tasks.TaskManager) *httptest.Server {
	t.Helper()
	gin.SetMode(gin.TestMode)
	h := &HandlerService{ctx: context.Background(), taskMgr: mgr}
	router := gin.New()
	router.GET("/tasks/events", h.StreamTasks)
	router.GET("/tasks/:id/events", h.StreamTask)

	srv := httptest.NewUnstartedServer(router)
	srv.Config.WriteTimeout = 100 * time.Millisecond
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
}

type frame struct {
	event string
	data  string
}

// readFrames reads SSE frames until n have arrived, skipping comments and the
// retry hint, and fails the test if they do not come in time.
func readFrames(t *testing.T, r *bufio.Reader, n int) []frame {
	t.Helper()
	out := make(chan []frame, 1)
	go func() {
		var frames []frame
		var cur frame
		for len(frames) < n {
			line, err := r.ReadString('\n')
			if err != nil {
				break
			}
			line = strings.TrimRight(line, "\n")
			switch {
			case line == "":
				if cur.event != "" {
					frames = append(frames, cur)
				}
				cur = frame{}
			case strings.HasPrefix(line, "event: "):
				cur.event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				cur.data = strings.TrimPrefix(line, "data: ")
			}
		}
		out <- frames
	}()
	select {
	case frames := <-out:
		if len(frames) < n {
			t.Fatalf("stream ended after %d frames, want %d: %+v", len(frames), n, frames)
		}
		return frames
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %d frames", n)
		return nil
	}
}

func decodeTask(t *testing.T, f frame) tasks.Task {
	t.Helper()
	if f.event != "task" {
		t.Fatalf("frame %+v is not a task event", f)
	}
	var task tasks.Task
	if err := json.Unmarshal([]byte(f.data), &task); err != nil {
		t.Fatalf("bad task payload %q: %v", f.data, err)
	}
	return task
}

func open(t *testing.T, url string) (*http.Response, *bufio.Reader) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp, bufio.NewReader(resp.Body)
}

// A task's stream opens with its current state, pushes each change as it
// happens, and ends with `done` once the task has — well past the server's
// WriteTimeout, which the stream has to lift for itself.
func TestStreamTaskFollowsOneTaskToItsEnd(t *testing.T) {
	sub := newFakeSub(tasks.Task{ID: "t-1", Status: tasks.TaskStatusRunning})
	mgr := &fakeTaskMgr{sub: sub}
	srv := serve(t, mgr)

	resp, r := open(t, srv.URL+"/tasks/t-1/events")
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}
	if resp.Header.Get("X-Accel-Buffering") != "no" {
		t.Error("missing X-Accel-Buffering: no; nginx would hold the events back")
	}
	if mgr.got != "t-1" {
		t.Errorf("subscribed to %q, want t-1", mgr.got)
	}

	if first := decodeTask(t, readFrames(t, r, 1)[0]); first.Status != tasks.TaskStatusRunning {
		t.Fatalf("first event = %+v, want the current state", first)
	}

	time.Sleep(250 * time.Millisecond) // past the 100ms WriteTimeout
	sub.push(tasks.Task{ID: "t-1", Status: tasks.TaskStatusRunning, Progress: &tasks.TaskProgress{Period: "2026-08", Records: 500, RecordsTotal: 1000}})
	sub.push(tasks.Task{ID: "t-1", Status: tasks.TaskStatusCompleted})

	frames := readFrames(t, r, 3)
	if p := decodeTask(t, frames[0]).Progress; p == nil || p.Records != 500 || p.RecordsTotal != 1000 {
		t.Errorf("progress event = %+v", p)
	}
	if last := decodeTask(t, frames[1]); last.Status != tasks.TaskStatusCompleted {
		t.Errorf("final event = %+v", last)
	}
	if frames[2].event != "done" {
		t.Errorf("stream did not end with done: %+v", frames[2])
	}

	select {
	case <-sub.closed:
	case <-time.After(time.Second):
		t.Error("subscription not closed after the stream ended")
	}
}

// A task that has already finished gets its outcome and `done` straight away.
func TestStreamTaskForAFinishedTask(t *testing.T) {
	srv := serve(t, &fakeTaskMgr{sub: newFakeSub(tasks.Task{ID: "t-1", Status: tasks.TaskStatusFailed, Error: "boom"})})

	_, r := open(t, srv.URL+"/tasks/t-1/events")
	frames := readFrames(t, r, 2)
	if got := decodeTask(t, frames[0]); got.Status != tasks.TaskStatusFailed || got.Error != "boom" {
		t.Errorf("event = %+v", got)
	}
	if frames[1].event != "done" {
		t.Errorf("second frame = %+v, want done", frames[1])
	}
}

// An unknown task is a plain 404 before any stream starts, which EventSource
// treats as final instead of reconnecting forever.
func TestStreamTaskUnknown(t *testing.T) {
	srv := serve(t, &fakeTaskMgr{err: tasks.ErrTaskNotFound})

	resp, err := http.Get(srv.URL + "/tasks/nope/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// The all-tasks stream sends every active task, then any change, and keeps a
// quiet connection alive with comments. Hanging up closes the subscription.
func TestStreamTasksFollowsEveryTask(t *testing.T) {
	old := heartbeatInterval
	heartbeatInterval = 20 * time.Millisecond
	t.Cleanup(func() { heartbeatInterval = old })

	sub := newFakeSub(
		tasks.Task{ID: "a", Provider: "alicloud", Status: tasks.TaskStatusRunning},
		tasks.Task{ID: "b", Provider: "volcengine", Status: tasks.TaskStatusRunning},
	)
	mgr := &fakeTaskMgr{sub: sub}
	srv := serve(t, mgr)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/tasks/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)

	if mgr.got != "" {
		t.Errorf("subscribed to %q, want every task", mgr.got)
	}
	frames := readFrames(t, r, 2)
	if decodeTask(t, frames[0]).ID != "a" || decodeTask(t, frames[1]).ID != "b" {
		t.Errorf("initial events = %+v", frames)
	}

	// A finished task does not end this stream; it keeps going.
	sub.push(tasks.Task{ID: "a", Status: tasks.TaskStatusCompleted})
	sub.push(tasks.Task{ID: "b", Status: tasks.TaskStatusRunning, Progress: &tasks.TaskProgress{Records: 7}})
	frames = readFrames(t, r, 2)
	if decodeTask(t, frames[0]).Status != tasks.TaskStatusCompleted || decodeTask(t, frames[1]).Progress.Records != 7 {
		t.Errorf("change events = %+v", frames)
	}

	sawPing := make(chan bool, 1)
	go func() {
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				sawPing <- false
				return
			}
			if strings.HasPrefix(line, ": ping") {
				sawPing <- true
				return
			}
		}
	}()
	select {
	case ok := <-sawPing:
		if !ok {
			t.Fatal("stream ended instead of sending a heartbeat")
		}
	case <-time.After(time.Second):
		t.Fatal("no heartbeat on an idle stream")
	}

	cancel()
	select {
	case <-sub.closed:
	case <-time.After(time.Second):
		t.Error("subscription left open after the client hung up")
	}
}
