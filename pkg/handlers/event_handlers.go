package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"goscan/pkg/logger"
	_ "goscan/pkg/models"
	"goscan/pkg/tasks"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// heartbeatInterval is how often an idle stream sends a comment line. Ingress
// controllers and load balancers drop connections that stay silent (nginx's
// default proxy_read_timeout is 60s); a sync period can easily be quieter than
// that between two progress updates.
var heartbeatInterval = 15 * time.Second

// StreamTask streams one task's state as Server-Sent Events
// @Summary Stream a task's progress (Server-Sent Events)
// @Description Pushes the task's state every time it changes, as `event: task` with the same JSON GET /tasks/{id} returns. The first event is the current state; when the task completes, fails or is cancelled the last `task` event is followed by `event: done` and the stream closes.
// @Description Use it from a browser with `new EventSource('/tasks/{id}/events')` and call `close()` on `done` — otherwise EventSource reconnects by itself, gets the finished task once more and is closed again, every few seconds.
// @Tags Task Management
// @Produce text/event-stream
// @Param id path string true "Task ID returned by POST /sync"
// @Success 200 {object} tasks.Task "event: task — one per change"
// @Failure 404 {object} models.ErrorResponse "Task not found (not active and not among the last 100 finished)"
// @Router /tasks/{id}/events [get]
func (h *HandlerService) StreamTask(c *gin.Context) {
	h.streamTasks(c, c.Param("id"))
}

// StreamTasks streams every task's state as Server-Sent Events
// @Summary Stream all tasks' progress (Server-Sent Events)
// @Description Pushes `event: task` for every active task on connect, then one each time any task changes — manual syncs, scheduled ones and notifications alike. The stream stays open; a task whose status is completed, failed or cancelled has finished.
// @Tags Task Management
// @Produce text/event-stream
// @Success 200 {object} tasks.Task "event: task — one per change of any task"
// @Router /tasks/events [get]
func (h *HandlerService) StreamTasks(c *gin.Context) {
	h.streamTasks(c, "")
}

// streamTasks serves a task feed as an SSE stream. taskID "" follows every task
// and never ends by itself; a single task's stream ends once the task has.
//
// The stream carries state, not a log of events: a client that reconnects gets
// the current state first, so there is nothing to replay and no event IDs.
func (h *HandlerService) streamTasks(c *gin.Context, taskID string) {
	feed, err := h.taskMgr.Subscribe(taskID)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, tasks.ErrTaskNotFound) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": true, "message": err.Error()})
		return
	}
	defer feed.Close()

	// The server's WriteTimeout (30s) would cut every stream off mid-sync.
	// Lift it for this response only; a dead client is noticed through the
	// request context instead.
	if err := http.NewResponseController(c.Writer).SetWriteDeadline(time.Time{}); err != nil {
		logger.Warn("cannot lift write deadline for task stream, it will be cut off at the server's WriteTimeout",
			zap.Error(err))
	}

	header := c.Writer.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("Connection", "keep-alive")
	// nginx (and so ingress-nginx) buffers proxied responses by default, which
	// would hold every event back until the buffer fills.
	header.Set("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)

	w := &sseWriter{c: c}
	w.raw("retry: 3000\n\n")

	for _, task := range feed.Initial() {
		w.task(task)
		if taskID != "" && task.IsFinal() {
			w.done()
			return
		}
	}
	if !w.flush() {
		return
	}

	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-h.ctx.Done():
			// Shutting down: without this the server's graceful shutdown would
			// wait on every open stream until its own deadline.
			return
		case <-feed.Ready():
			for _, task := range feed.Drain() {
				w.task(task)
				if taskID != "" && task.IsFinal() {
					w.done()
					return
				}
			}
		case <-heartbeat.C:
			w.raw(": ping\n\n")
		}
		if !w.flush() {
			return
		}
	}
}

// sseWriter writes SSE frames and remembers the first write error, after which
// every call is a no-op and flush reports false.
type sseWriter struct {
	c   *gin.Context
	err error
}

func (w *sseWriter) raw(s string) {
	if w.err == nil {
		_, w.err = fmt.Fprint(w.c.Writer, s)
	}
}

func (w *sseWriter) task(task tasks.Task) {
	data, err := json.Marshal(task)
	if err != nil {
		logger.Error("cannot encode task for stream", zap.String("task_id", task.ID), zap.Error(err))
		return
	}
	w.raw("event: task\ndata: " + string(data) + "\n\n")
}

// done tells the client this task's stream is over, so it can close its
// EventSource instead of letting it reconnect.
func (w *sseWriter) done() {
	w.raw("event: done\ndata: {}\n\n")
	w.flush()
}

func (w *sseWriter) flush() bool {
	if w.err != nil {
		return false
	}
	w.c.Writer.Flush()
	return true
}
