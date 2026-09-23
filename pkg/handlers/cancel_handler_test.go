package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"goscan/pkg/tasks"

	"github.com/gin-gonic/gin"
)

type cancelTaskMgr struct {
	tasks.TaskManager
	err error
	got string
}

func (m *cancelTaskMgr) CancelTask(taskID string) error {
	m.got = taskID
	return m.err
}

// The stop button needs to tell three answers apart: accepted (show
// "stopping", wait for the stream), gone, and too late / not stoppable.
func TestDeleteTaskStatusCodes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"stop requested", nil, http.StatusAccepted},
		{"unknown task", fmt.Errorf("%w: x", tasks.ErrTaskNotFound), http.StatusNotFound},
		{"already finished", fmt.Errorf("%w: done", tasks.ErrTaskNotCancellable), http.StatusConflict},
	}
	for _, tc := range cases {
		mgr := &cancelTaskMgr{err: tc.err}
		router := gin.New()
		router.DELETE("/tasks/:id", (&HandlerService{ctx: context.Background(), taskMgr: mgr}).DeleteTask)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/tasks/t-1", nil))
		if rec.Code != tc.want {
			t.Errorf("%s: status %d, want %d (%s)", tc.name, rec.Code, tc.want, rec.Body.String())
		}
		if mgr.got != "t-1" {
			t.Errorf("%s: cancelled %q", tc.name, mgr.got)
		}
	}
}
