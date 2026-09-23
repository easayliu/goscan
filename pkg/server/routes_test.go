package server

import (
	"testing"

	"goscan/pkg/handlers"

	"github.com/gin-gonic/gin"
)

// /tasks/events sits next to /tasks/:id. gin accepts a static segment beside a
// parameter one, but refuses conflicting routes by panicking at registration —
// so the whole API route table has to build, and the stream routes be in it.
func TestRoutesRegisterTheTaskStreams(t *testing.T) {
	gin.SetMode(gin.TestMode)
	s := &HTTPServer{router: gin.New(), handlerSvc: &handlers.HandlerService{}}
	s.setupAPIRoutes()

	want := map[string]bool{
		"GET /tasks/events":     false,
		"GET /tasks/:id":        false,
		"GET /tasks/:id/events": false,
	}
	for _, r := range s.router.Routes() {
		key := r.Method + " " + r.Path
		if _, ok := want[key]; ok {
			want[key] = true
		}
	}
	for route, found := range want {
		if !found {
			t.Errorf("route %s not registered", route)
		}
	}
}
