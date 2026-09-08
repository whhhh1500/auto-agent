package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type flushRecorder struct {
	header  http.Header
	body    bytes.Buffer
	status  int
	flushes int
}

func (r *flushRecorder) Header() http.Header               { return r.header }
func (r *flushRecorder) Write(payload []byte) (int, error) { return r.body.Write(payload) }
func (r *flushRecorder) WriteHeader(status int)            { r.status = status }
func (r *flushRecorder) Flush()                            { r.flushes++ }

func TestStatusWriterPreservesFlush(t *testing.T) {
	recorder := &flushRecorder{header: make(http.Header)}
	writer := &statusWriter{ResponseWriter: recorder, status: http.StatusOK}
	if err := writeSSE(writer, "run/start", map[string]string{"status": "ok"}); err != nil {
		t.Fatalf("write SSE through no-deadline wrapper: %v", err)
	}
	if recorder.flushes != 1 || !strings.Contains(recorder.body.String(), "event: run/start") {
		t.Fatalf("stream was not flushed through wrapper: flushes=%d body=%q", recorder.flushes, recorder.body.String())
	}
}

type deadlineFailWriter struct {
	header    http.Header
	writes    int
	deadlines []time.Time
	writeErr  error
	flushErr  error
}

func (w *deadlineFailWriter) Header() http.Header { return w.header }
func (w *deadlineFailWriter) Write(payload []byte) (int, error) {
	w.writes++
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return len(payload), nil
}
func (*deadlineFailWriter) WriteHeader(int) {}
func (w *deadlineFailWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return nil
}
func (w *deadlineFailWriter) FlushError() error { return w.flushErr }

func TestSSEStreamStopsAfterFirstTransportFailure(t *testing.T) {
	for _, test := range []struct {
		name     string
		writeErr error
		flushErr error
	}{
		{name: "write", writeErr: context.DeadlineExceeded},
		{name: "flush", flushErr: context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := &deadlineFailWriter{header: make(http.Header), writeErr: test.writeErr, flushErr: test.flushErr}
			stream := newSSEStream(&statusWriter{ResponseWriter: recorder, status: http.StatusOK}, 25*time.Millisecond)
			if err := stream.Write("run/start", map[string]string{"status": "ok"}); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("first write error=%v, want deadline exceeded", err)
			}
			if err := stream.Write("run/end", map[string]string{"status": "failed"}); err != nil {
				t.Fatalf("suppressed write error=%v", err)
			}
			if recorder.writes != 1 {
				t.Fatalf("writes=%d, want one failed transport attempt", recorder.writes)
			}
			if len(recorder.deadlines) != 1 || recorder.deadlines[0].IsZero() {
				t.Fatalf("deadline lifecycle=%v, want one retained failure deadline", recorder.deadlines)
			}
		})
	}
}

func TestSSEStreamClearsDeadlineAfterSuccessfulWrite(t *testing.T) {
	recorder := &deadlineFailWriter{header: make(http.Header)}
	if err := newSSEStream(recorder, 25*time.Millisecond).Write("run/end", map[string]string{"status": "ok"}); err != nil {
		t.Fatalf("write SSE: %v", err)
	}
	if len(recorder.deadlines) != 2 || recorder.deadlines[0].IsZero() || !recorder.deadlines[1].IsZero() {
		t.Fatalf("deadline lifecycle=%v, want set then clear", recorder.deadlines)
	}
}

func TestDecodeJSONRejectsTrailingValue(t *testing.T) {
	server := &Server{maxBody: 1 << 20}
	request := httptest.NewRequest(http.MethodPost, "/", bufio.NewReader(strings.NewReader(`{"message":"a"} {"message":"b"}`)))
	response := httptest.NewRecorder()
	var target struct {
		Message string `json:"message"`
	}
	if server.decodeJSON(response, request, &target) {
		t.Fatal("multiple JSON values were accepted")
	}
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", response.Code)
	}
	if got, want := response.Body.String(), "{\"error\":\"request body contains multiple JSON values\"}\n"; got != want {
		t.Fatalf("trailing-value envelope=%q want=%q", got, want)
	}
}

func TestDecodeJSONIgnoresOnlyUnknownTopLevelFields(t *testing.T) {
	server := &Server{maxBody: 1 << 20}
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"message":"ok","future_client_field":true}`))
	response := httptest.NewRecorder()
	var target struct {
		Message string `json:"message"`
	}
	if !server.decodeJSON(response, request, &target) {
		t.Fatalf("top-level forward-compatible field was rejected: %s", response.Body.String())
	}
	if target.Message != "ok" {
		t.Fatalf("decoded target = %#v", target)
	}

	request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"nested":{"known":"ok","unexpected":true}}`))
	response = httptest.NewRecorder()
	var nestedTarget struct {
		Nested struct {
			Known string `json:"known"`
		} `json:"nested"`
	}
	if server.decodeJSON(response, request, &nestedTarget) {
		t.Fatal("unknown nested field was accepted")
	}
	if response.Code != http.StatusBadRequest {
		t.Fatalf("nested field status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestPaginateSessionEvents(t *testing.T) {
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	user, _ := global.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "alice"})
	scope, _ := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-a"})
	principal := core.Principal{SubjectID: "alice", TenantID: "tenant", Scope: user, Grants: core.NewPermissionSet()}
	session, err := core.NewSession(core.SessionOptions{ID: "session-a", ProfileID: "test.agent", Principal: principal, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 5; index++ {
		if _, err := session.Append("run-a", core.EvUserMessage, core.UserMessageData{Text: string(rune('a' + index))}); err != nil {
			t.Fatal(err)
		}
	}
	events, more := paginateSessionEvents(session, 1, 2)
	if !more || len(events) != 2 || events[0].Seq != 2 || events[1].Seq != 3 {
		t.Fatalf("unexpected page: %#v more=%t", events, more)
	}
}

func TestEffectiveRunStatusPreservesLimited(t *testing.T) {
	status := effectiveRunStatus(core.TurnResult{Status: core.RunLimited}, nil)
	if status != core.RunLimited {
		t.Fatalf("limited run was recorded as %q", status)
	}
}
