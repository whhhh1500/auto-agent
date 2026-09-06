package server

import (
	"bufio"
	"bytes"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	writeSSE(writer, writer, "run/start", map[string]string{"status": "ok"})
	if recorder.flushes != 1 || !strings.Contains(recorder.body.String(), "event: run/start") {
		t.Fatalf("stream was not flushed through wrapper: flushes=%d body=%q", recorder.flushes, recorder.body.String())
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
