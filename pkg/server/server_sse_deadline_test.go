package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// ssePipeListener supplies one real net/http server connection. Its peer keeps
// the request open but never reads a response, so net.Pipe makes Flush block
// until ResponseController's write deadline expires.
type ssePipeListener struct {
	connection net.Conn
	closed     chan struct{}
	once       sync.Once
}

func newSSEPipeListener(connection net.Conn) *ssePipeListener {
	return &ssePipeListener{connection: connection, closed: make(chan struct{})}
}

func (listener *ssePipeListener) Accept() (net.Conn, error) {
	if listener.connection != nil {
		connection := listener.connection
		listener.connection = nil
		return connection, nil
	}
	<-listener.closed
	return nil, net.ErrClosed
}

func (listener *ssePipeListener) Close() error {
	listener.once.Do(func() { close(listener.closed) })
	return nil
}

func (*ssePipeListener) Addr() net.Addr { return ssePipeAddr("sse-pipe") }

type ssePipeAddr string

func (address ssePipeAddr) Network() string { return "pipe" }
func (address ssePipeAddr) String() string  { return string(address) }

type sseCloseSignalConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (connection *sseCloseSignalConn) Close() error {
	connection.once.Do(func() { close(connection.closed) })
	return connection.Conn.Close()
}

type sseCancelModel struct{}

func (sseCancelModel) Provider() string         { return "sse-test" }
func (sseCancelModel) ArtifactRevision() string { return "sse-test/v1" }
func (sseCancelModel) Stream(ctx context.Context, _ core.GenerateOptions, _ func(core.StreamChunk)) error {
	<-ctx.Done()
	return ctx.Err()
}

func newSSEDeadlineServer(t *testing.T) (*Server, *storage.SQLSessionStore, *core.Session) {
	t.Helper()
	_, sessions := openConfigFenceStore(t, "sse-deadline")
	global := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	product, _ := global.Child(core.ScopeRef{Kind: core.ScopeProduct, ID: "product"})
	tenant, _ := product.Child(core.ScopeRef{Kind: core.ScopeTenant, ID: "acme"})
	user, _ := tenant.Child(core.ScopeRef{Kind: core.ScopeUser, ID: "alice"})
	principal := core.Principal{SubjectID: "alice", TenantID: "acme", Scope: user, Grants: core.NewPermissionSet(core.PermRead)}
	profiles := core.NewAgentProfileRegistry()
	name := "Agent"
	model := core.ModelSelection{Provider: "sse-test", Model: "sse-test-1"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: product, ProfileID: "product.agent", Name: &name, Model: &model}); err != nil {
		t.Fatal(err)
	}
	runtime := &core.Runtime{
		Capabilities: core.NewCapabilityRegistry(), Profiles: profiles,
		Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) {
			return sseCancelModel{}, nil
		}),
	}
	sessionScope, _ := user.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "session-sse-deadline"})
	session, err := core.NewSession(core.SessionOptions{ID: "session-sse-deadline", ProfileID: "product.agent", Principal: principal, Scope: sessionScope})
	if err != nil {
		t.Fatal(err)
	}
	if err := sessions.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	server, err := New(Config{
		Runtime: runtime, Sessions: sessions, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Authenticator: AuthenticatorFunc(func(*http.Request) (core.Principal, error) { return principal, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	server.sseWriteTimeout = 40 * time.Millisecond
	return server, sessions, session
}

func TestSSEDeadlineCancelsSlowPipeReaderAndPersistsTerminal(t *testing.T) {
	api, sessions, session := newSSEDeadlineServer(t)
	client, serverConnection := net.Pipe()
	defer client.Close()
	serverClosed := make(chan struct{})
	listener := newSSEPipeListener(&sseCloseSignalConn{Conn: serverConnection, closed: serverClosed})
	handlerDone := make(chan struct{})
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.Handler().ServeHTTP(w, r)
		close(handlerDone)
	})}
	serveDone := make(chan error, 1)
	go func() { serveDone <- httpServer.Serve(listener) }()

	body := `{"message":"slow reader"}`
	request := fmt.Sprintf("POST /v1/sessions/%s/runs HTTP/1.1\r\nHost: sse-pipe\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", session.ID(), len(body), body)
	if err := client.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(client, request); err != nil {
		t.Fatalf("write request: %v", err)
	}
	_ = client.SetWriteDeadline(time.Time{})

	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("handler remained blocked after SSE transport deadline")
	}
	// This arrives only after net/http has run finishRequest. It proves the
	// retained expired deadline also bounds a final response flush while the
	// client still holds its pipe end open without reading.
	select {
	case <-serverClosed:
	case <-time.After(time.Second):
		t.Fatal("server connection remained open after final response flush")
	}

	stored, err := sessions.Load(context.Background(), session.ID())
	if err != nil {
		t.Fatal(err)
	}
	var runID string
	for _, event := range stored.Events() {
		if event.Type == core.EvRunStart {
			runID = event.RunID
			break
		}
	}
	if runID == "" {
		t.Fatalf("durable events missing run start: %#v", stored.Events())
	}
	if status, ok := stored.RunStatus(runID); !ok || status != core.RunCancelled {
		t.Fatalf("durable terminal status=%q exists=%t, want cancelled", status, ok)
	}

	if err := httpServer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("serve result=%v, want server closed", err)
	}
}
