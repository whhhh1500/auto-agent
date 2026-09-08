package sandboxexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/execution/sandbox"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type runtimeExecProvider struct {
	mu                    sync.Mutex
	mode                  string
	starts, execs, closes int
	events                []string
	compositions          []string
	mounts                []sandbox.Mount
	sessionDirsPresent    bool
	startErr              error
	cancel                context.CancelFunc
	outerDone             <-chan struct{}
}

func (p *runtimeExecProvider) ID() sandbox.ProviderID { return "local-ephemeral" }
func (p *runtimeExecProvider) Probe(context.Context) sandbox.AssuranceReport {
	return sandbox.AssuranceReport{Available: true, Actual: sandbox.Assurance{Level: sandbox.AssuranceProcess, SharedKernel: true}, Network: sandbox.NetworkDisabled, SupportedNetworks: []sandbox.NetworkPolicy{sandbox.NetworkDisabled}, LimitsEnforced: true, MountsEnforced: true}
}
func (p *runtimeExecProvider) Start(_ context.Context, spec sandbox.SessionSpec) (sandbox.Session, error) {
	p.mu.Lock()
	p.starts++
	p.mounts = append([]sandbox.Mount(nil), spec.Mounts...)
	p.mu.Unlock()
	if p.startErr != nil {
		return nil, p.startErr
	}
	if p.mode == "deadline-start" && p.outerDone != nil {
		<-p.outerDone
		return nil, errors.New("provider start deadline")
	}
	if p.mode == "cancel-start" && p.cancel != nil {
		p.cancel()
		return nil, errors.New("provider start failure")
	}
	return &runtimeExecSession{provider: p}, nil
}

// rootOwnershipExecProvider simulates a Windows-style Start that may adopt
// the root before its session can be returned. The boolean is per invocation,
// not a provider-wide static capability.
type rootOwnershipExecProvider struct {
	*runtimeExecProvider
	owned bool
}

func (p *rootOwnershipExecProvider) StartWithRootOwnership(ctx context.Context, spec sandbox.SessionSpec) (sandbox.Session, bool, error) {
	session, err := p.runtimeExecProvider.Start(ctx, spec)
	return session, p.owned, err
}

type runtimeExecSession struct{ provider *runtimeExecProvider }

func (s *runtimeExecSession) Assurance() sandbox.AssuranceReport {
	return sandbox.AssuranceReport{Available: true, Actual: sandbox.Assurance{Level: sandbox.AssuranceProcess, SharedKernel: true, NetworkIsolation: true}, Network: sandbox.NetworkDisabled, LimitsEnforced: true, MountsEnforced: true}
}
func (s *runtimeExecSession) Limits() sandbox.Limits {
	return sandbox.Limits{WallTime: time.Second, MaxCPUTime: time.Second, MaxMemoryBytes: 1 << 20, MaxOutputBytes: 1 << 20}
}
func (s *runtimeExecSession) Run(context.Context, sandbox.Command) (sandbox.ArtifactSet, error) {
	return sandbox.ArtifactSet{Items: []sandbox.Artifact{}}, nil
}
func (s *runtimeExecSession) Close(context.Context) error {
	s.provider.mu.Lock()
	s.provider.closes++
	s.provider.events = append(s.provider.events, "close")
	s.provider.mu.Unlock()
	if s.provider.mode == "deadline-close" && s.provider.outerDone != nil {
		<-s.provider.outerDone
		return errors.New("provider close deadline")
	}
	if s.provider.mode == "cancel-close" && s.provider.cancel != nil {
		s.provider.cancel()
		return errors.New("provider close failure")
	}
	if s.provider.mode == "close" {
		return errors.New("close secret")
	}
	return nil
}
func (s *runtimeExecSession) Exec(_ context.Context, request sandbox.ExecRequest) (sandbox.ExecResult, error) {
	s.provider.mu.Lock()
	s.provider.execs++
	s.provider.events = append(s.provider.events, "exec")
	s.provider.compositions = append(s.provider.compositions, request.Lease.CompositionRev)
	mode := s.provider.mode
	mounts := append([]sandbox.Mount(nil), s.provider.mounts...)
	if len(s.provider.mounts) == 2 {
		root := filepath.Dir(s.provider.mounts[0].Source)
		s.provider.sessionDirsPresent = true
		for _, name := range []string{"home", "tmp", "cache"} {
			if _, err := os.Stat(filepath.Join(root, name)); err != nil {
				s.provider.sessionDirsPresent = false
			}
		}
	}
	s.provider.mu.Unlock()
	d := sha256.Sum256(nil)
	r := sandbox.ExecResult{Stdout: sandbox.Output{Digest: hex.EncodeToString(d[:])}, Stderr: sandbox.Output{Digest: hex.EncodeToString(d[:])}, Artifacts: sandbox.ArtifactSet{Items: []sandbox.Artifact{}}}
	switch mode {
	case "deadline-exec":
		if s.provider.outerDone != nil {
			<-s.provider.outerDone
		}
		return r, errors.New("provider exec deadline")
	case "cancel-exec":
		if s.provider.cancel != nil {
			s.provider.cancel()
		}
		return r, errors.New("provider exec failure")
	case "publish-context", "deadline-publish":
		content := []byte("published")
		if len(mounts) != 2 || os.WriteFile(filepath.Join(mounts[1].Source, "out.txt"), content, 0o600) != nil {
			return r, errors.New("artifact setup failure")
		}
		digest := sha256.Sum256(content)
		r.Artifacts = sandbox.ArtifactSet{Items: []sandbox.Artifact{{Key: "out.txt", Digest: hex.EncodeToString(digest[:]), Size: int64(len(content)), Verified: true}}}
	case "nonzero":
		r.ExitCode = 7
		return r, sandbox.ErrExecFailed
	case "timeout":
		r.TimedOut = true
		return r, sandbox.ErrExecTimeout
	case "output":
		return r, sandbox.ErrExecOutputLimit
	}
	return r, nil
}

type finalizingExecProvider struct {
	*runtimeExecProvider
	finalizeErrs []error
}

func (p *finalizingExecProvider) Start(ctx context.Context, spec sandbox.SessionSpec) (sandbox.Session, error) {
	raw, err := p.runtimeExecProvider.Start(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &finalizingExecSession{runtimeExecSession: raw.(*runtimeExecSession), provider: p}, nil
}

type finalizingExecSession struct {
	*runtimeExecSession
	provider *finalizingExecProvider
}

func (s *finalizingExecSession) FinalizePublication(_ context.Context, outcome sandbox.PublicationOutcome) error {
	s.provider.mu.Lock()
	defer s.provider.mu.Unlock()
	s.provider.events = append(s.provider.events, "finalize:"+string(outcome))
	if len(s.provider.finalizeErrs) > 0 {
		err := s.provider.finalizeErrs[0]
		s.provider.finalizeErrs = s.provider.finalizeErrs[1:]
		return err
	}
	return nil
}

type agentToolModel struct {
	calls int
	mu    sync.Mutex
}

func (m *agentToolModel) Provider() string { return "test" }
func (m *agentToolModel) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.mu.Lock()
	m.calls++
	n := m.calls
	m.mu.Unlock()
	if n == 1 {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCalls: []core.ToolCall{{ID: "call-1", Name: "sandbox.exec", Args: map[string]any{"argv": []any{"echo", "ok"}}}}})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "done"})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}

type testJournal struct {
	mu      sync.Mutex
	records map[string]core.ToolInvocationRecord
}

func (j *testJournal) BeginToolInvocation(_ context.Context, inv core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.records == nil {
		j.records = map[string]core.ToolInvocationRecord{}
	}
	k := inv.SessionID + "/" + inv.RunID + "/" + inv.CallID
	if r, ok := j.records[k]; ok {
		if r.State == core.ToolInvocationCompleted {
			return r, core.ToolInvocationReplay, nil
		}
		return r, core.ToolInvocationExecuteRetry, nil
	}
	r := core.ToolInvocationRecord{ToolInvocation: inv, State: core.ToolInvocationStarted}
	j.records[k] = r
	return r, core.ToolInvocationExecuteNew, nil
}
func (j *testJournal) CompleteToolInvocation(_ context.Context, inv core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	k := inv.SessionID + "/" + inv.RunID + "/" + inv.CallID
	r := j.records[k]
	r.State = core.ToolInvocationCompleted
	r.Result = &result
	j.records[k] = r
	return r, nil
}
func (j *testJournal) MarkToolInvocationUncertain(context.Context, core.ToolInvocation, string) error {
	return nil
}

type testObjectStore struct {
	puts         []string
	deletes      []string
	bodies       map[string][]byte
	failAfter    int
	precondition bool
	cancel       context.CancelFunc
	wait         bool
}

func (s *testObjectStore) Get(context.Context, string) ([]byte, string, error) {
	return nil, "", storage.ErrObjectNotFound
}
func (s *testObjectStore) Put(_ context.Context, key string, data []byte, _ storage.PutOptions) (string, error) {
	s.puts = append(s.puts, key)
	if s.bodies == nil {
		s.bodies = make(map[string][]byte)
	}
	s.bodies[key] = append([]byte(nil), data...)
	return hex.EncodeToString(data), nil
}
func (s *testObjectStore) Delete(_ context.Context, key string) error {
	s.deletes = append(s.deletes, key)
	delete(s.bodies, key)
	return nil
}
func (s *testObjectStore) List(context.Context, string, string, int) ([]storage.ObjectItem, error) {
	return nil, nil
}
func (s *testObjectStore) PutStream(ctx context.Context, key string, body io.Reader, _ int64, _ storage.PutOptions) (string, error) {
	if s.wait {
		<-ctx.Done()
		return "", ctx.Err()
	}
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	if s.precondition {
		return "", storage.ErrPreconditionFailed
	}
	if s.failAfter >= 0 && len(s.puts) >= s.failAfter {
		return "", errors.New("injected store failure")
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	s.puts = append(s.puts, key)
	if s.bodies == nil {
		s.bodies = make(map[string][]byte)
	}
	s.bodies[key] = data
	return key, nil
}

// putOnlyTestObjectStore deliberately omits StreamingObjectPutter so tests
// exercise the bounded byte-slice compatibility path.
type putOnlyTestObjectStore struct {
	puts    []string
	deletes []string
	bodies  map[string][]byte
}

func (s *putOnlyTestObjectStore) Get(context.Context, string) ([]byte, string, error) {
	return nil, "", storage.ErrObjectNotFound
}
func (s *putOnlyTestObjectStore) Put(_ context.Context, key string, data []byte, _ storage.PutOptions) (string, error) {
	s.puts = append(s.puts, key)
	if s.bodies == nil {
		s.bodies = make(map[string][]byte)
	}
	s.bodies[key] = append([]byte(nil), data...)
	return hex.EncodeToString(data), nil
}
func (s *putOnlyTestObjectStore) Delete(_ context.Context, key string) error {
	s.deletes = append(s.deletes, key)
	delete(s.bodies, key)
	return nil
}
func (s *putOnlyTestObjectStore) List(context.Context, string, string, int) ([]storage.ObjectItem, error) {
	return nil, nil
}

func TestManifestDeclaresBoundedArgvAndSandboxPolicy(t *testing.T) {
	manifest := (&Capability{}).Manifest()
	if manifest.Tool == nil || manifest.MaxOutputBytes != int(sandbox.DefaultExecCombinedBytes) || manifest.TimeoutMs == 0 {
		t.Fatalf("manifest missing execution bounds: %#v", manifest)
	}
	if manifest.Execution == nil || manifest.Execution.Sandbox.Mode != core.SandboxWorkspaceWrite || !manifest.Execution.Writes || len(manifest.RequiredPermissions) != 1 || manifest.RequiredPermissions[0] != core.PermWrite {
		t.Fatalf("manifest sandbox policy=%#v", manifest.Execution)
	}
}

func TestPublishArtifactsNestedDigestAndKeyIsolation(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	content := []byte("artifact")
	if err := os.WriteFile(filepath.Join(root, "nested", "out.txt"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	store := &testObjectStore{failAfter: -1}
	refs, err := publishArtifacts(context.Background(), store, root, "tenant-a", "run-a", "call-a", sandbox.ArtifactSet{Items: []sandbox.Artifact{{Key: "nested/out.txt", Digest: hex.EncodeToString(digest[:]), Size: int64(len(content)), Verified: true}}})
	if err != nil || len(refs) != 1 {
		t.Fatalf("publish refs=%#v err=%v", refs, err)
	}
	if refs[0].Key != "tenants/tenant-a/artifacts/sandbox/run-a/call-a/nested/out.txt" || refs[0].Digest != hex.EncodeToString(digest[:]) {
		t.Fatalf("ref=%#v", refs[0])
	}
}

func TestPublishArtifactsRejectsSymlinkAndExtraAndRollsBack(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "one"), []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "two"), []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := &testObjectStore{failAfter: 1}
	one := sha256.Sum256([]byte("one"))
	two := sha256.Sum256([]byte("two"))
	_, err := publishArtifacts(context.Background(), store, root, "t", "r", "c", sandbox.ArtifactSet{Items: []sandbox.Artifact{{Key: "one", Digest: hex.EncodeToString(one[:]), Size: 3, Verified: true}, {Key: "two", Digest: hex.EncodeToString(two[:]), Size: 3, Verified: true}}})
	if err == nil || len(store.deletes) != 1 {
		t.Fatalf("partial publish err=%v deletes=%v", err, store.deletes)
	}
	if err := os.Symlink(filepath.Join(root, "one"), filepath.Join(root, "link")); err == nil {
		_, err = publishArtifacts(context.Background(), store, root, "t", "r", "c", sandbox.ArtifactSet{})
		if !errors.Is(err, sandbox.ErrArtifactUnverified) {
			t.Fatalf("symlink err=%v", err)
		}
	}
}

func TestPublishArtifactsRollsBackOnVerificationAfterPriorPut(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "one"), []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "two"), []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	one := sha256.Sum256([]byte("one"))
	store := &testObjectStore{failAfter: -1}
	_, err := publishArtifacts(context.Background(), store, root, "t", "r", "c", sandbox.ArtifactSet{Items: []sandbox.Artifact{
		{Key: "one", Digest: hex.EncodeToString(one[:]), Size: 3, Verified: true},
		{Key: "two", Digest: "wrong", Size: 3, Verified: true},
	}})
	if err == nil || len(store.puts) != 1 || len(store.deletes) != 1 {
		t.Fatalf("verification rollback err=%v puts=%v deletes=%v", err, store.puts, store.deletes)
	}
}

func TestPublishArtifactsRollsBackOnArtifactMutationRaces(t *testing.T) {
	for _, test := range []struct {
		name       string
		hooks      func(t *testing.T, path string, original []byte, hookErr *error) artifactPublishHooks
		wantUpload bool
	}{
		{
			name: "after_preflight_digest",
			hooks: func(_ *testing.T, path string, original []byte, hookErr *error) artifactPublishHooks {
				return artifactPublishHooks{afterPreflightDigest: func(string) {
					*hookErr = os.WriteFile(path, []byte(strings.Repeat("b", len(original))), 0o600)
				}}
			},
		},
		{
			name:       "during_stream",
			wantUpload: true,
			hooks: func(_ *testing.T, path string, original []byte, hookErr *error) artifactPublishHooks {
				return artifactPublishHooks{afterFirstRead: func(got string) {
					if got != path {
						*hookErr = errors.New("stream hook received unexpected path")
						return
					}
					*hookErr = os.WriteFile(path, []byte(strings.Repeat("c", len(original))), 0o600)
				}}
			},
		},
		{
			name: "path_replacement",
			hooks: func(_ *testing.T, path string, original []byte, hookErr *error) artifactPublishHooks {
				return artifactPublishHooks{afterPreflightDigest: func(_ string) {
					replacement := path + ".replacement"
					if err := os.WriteFile(replacement, original, 0o600); err != nil {
						*hookErr = err
						return
					}
					*hookErr = os.Rename(replacement, path)
				}}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			original := []byte(strings.Repeat("a", 128<<10))
			path := filepath.Join(root, "output.bin")
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(original)
			store := &testObjectStore{failAfter: -1}
			var hookErr error
			_, err := publishArtifactsWithHooks(context.Background(), store, root, "tenant", "run", "call", sandbox.ArtifactSet{Items: []sandbox.Artifact{{Key: "output.bin", Digest: hex.EncodeToString(digest[:]), Size: int64(len(original)), Verified: true}}}, test.hooks(t, path, original, &hookErr))
			if hookErr != nil {
				t.Fatalf("mutation hook failed: %v", hookErr)
			}
			if !errors.Is(err, sandbox.ErrArtifactUnverified) {
				t.Fatalf("publish err=%v, want ErrArtifactUnverified", err)
			}
			if test.wantUpload {
				if len(store.puts) != 1 || len(store.deletes) != 1 || store.puts[0] != store.deletes[0] {
					t.Fatalf("successful mismatched upload must be removed: puts=%v deletes=%v", store.puts, store.deletes)
				}
			} else if len(store.puts) != 0 || len(store.deletes) != 0 {
				t.Fatalf("pre-upload path race must not publish: puts=%v deletes=%v", store.puts, store.deletes)
			}
			if len(store.bodies) != 0 {
				t.Fatalf("cleanup left published object: %#v", store.bodies)
			}
		})
	}
}

func TestPublishArtifactsNonStreamingPostReadMutationRollsBack(t *testing.T) {
	root := t.TempDir()
	original := []byte(strings.Repeat("x", 128<<10))
	path := filepath.Join(root, "output.bin")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(original)
	store := &putOnlyTestObjectStore{}
	var hookErr error
	_, err := publishArtifactsWithHooks(context.Background(), store, root, "tenant", "run", "call", sandbox.ArtifactSet{Items: []sandbox.Artifact{{Key: "output.bin", Digest: hex.EncodeToString(digest[:]), Size: int64(len(original)), Verified: true}}}, artifactPublishHooks{afterFirstRead: func(path string) {
		hookErr = os.WriteFile(path, original, 0o600)
	}})
	if hookErr != nil {
		t.Fatalf("mutate non-streaming artifact: %v", hookErr)
	}
	if !errors.Is(err, sandbox.ErrArtifactUnverified) {
		t.Fatalf("publish err=%v, want ErrArtifactUnverified", err)
	}
	if len(store.puts) != 1 || len(store.deletes) != 1 || store.puts[0] != store.deletes[0] || len(store.bodies) != 0 {
		t.Fatalf("non-streaming mutation must be rolled back: puts=%v deletes=%v bodies=%v", store.puts, store.deletes, store.bodies)
	}
}

func TestPublishArtifactsRejectsArtifactSetMutationAfterEnumeration(t *testing.T) {
	root := t.TempDir()
	content := []byte("approved")
	if err := os.WriteFile(filepath.Join(root, "output.txt"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	store := &testObjectStore{failAfter: -1}
	_, err := publishArtifactsWithHooks(context.Background(), store, root, "tenant", "run", "call", sandbox.ArtifactSet{Items: []sandbox.Artifact{{Key: "output.txt", Digest: hex.EncodeToString(digest[:]), Size: int64(len(content)), Verified: true}}}, artifactPublishHooks{beforeUploadRead: func(string) {
		if err := os.WriteFile(filepath.Join(root, "added.txt"), []byte("unverified"), 0o600); err != nil {
			t.Fatalf("add artifact after enumeration: %v", err)
		}
	}})
	if !errors.Is(err, sandbox.ErrArtifactUnverified) {
		t.Fatalf("publish err=%v, want ErrArtifactUnverified", err)
	}
	if len(store.puts) != 1 || len(store.deletes) != 1 || store.puts[0] != store.deletes[0] {
		t.Fatalf("artifact-set mutation must roll back: puts=%v deletes=%v", store.puts, store.deletes)
	}
}

func TestPublishArtifactsRejectsHardlinkedArtifact(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "output.txt")
	if err := os.WriteFile(path, []byte("artifact"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(path, filepath.Join(root, "alias.txt")); err != nil {
		t.Skipf("filesystem does not support hard links: %v", err)
	}
	store := &testObjectStore{failAfter: -1}
	_, err := publishArtifacts(context.Background(), store, root, "tenant", "run", "call", sandbox.ArtifactSet{})
	if !errors.Is(err, sandbox.ErrArtifactUnverified) {
		t.Fatalf("hardlink publish err=%v, want ErrArtifactUnverified", err)
	}
	if len(store.puts) != 0 {
		t.Fatalf("hardlinked artifact reached object store: %v", store.puts)
	}
}

func TestPublishArtifactsPreservesCreateOnlyPrecondition(t *testing.T) {
	root := t.TempDir()
	content := []byte("already exists")
	if err := os.WriteFile(filepath.Join(root, "output.txt"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	store := &testObjectStore{failAfter: -1, precondition: true}
	_, err := publishArtifacts(context.Background(), store, root, "tenant", "run", "call", sandbox.ArtifactSet{Items: []sandbox.Artifact{{Key: "output.txt", Digest: hex.EncodeToString(digest[:]), Size: int64(len(content)), Verified: true}}})
	if !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Fatalf("publish err=%v, want create-only precondition error", err)
	}
	if len(store.puts) != 0 || len(store.deletes) != 0 {
		t.Fatalf("precondition failure must not delete a pre-existing object: puts=%v deletes=%v", store.puts, store.deletes)
	}
}

func TestArtifactUploadReaderHashesOnlyActualBoundedBytes(t *testing.T) {
	reader := newArtifactUploadReader(context.Background(), strings.NewReader("abcd"), 3, nil)
	data, err := io.ReadAll(reader)
	if !errors.Is(err, sandbox.ErrArtifactUnverified) {
		t.Fatalf("oversized reader err=%v, want ErrArtifactUnverified", err)
	}
	if string(data) != "abc" || reader.Size() != 3 {
		t.Fatalf("reader exposed unbounded data=%q size=%d", data, reader.Size())
	}
	want := sha256.Sum256([]byte("abc"))
	if reader.Digest() != hex.EncodeToString(want[:]) {
		t.Fatalf("reader digest=%s want %s", reader.Digest(), hex.EncodeToString(want[:]))
	}
}

func TestArtifactUploadReaderPreservesCancellationIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader := newArtifactUploadReader(ctx, strings.NewReader("artifact"), sandbox.DefaultExecArtifactBytes, nil)
	if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("reader cancellation err=%v, want context.Canceled", err)
	}
}

func TestCapabilityRequiresAcceptedInvocation(t *testing.T) {
	capability := &Capability{}
	_, err := capability.Execute(context.Background(), core.CapabilityRequest{Args: map[string]any{"argv": []any{"echo"}}})
	if !errors.Is(err, core.ErrAcceptedInvocationRequired) {
		t.Fatalf("err=%v", err)
	}
}

func TestConfigNetworkSelectionDefaultsAndRejectsAssuranceMismatch(t *testing.T) {
	limits := sandbox.Limits{WallTime: time.Second, MaxCPUTime: time.Second, MaxMemoryBytes: 1 << 20, MaxOutputBytes: 1 << 20}
	registry, err := sandbox.NewRegistry(1, sandbox.Registration{
		Metadata: sandbox.Metadata{ID: "local-ephemeral", Version: "1", ImplementationRevision: "network-config-test", Assurance: sandbox.Assurance{Level: sandbox.AssuranceProcess, SharedKernel: true}},
		Provider: &runtimeExecProvider{},
	})
	if err != nil {
		t.Fatal(err)
	}
	base := func(network sandbox.NetworkPolicy, requestedIsolation bool) Config {
		return Config{Registry: registry, ProviderID: "local-ephemeral", Version: "1", Network: network, RequestedIsolation: requestedIsolation, Limits: limits, ArtifactPolicy: sandbox.ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 1 << 20}, WorkRoot: t.TempDir(), ObjectStore: &testObjectStore{failAfter: -1}}
	}
	defaultCapability, err := New(base("", false))
	if err != nil {
		t.Fatalf("compatibility config: %v", err)
	}
	if defaultCapability.config.Network != sandbox.NetworkDisabled || !defaultCapability.config.RequestedIsolation {
		t.Fatalf("compatibility network=%q isolation=%t", defaultCapability.config.Network, defaultCapability.config.RequestedIsolation)
	}
	weakCapability, err := New(base(sandbox.NetworkHost, false))
	if err != nil {
		t.Fatalf("host config: %v", err)
	}
	if weakCapability.config.Network != sandbox.NetworkHost || weakCapability.config.RequestedIsolation {
		t.Fatalf("host network=%q isolation=%t", weakCapability.config.Network, weakCapability.config.RequestedIsolation)
	}
	for _, test := range []struct {
		name     string
		network  sandbox.NetworkPolicy
		isolated bool
	}{
		{name: "host with isolation", network: sandbox.NetworkHost, isolated: true},
		{name: "disabled without isolation", network: sandbox.NetworkDisabled, isolated: false},
		{name: "isolated without isolation", network: sandbox.NetworkIsolated, isolated: false},
		{name: "unknown policy", network: sandbox.NetworkPolicy("unknown"), isolated: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(base(test.network, test.isolated)); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("New() = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func TestProviderErrorPreservesOuterContextIdentity(t *testing.T) {
	for _, test := range []struct {
		name string
		ctx  error
	}{
		{name: "canceled", ctx: context.Canceled},
		{name: "deadline", ctx: context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := contextWithErr{Context: context.Background(), err: test.ctx}
			if got := mapProviderError(ctx, errors.New("provider detail")); !errors.Is(got, test.ctx) {
				t.Fatalf("got=%v, want %v", got, test.ctx)
			}
			if got := mapProviderError(context.Background(), errors.New("provider detail")); !errors.Is(got, ErrUnavailable) {
				t.Fatalf("got=%v, want ErrUnavailable", got)
			}
		})
	}
}

func TestCapabilityStartOwnershipControlsFailedStartCleanup(t *testing.T) {
	limits := sandbox.Limits{WallTime: time.Second, MaxCPUTime: time.Second, MaxMemoryBytes: 1 << 20, MaxOutputBytes: 1 << 20}
	for _, test := range []struct {
		name      string
		provider  sandbox.Provider
		wantRoots int
	}{
		{
			name:      "ordinary start failure cleans adapter root",
			provider:  &runtimeExecProvider{startErr: errors.New("ordinary start failure")},
			wantRoots: 0,
		},
		{
			name:      "owned partial start preserves provider root",
			provider:  &rootOwnershipExecProvider{runtimeExecProvider: &runtimeExecProvider{startErr: errors.New("post-adoption start failure")}, owned: true},
			wantRoots: 1,
		},
		{
			name:      "owned successful non-finalizer preserves provider root",
			provider:  &rootOwnershipExecProvider{runtimeExecProvider: &runtimeExecProvider{}, owned: true},
			wantRoots: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			registry, err := sandbox.NewRegistry(1, sandbox.Registration{Metadata: sandbox.Metadata{ID: test.provider.ID(), Version: "1", ImplementationRevision: "ownership-test-v1", Assurance: sandbox.Assurance{Level: sandbox.AssuranceProcess, SharedKernel: true}}, Provider: test.provider})
			if err != nil {
				t.Fatal(err)
			}
			capability, err := New(Config{Registry: registry, ProviderID: test.provider.ID(), Version: "1", Limits: limits, ArtifactPolicy: sandbox.ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 1 << 20}, WorkRoot: root, ObjectStore: &testObjectStore{failAfter: -1}})
			if err != nil {
				t.Fatal(err)
			}
			if err := runSandboxCapabilityForOwnershipTest(t, capability); err != nil {
				t.Fatalf("run capability: %v", err)
			}
			entries, err := os.ReadDir(filepath.Join(root, "sessions"))
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != test.wantRoots {
				t.Fatalf("session root count=%d want=%d entries=%v", len(entries), test.wantRoots, entries)
			}
		})
	}
}

func runSandboxCapabilityForOwnershipTest(t *testing.T, capability *Capability) error {
	t.Helper()
	scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
	principal := core.Principal{SubjectID: "subject", TenantID: "tenant", Scope: scope, Grants: core.NewPermissionSet(core.PermWrite)}
	capabilities := core.NewCapabilityRegistry()
	if err := capabilities.Register(scope, capability); err != nil {
		return err
	}
	profiles := core.NewAgentProfileRegistry()
	selection := core.ModelSelection{Provider: "test", Model: "test"}
	if err := profiles.Bind(core.AgentProfileLayer{Scope: scope, ProfileID: "general", Model: &selection, AddCapabilities: []string{"sandbox.exec"}}); err != nil {
		return err
	}
	sessionScope, err := scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "ownership-session"})
	if err != nil {
		return err
	}
	session, err := core.NewSession(core.SessionOptions{ID: "ownership-session", ProfileID: "general", Principal: principal, Scope: sessionScope})
	if err != nil {
		return err
	}
	runtime := &core.Runtime{Capabilities: capabilities, Profiles: profiles, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return &agentToolModel{}, nil }), ToolJournal: &testJournal{}}
	_, err = runtime.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "ownership-run", Text: "run"}, nil)
	return err
}

func TestCapabilityExecutePreservesOuterContextAcrossLifecycle(t *testing.T) {
	for _, test := range []struct {
		mode string
		want error
	}{
		{mode: "cancel-start", want: context.Canceled},
		{mode: "cancel-exec", want: context.Canceled},
		{mode: "cancel-close", want: context.Canceled},
		{mode: "publish-context", want: context.Canceled},
		{mode: "deadline-start", want: context.DeadlineExceeded},
		{mode: "deadline-exec", want: context.DeadlineExceeded},
		{mode: "deadline-close", want: context.DeadlineExceeded},
		{mode: "deadline-publish", want: context.DeadlineExceeded},
	} {
		t.Run(test.mode, func(t *testing.T) {
			provider := &runtimeExecProvider{mode: test.mode}
			store := &testObjectStore{failAfter: -1}
			ctx, cancel := context.WithCancel(context.Background())
			if strings.HasPrefix(test.mode, "deadline-") {
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
			}
			provider.cancel = cancel
			provider.outerDone = ctx.Done()
			if test.mode == "publish-context" {
				store.cancel = cancel
			}
			if test.mode == "deadline-publish" {
				store.wait = true
			}

			registry, err := sandbox.NewRegistry(2, sandbox.Registration{Metadata: sandbox.Metadata{ID: provider.ID(), Version: "1", ImplementationRevision: "test-v1", Assurance: sandbox.Assurance{Level: sandbox.AssuranceProcess, SharedKernel: true}}, Provider: provider})
			if err != nil {
				t.Fatal(err)
			}
			limits := sandbox.Limits{WallTime: time.Second, MaxCPUTime: time.Second, MaxMemoryBytes: 1 << 20, MaxOutputBytes: 1 << 20}
			capability, err := New(Config{Registry: registry, ProviderID: provider.ID(), Version: "1", Limits: limits, ArtifactPolicy: sandbox.ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 1 << 20}, WorkRoot: t.TempDir(), ObjectStore: store})
			if err != nil {
				t.Fatal(err)
			}
			scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
			principal := core.Principal{SubjectID: "subject", TenantID: "tenant", Scope: scope, Grants: core.NewPermissionSet(core.PermWrite)}
			caps := core.NewCapabilityRegistry()
			if err := caps.Register(scope, capability); err != nil {
				t.Fatal(err)
			}
			profiles := core.NewAgentProfileRegistry()
			selection := core.ModelSelection{Provider: "test", Model: "test"}
			if err := profiles.Bind(core.AgentProfileLayer{Scope: scope, ProfileID: "general", Model: &selection, AddCapabilities: []string{"sandbox.exec"}}); err != nil {
				t.Fatal(err)
			}
			sessionScope, err := scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "context-session"})
			if err != nil {
				t.Fatal(err)
			}
			session, err := core.NewSession(core.SessionOptions{ID: "context-session", ProfileID: "general", Principal: principal, Scope: sessionScope})
			if err != nil {
				t.Fatal(err)
			}
			runtime := &core.Runtime{Capabilities: caps, Profiles: profiles, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return &agentToolModel{}, nil }), ToolJournal: &testJournal{}}
			_, err = runtime.RunTurn(ctx, principal, session, core.TurnInput{RunID: "context-run", Text: "run"}, nil)
			if !errors.Is(err, test.want) {
				t.Fatalf("err=%v, want %v", err, test.want)
			}
		})
	}
}

func TestCapabilityExecutePublicationFinalizerOwnsTerminalCleanup(t *testing.T) {
	for _, test := range []struct {
		name         string
		mode         string
		failAfter    int
		finalizeErrs []error
		wantEvents   []string
		cancel       bool
	}{
		{name: "published", mode: "success", failAfter: -1, wantEvents: []string{"exec", "finalize:published", "close"}},
		{name: "finalizer retry", mode: "success", failAfter: -1, finalizeErrs: []error{errors.New("first finalizer failure")}, wantEvents: []string{"exec", "finalize:published", "finalize:published", "close"}},
		{name: "publication failure", mode: "publish-context", failAfter: 0, wantEvents: []string{"exec", "finalize:failed", "close"}},
		{name: "canceled execution", mode: "cancel-exec", failAfter: -1, wantEvents: []string{"exec", "finalize:failed", "close"}, cancel: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := &runtimeExecProvider{mode: test.mode}
			provider := &finalizingExecProvider{runtimeExecProvider: base, finalizeErrs: append([]error(nil), test.finalizeErrs...)}
			store := &testObjectStore{failAfter: test.failAfter}
			root := t.TempDir()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if test.cancel {
				base.cancel = cancel
			}
			registry, err := sandbox.NewRegistry(1, sandbox.Registration{Metadata: sandbox.Metadata{ID: provider.ID(), Version: "1", ImplementationRevision: "test-v1", Assurance: sandbox.Assurance{Level: sandbox.AssuranceProcess, SharedKernel: true}}, Provider: provider})
			if err != nil {
				t.Fatal(err)
			}
			limits := sandbox.Limits{WallTime: time.Second, MaxCPUTime: time.Second, MaxMemoryBytes: 1 << 20, MaxOutputBytes: 1 << 20}
			capability, err := New(Config{Registry: registry, ProviderID: provider.ID(), Version: "1", Limits: limits, ArtifactPolicy: sandbox.ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 1 << 20}, WorkRoot: root, ObjectStore: store})
			if err != nil {
				t.Fatal(err)
			}
			scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
			principal := core.Principal{SubjectID: "subject", TenantID: "tenant", Scope: scope, Grants: core.NewPermissionSet(core.PermWrite)}
			caps := core.NewCapabilityRegistry()
			if err := caps.Register(scope, capability); err != nil {
				t.Fatal(err)
			}
			profiles := core.NewAgentProfileRegistry()
			selection := core.ModelSelection{Provider: "test", Model: "test"}
			if err := profiles.Bind(core.AgentProfileLayer{Scope: scope, ProfileID: "general", Model: &selection, AddCapabilities: []string{"sandbox.exec"}}); err != nil {
				t.Fatal(err)
			}
			sessionScope, err := scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "finalizer-session"})
			if err != nil {
				t.Fatal(err)
			}
			session, err := core.NewSession(core.SessionOptions{ID: "finalizer-session", ProfileID: "general", Principal: principal, Scope: sessionScope})
			if err != nil {
				t.Fatal(err)
			}
			runtime := &core.Runtime{Capabilities: caps, Profiles: profiles, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return &agentToolModel{}, nil }), ToolJournal: &testJournal{}}
			_, runErr := runtime.RunTurn(ctx, principal, session, core.TurnInput{RunID: "finalizer-run", Text: "run"}, nil)
			if test.name == "published" && runErr != nil {
				t.Fatalf("published run err=%v", runErr)
			}
			if test.cancel && !errors.Is(runErr, context.Canceled) {
				t.Fatalf("canceled run err=%v", runErr)
			}
			base.mu.Lock()
			events := append([]string(nil), base.events...)
			base.mu.Unlock()
			if strings.Join(events, ",") != strings.Join(test.wantEvents, ",") {
				t.Fatalf("lifecycle events=%v want=%v", events, test.wantEvents)
			}
			entries, err := os.ReadDir(filepath.Join(root, "sessions"))
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 {
				t.Fatalf("publication-aware provider root was generically deleted: %v", entries)
			}
		})
	}
}

type contextWithErr struct {
	context.Context
	err error
}

func (c contextWithErr) Err() error { return c.err }

func TestCapabilityExecuteThroughCoreGuardAndRegistry(t *testing.T) {
	for _, mode := range []string{"success", "nonzero", "timeout", "output", "close"} {
		t.Run(mode, func(t *testing.T) {
			provider := &runtimeExecProvider{mode: mode}
			registry, err := sandbox.NewRegistry(4, sandbox.Registration{Metadata: sandbox.Metadata{ID: provider.ID(), Version: "1", ImplementationRevision: "test-v1", Assurance: sandbox.Assurance{Level: sandbox.AssuranceProcess, SharedKernel: true}}, Provider: provider})
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			limits := sandbox.Limits{WallTime: time.Second, MaxCPUTime: time.Second, MaxMemoryBytes: 1 << 20, MaxOutputBytes: 1 << 20}
			capability, err := New(Config{Registry: registry, ProviderID: provider.ID(), Version: "1", Limits: limits, ArtifactPolicy: sandbox.ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 1 << 20}, WorkRoot: root, ObjectStore: &testObjectStore{failAfter: -1}})
			if err != nil {
				t.Fatal(err)
			}
			scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
			principal := core.Principal{SubjectID: "subject", TenantID: "tenant", Scope: scope, Grants: core.NewPermissionSet(core.PermWrite)}
			caps := core.NewCapabilityRegistry()
			if err := caps.Register(scope, capability); err != nil {
				t.Fatal(err)
			}
			profiles := core.NewAgentProfileRegistry()
			modelSelection := core.ModelSelection{Provider: "test", Model: "test"}
			if err := profiles.Bind(core.AgentProfileLayer{Scope: scope, ProfileID: "general", Model: &modelSelection, AddCapabilities: []string{"sandbox.exec"}}); err != nil {
				t.Fatal(err)
			}
			sessionScope, err := scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: "sandbox-session"})
			if err != nil {
				t.Fatal(err)
			}
			session, err := core.NewSession(core.SessionOptions{ID: "sandbox-session", ProfileID: "general", Principal: principal, Scope: sessionScope})
			if err != nil {
				t.Fatal(err)
			}
			model := &agentToolModel{}
			runtime := &core.Runtime{Capabilities: caps, Profiles: profiles, DiscloseTools: true, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return model, nil }), ToolJournal: &testJournal{}}
			result, err := runtime.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: "sandbox-run", Text: "run"}, nil)
			if err != nil {
				t.Fatalf("RunTurn err=%v result=%#v", err, result)
			}
			provider.mu.Lock()
			starts, execs, closes := provider.starts, provider.execs, provider.closes
			mounts := append([]sandbox.Mount(nil), provider.mounts...)
			sessionDirsPresent := provider.sessionDirsPresent
			provider.mu.Unlock()
			if starts != 1 || execs != 1 || closes != 1 {
				t.Fatalf("provider lifecycle starts=%d execs=%d closes=%d", starts, execs, closes)
			}
			if len(mounts) != 2 {
				t.Fatalf("mounts=%#v", mounts)
			}
			workspaceRoot := filepath.Dir(mounts[0].Source)
			if filepath.Dir(mounts[1].Source) != workspaceRoot {
				t.Fatalf("workspace and artifacts are not siblings in one session root: %#v", mounts)
			}
			if !sessionDirsPresent {
				t.Fatalf("session directories were not present during execution")
			}
			sessionEntries, err := os.ReadDir(filepath.Join(root, "sessions"))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "close" && len(sessionEntries) != 1 {
				t.Fatalf("legacy close failure did not preserve its session root: %v", sessionEntries)
			}
			if mode != "close" && len(sessionEntries) != 0 {
				t.Fatalf("session root was not cleaned up: %v", sessionEntries)
			}
			provider.mu.Lock()
			if len(provider.compositions) != 1 || provider.compositions[0] == "" {
				provider.mu.Unlock()
				t.Fatalf("composition revision was not propagated: %#v", provider.compositions)
			}
			provider.mu.Unlock()
			toolResult, exists, readErr := session.ToolResult("sandbox-run", "call-1")
			if readErr != nil || !exists {
				t.Fatalf("tool result=%#v exists=%v err=%v", toolResult, exists, readErr)
			}
			if mode != "close" {
				var payload map[string]any
				if json.Unmarshal([]byte(toolResult.Content), &payload) != nil {
					t.Fatalf("content is not stable JSON: %q", toolResult.Content)
				}
				if _, ok := payload["stdout"]; !ok {
					t.Fatalf("missing stdout payload: %#v", payload)
				}
			}
			if mode == "close" && toolResult.OK {
				t.Fatalf("close failure tool result=%#v", toolResult)
			}
		})
	}
}

func TestCapabilityExecuteConcurrentIndependentRuns(t *testing.T) {
	const count = 20
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			provider := &runtimeExecProvider{mode: "success"}
			registry, err := sandbox.NewRegistry(2, sandbox.Registration{Metadata: sandbox.Metadata{ID: provider.ID(), Version: "1", ImplementationRevision: "concurrent-v1", Assurance: sandbox.Assurance{Level: sandbox.AssuranceProcess, SharedKernel: true}}, Provider: provider})
			if err != nil {
				errs <- err
				return
			}
			root := t.TempDir()
			limits := sandbox.Limits{WallTime: time.Second, MaxCPUTime: time.Second, MaxMemoryBytes: 1 << 20, MaxOutputBytes: 1 << 20}
			capability, err := New(Config{Registry: registry, ProviderID: provider.ID(), Version: "1", Limits: limits, ArtifactPolicy: sandbox.ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 1 << 20}, WorkRoot: root, ObjectStore: &testObjectStore{failAfter: -1}})
			if err != nil {
				errs <- err
				return
			}
			scope := core.MustScopePath(core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"})
			principal := core.Principal{SubjectID: "subject", TenantID: "tenant", Scope: scope, Grants: core.NewPermissionSet(core.PermWrite)}
			caps := core.NewCapabilityRegistry()
			if err := caps.Register(scope, capability); err != nil {
				errs <- err
				return
			}
			profiles := core.NewAgentProfileRegistry()
			sel := core.ModelSelection{Provider: "test", Model: "test"}
			if err := profiles.Bind(core.AgentProfileLayer{Scope: scope, ProfileID: "general", Model: &sel, AddCapabilities: []string{"sandbox.exec"}}); err != nil {
				errs <- err
				return
			}
			sessionID := "sandbox-session-" + strconv.Itoa(index)
			runID := "sandbox-run-" + strconv.Itoa(index)
			sessionScope, err := scope.Child(core.ScopeRef{Kind: core.ScopeSession, ID: sessionID})
			if err != nil {
				errs <- err
				return
			}
			session, err := core.NewSession(core.SessionOptions{ID: sessionID, ProfileID: "general", Principal: principal, Scope: sessionScope})
			if err != nil {
				errs <- err
				return
			}
			runtime := &core.Runtime{Capabilities: caps, Profiles: profiles, Models: core.ModelResolverFunc(func(context.Context, core.ModelSelection) (core.LlmAdapter, error) { return &agentToolModel{}, nil }), ToolJournal: &testJournal{}}
			if _, err := runtime.RunTurn(context.Background(), principal, session, core.TurnInput{RunID: runID, Text: "run"}, nil); err != nil {
				errs <- err
				return
			}
			provider.mu.Lock()
			starts, execs, closes := provider.starts, provider.execs, provider.closes
			provider.mu.Unlock()
			if starts != 1 || execs != 1 || closes != 1 {
				errs <- errors.New("invalid provider lifecycle counts")
				return
			}
			errs <- nil
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}
