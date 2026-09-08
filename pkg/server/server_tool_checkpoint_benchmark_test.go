package server

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/internal/testdb"
	core "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"

	_ "modernc.org/sqlite"
)

const (
	checkpointPerfSamplesEnv = "HARNESS_CHECKPOINT_PERF_SAMPLES"
	checkpointPerfBatchEnv   = "HARNESS_CHECKPOINT_PERF_BATCH"
)

// BenchmarkProtectedToolCheckpoint measures one complete, deterministic tool
// turn plus the final session flush. Setup and backend reset are excluded. The
// direct arm still uses the guarded ToolJournal path; noop_wrapper isolates
// wrapper overhead; durable_checkpoint adds the pre-tool session checkpoint.
func BenchmarkProtectedToolCheckpoint(b *testing.B) {
	b.Run("memory", func(b *testing.B) {
		benchmarkCheckpointPerfBackend(b, func(testing.TB) checkpointPerfBackend {
			return checkpointPerfMemoryBackend()
		})
	})
	b.Run("sqlite_file", func(b *testing.B) {
		benchmarkCheckpointPerfBackend(b, checkpointPerfSQLiteBackend)
	})
	b.Run("postgres", func(b *testing.B) {
		if os.Getenv("HARNESS_TEST_PG_DSN") == "" {
			b.Skip("HARNESS_TEST_PG_DSN is not configured")
		}
		benchmarkCheckpointPerfBackend(b, func(tb testing.TB) checkpointPerfBackend {
			open := testdb.Postgres(tb)
			return checkpointPerfSQLBackend(tb, open(), storage.SQLDialectPostgres, "postgres")
		})
	})
}

// TestPreToolCheckpointLatencyMeasurement records per-operation distributions.
// It is opt-in because it is a measurement, not a correctness gate. PostgreSQL
// uses internal/testdb's disposable schema and is omitted when no test DSN is
// configured. Throughput is one-worker measured-path throughput; setup/reset
// time is intentionally excluded from both latency and throughput.
func TestPreToolCheckpointLatencyMeasurement(t *testing.T) {
	if os.Getenv("HARNESS_RUN_CHECKPOINT_PERF") != "1" {
		t.Skip("set HARNESS_RUN_CHECKPOINT_PERF=1 to run checkpoint latency measurement")
	}
	samples := checkpointPerfSampleCount(t)
	batchSize := checkpointPerfBatchSize(t)
	t.Logf("CHECKPOINT_PERF environment go=%s os=%s arch=%s gomaxprocs=%d samples_per_case=%d operations_per_sample=%d latency_stat=batch_normalized_per_operation", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0), samples, batchSize)

	backends := []checkpointPerfBackendFactory{
		{name: "memory", open: func(testing.TB) checkpointPerfBackend { return checkpointPerfMemoryBackend() }},
		{name: "sqlite_file", open: checkpointPerfSQLiteBackend},
	}
	if os.Getenv("HARNESS_TEST_PG_DSN") != "" {
		backends = append(backends, checkpointPerfBackendFactory{
			name: "postgres",
			open: func(tb testing.TB) checkpointPerfBackend {
				open := testdb.Postgres(tb)
				return checkpointPerfSQLBackend(tb, open(), storage.SQLDialectPostgres, "postgres")
			},
		})
	} else {
		t.Log("CHECKPOINT_PERF backend=postgres status=not_measured reason=HARNESS_TEST_PG_DSN_not_configured")
	}

	for _, backendFactory := range backends {
		t.Run(backendFactory.name, func(t *testing.T) {
			backendsByMode := make(map[checkpointPerfMode]checkpointPerfBackend, len(checkpointPerfModes))
			latencies := make(map[checkpointPerfMode][]time.Duration, len(checkpointPerfModes))
			measured := make(map[checkpointPerfMode]time.Duration, len(checkpointPerfModes))
			for _, mode := range checkpointPerfModes {
				backend := backendFactory.open(t)
				backendsByMode[mode] = backend
				backend.reset(t)
				for warmup := 0; warmup < 10; warmup++ {
					op := newCheckpointPerfOperation(t, backend, mode, fmt.Sprintf("checkpoint-warmup-%d", warmup))
					if err := op.execute(context.Background()); err != nil {
						t.Fatalf("warmup mode=%s: %v", mode, err)
					}
					if err := op.validate(context.Background()); err != nil {
						t.Fatalf("validate warmup mode=%s: %v", mode, err)
					}
				}
				latencies[mode] = make([]time.Duration, 0, samples)
			}
			runtime.GC()
			for sample := 0; sample < samples; sample++ {
				// Rotate the first mode on every sample so sustained machine or
				// filesystem drift is distributed across all three arms.
				for offset := range checkpointPerfModes {
					mode := checkpointPerfModes[(sample+offset)%len(checkpointPerfModes)]
					backend := backendsByMode[mode]
					backend.reset(t)
					operations := make([]checkpointPerfOperation, 0, batchSize)
					for operation := 0; operation < batchSize; operation++ {
						id := fmt.Sprintf("checkpoint-performance-%d", operation)
						operations = append(operations, newCheckpointPerfOperation(t, backend, mode, id))
					}
					started := time.Now()
					var runErr error
					for operation := range operations {
						if err := operations[operation].execute(context.Background()); err != nil {
							runErr = err
							break
						}
					}
					elapsed := time.Since(started)
					if runErr != nil {
						t.Fatalf("sample=%d mode=%s: %v", sample, mode, runErr)
					}
					for operation := range operations {
						if err := operations[operation].validate(context.Background()); err != nil {
							t.Fatalf("validate sample=%d mode=%s operation=%d: %v", sample, mode, operation, err)
						}
					}
					latencies[mode] = append(latencies[mode], elapsed/time.Duration(batchSize))
					measured[mode] += elapsed
				}
			}
			results := make(map[checkpointPerfMode]checkpointPerfStats, len(checkpointPerfModes))
			for _, mode := range checkpointPerfModes {
				stats := summarizeCheckpointPerf(latencies[mode], measured[mode], samples*batchSize)
				results[mode] = stats
				t.Logf("CHECKPOINT_PERF backend=%s mode=%s samples=%d operations=%d batch_normalized_p50=%s batch_normalized_p95=%s batch_normalized_p99=%s mean=%s measured_path_throughput_ops_per_sec=%.2f", backendFactory.name, mode, samples, samples*batchSize, stats.p50, stats.p95, stats.p99, stats.mean, stats.throughput)
			}
			baseline := results[checkpointPerfDirect]
			for _, mode := range []checkpointPerfMode{checkpointPerfNoop, checkpointPerfDurable} {
				stats := results[mode]
				t.Logf("CHECKPOINT_PERF_OVERHEAD backend=%s mode=%s versus=%s p50_delta=%s p95_delta=%s p99_delta=%s throughput_change_percent=%.2f", backendFactory.name, mode, checkpointPerfDirect, stats.p50-baseline.p50, stats.p95-baseline.p95, stats.p99-baseline.p99, percentChange(stats.throughput, baseline.throughput))
			}
		})
	}
}

type checkpointPerfMode string

const (
	checkpointPerfDirect  checkpointPerfMode = "direct_journal"
	checkpointPerfNoop    checkpointPerfMode = "noop_wrapper"
	checkpointPerfDurable checkpointPerfMode = "durable_checkpoint"
)

var checkpointPerfModes = []checkpointPerfMode{checkpointPerfDirect, checkpointPerfNoop, checkpointPerfDurable}

type checkpointPerfBackend struct {
	name    string
	reset   func(testing.TB)
	prepare func(testing.TB, string) (core.SessionStore, *core.Session)
}

type checkpointPerfBackendFactory struct {
	name string
	open func(testing.TB) checkpointPerfBackend
}

func checkpointPerfMemoryBackend() checkpointPerfBackend {
	return checkpointPerfBackend{
		name:  "memory",
		reset: func(testing.TB) {},
		prepare: func(tb testing.TB, id string) (core.SessionStore, *core.Session) {
			tb.Helper()
			store := core.NewMemorySessionStore()
			session := checkpointPerfSession(tb, id)
			if err := store.Create(context.Background(), session); err != nil {
				tb.Fatal(err)
			}
			return store, session
		},
	}
}

func checkpointPerfSQLiteBackend(tb testing.TB) checkpointPerfBackend {
	tb.Helper()
	db, err := sql.Open("sqlite", filepath.Join(tb.TempDir(), "checkpoint-performance.db"))
	if err != nil {
		tb.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	tb.Cleanup(func() { _ = db.Close() })
	return checkpointPerfSQLBackend(tb, db, storage.SQLDialectSQLite, "sqlite_file")
}

func checkpointPerfSQLBackend(tb testing.TB, db *sql.DB, dialect storage.SQLDialect, name string) checkpointPerfBackend {
	tb.Helper()
	store, err := storage.OpenSQLSessionStore(context.Background(), db, dialect)
	if err != nil {
		tb.Fatal(err)
	}
	return checkpointPerfBackend{
		name: name,
		reset: func(tb testing.TB) {
			tb.Helper()
			checkpointPerfResetSQL(tb, db)
		},
		prepare: func(tb testing.TB, id string) (core.SessionStore, *core.Session) {
			tb.Helper()
			session := checkpointPerfSession(tb, id)
			if err := store.Create(context.Background(), session); err != nil {
				tb.Fatal(err)
			}
			return store, session
		},
	}
}

func checkpointPerfResetSQL(tb testing.TB, db *sql.DB) {
	tb.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM event_chunks"); err != nil {
		tb.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM sessions"); err != nil {
		tb.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
}

func benchmarkCheckpointPerfBackend(b *testing.B, open func(testing.TB) checkpointPerfBackend) {
	for _, mode := range checkpointPerfModes {
		mode := mode
		b.Run(string(mode), func(b *testing.B) {
			b.StopTimer()
			backend := open(b)
			backend.reset(b)
			preflight := newCheckpointPerfOperation(b, backend, mode, "checkpoint-performance-preflight")
			if err := preflight.execute(context.Background()); err != nil {
				b.Fatal(err)
			}
			if err := preflight.validate(context.Background()); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				backend.reset(b)
				op := newCheckpointPerfOperation(b, backend, mode, "checkpoint-performance")
				b.StartTimer()
				if err := op.execute(context.Background()); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if err := op.validateState(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type checkpointPerfOperation struct {
	agent   *core.Agent
	writer  *storage.WriteBehind
	store   core.SessionStore
	session *core.Session
	model   *checkpointPerfModel
	tool    *checkpointPerfToolRuntime
	result  core.TurnResult
}

func newCheckpointPerfOperation(tb testing.TB, backend checkpointPerfBackend, mode checkpointPerfMode, sessionID string) checkpointPerfOperation {
	tb.Helper()
	store, session := backend.prepare(tb, sessionID)
	writer := storage.NewWriteBehind(store, session, 0, -1)
	journal := core.ToolInvocationJournal(checkpointPerfJournal{})
	switch mode {
	case checkpointPerfDirect:
	case checkpointPerfNoop:
		journal = &checkpointToolInvocationJournal{
			ToolInvocationJournal: journal,
			checkpoint:            func(context.Context) error { return nil },
			cancel:                func() {},
			failure:               &toolCheckpointFailure{},
		}
	case checkpointPerfDurable:
		runtimeCopy, _ := runtimeWithToolCheckpoint(&core.Runtime{ToolJournal: journal}, writer, func() {})
		journal = runtimeCopy.ToolJournal
	default:
		tb.Fatalf("unknown checkpoint performance mode %q", mode)
	}
	model := &checkpointPerfModel{}
	tool := &checkpointPerfToolRuntime{}
	agent, err := core.NewAgent(core.AgentOptions{
		LLM:          model,
		Tools:        tool,
		Session:      session,
		MaxSteps:     2,
		MaxToolCalls: 1,
		ToolJournal:  journal,
		OnEvent: func(core.SessionEvent) {
			writer.MarkDirty()
		},
	})
	if err != nil {
		tb.Fatal(err)
	}
	return checkpointPerfOperation{agent: agent, writer: writer, store: store, session: session, model: model, tool: tool}
}

func (o *checkpointPerfOperation) execute(ctx context.Context) error {
	result, err := o.agent.RunTurn(ctx, core.TurnInput{RunID: "run-checkpoint-perf", Text: "perform the local tool"})
	if err != nil {
		return err
	}
	o.result = result
	if err := o.writer.Flush(ctx); err != nil {
		return err
	}
	return nil
}

func (o *checkpointPerfOperation) validate(ctx context.Context) error {
	if err := o.validateState(); err != nil {
		return err
	}
	stored, err := o.store.Load(ctx, o.session.ID())
	if err != nil {
		return fmt.Errorf("load persisted session: %w", err)
	}
	if !reflect.DeepEqual(stored.Events(), o.session.Events()) {
		return fmt.Errorf("persisted events differ: got=%d want=%d", stored.Version(), o.session.Version())
	}
	return nil
}

func (o *checkpointPerfOperation) validateState() error {
	if o.result.Status != core.RunCompleted {
		return fmt.Errorf("run status=%s", o.result.Status)
	}
	if o.model.calls != 2 {
		return fmt.Errorf("model calls=%d, want 2", o.model.calls)
	}
	if o.tool.calls != 1 {
		return fmt.Errorf("tool calls=%d, want 1", o.tool.calls)
	}
	if got, want := o.writer.SavedVersion(), o.session.Version(); got != want {
		return fmt.Errorf("saved version=%d, want %d", got, want)
	}
	return nil
}

type checkpointPerfModel struct{ calls int }

func (*checkpointPerfModel) Provider() string { return "checkpoint-performance-local" }

func (m *checkpointPerfModel) Stream(_ context.Context, _ core.GenerateOptions, emit func(core.StreamChunk)) error {
	m.calls++
	if m.calls == 1 {
		call := core.ToolCall{ID: "call-checkpoint-perf", Name: "performance.write", Args: map[string]any{"value": "one"}}
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
		return nil
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: "done"})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
	return nil
}

type checkpointPerfToolRuntime struct{ calls int }

func (t *checkpointPerfToolRuntime) Schemas() []core.ToolSchema {
	return []core.ToolSchema{{Name: "performance.write", Description: "local deterministic performance tool", Parameters: checkpointPerfParameters()}}
}

func (*checkpointPerfToolRuntime) Authorized(name string) bool { return name == "performance.write" }

func (*checkpointPerfToolRuntime) MaxCallBudget() int { return 1 }

func (*checkpointPerfToolRuntime) ManifestFor(name string) (core.CapabilityManifest, bool) {
	if name != "performance.write" {
		return core.CapabilityManifest{}, false
	}
	return core.CapabilityManifest{
		ID: "performance.write", Version: "1.0.0", Name: "Performance write", Kind: core.KindTool,
		Contract: "harness.tool/v1", RequiredPermissions: []core.Permission{core.PermWrite},
		Idempotent: true, Tool: &core.ToolExposure{Parameters: checkpointPerfParameters()},
	}, true
}

func (t *checkpointPerfToolRuntime) Execute(context.Context, core.ToolCall) (core.CapabilityResult, error) {
	t.calls++
	return core.CapabilityResult{Content: `{"written":true}`, OK: true}, nil
}

func checkpointPerfParameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"value": map[string]any{"type": "string"},
		},
		"required": []any{"value"},
	}
}

type checkpointPerfJournal struct{}

func (checkpointPerfJournal) BeginToolInvocation(_ context.Context, invocation core.ToolInvocation) (core.ToolInvocationRecord, core.ToolInvocationDecision, error) {
	now := time.Now().UTC()
	return core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationStarted, StartedAt: now, UpdatedAt: now}, core.ToolInvocationExecuteNew, nil
}

func (checkpointPerfJournal) CompleteToolInvocation(_ context.Context, invocation core.ToolInvocation, result core.CapabilityResult) (core.ToolInvocationRecord, error) {
	now := time.Now().UTC()
	return core.ToolInvocationRecord{ToolInvocation: invocation, State: core.ToolInvocationCompleted, Result: &result, StartedAt: now, UpdatedAt: now, CompletedAt: now}, nil
}

func (checkpointPerfJournal) MarkToolInvocationUncertain(context.Context, core.ToolInvocation, string) error {
	return nil
}

func checkpointPerfSession(tb testing.TB, id string) *core.Session {
	tb.Helper()
	global := core.ScopeRef{Kind: core.ScopeGlobal, ID: "global"}
	product := core.ScopeRef{Kind: core.ScopeProduct, ID: "performance"}
	tenant := core.ScopeRef{Kind: core.ScopeTenant, ID: "tenant-perf"}
	user := core.ScopeRef{Kind: core.ScopeUser, ID: "user-perf"}
	sessionRef := core.ScopeRef{Kind: core.ScopeSession, ID: id}
	userScope := core.MustScopePath(global, product, tenant, user)
	principal := core.Principal{
		SubjectID: "user-perf", TenantID: "tenant-perf", Scope: userScope,
		Grants: core.NewPermissionSet(core.PermRead, core.PermWrite),
	}
	session, err := core.NewSession(core.SessionOptions{
		ID: id, ProfileID: "performance.agent", Principal: principal,
		Scope: core.MustScopePath(global, product, tenant, user, sessionRef),
	})
	if err != nil {
		tb.Fatal(err)
	}
	return session
}

type checkpointPerfStats struct {
	p50        time.Duration
	p95        time.Duration
	p99        time.Duration
	mean       time.Duration
	throughput float64
}

func summarizeCheckpointPerf(latencies []time.Duration, measured time.Duration, operations int) checkpointPerfStats {
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return checkpointPerfStats{
		p50:        checkpointPerfPercentile(sorted, 50),
		p95:        checkpointPerfPercentile(sorted, 95),
		p99:        checkpointPerfPercentile(sorted, 99),
		mean:       measured / time.Duration(operations),
		throughput: float64(operations) / measured.Seconds(),
	}
}

func checkpointPerfPercentile(sorted []time.Duration, percentile int) time.Duration {
	index := (len(sorted)*percentile + 99) / 100
	if index < 1 {
		index = 1
	}
	return sorted[index-1]
}

func percentChange(value, baseline float64) float64 {
	if baseline == 0 {
		return 0
	}
	return (value/baseline - 1) * 100
}

func checkpointPerfSampleCount(tb testing.TB) int {
	tb.Helper()
	raw := os.Getenv(checkpointPerfSamplesEnv)
	if raw == "" {
		return 500
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 100 || value > 100000 {
		tb.Fatalf("%s must be an integer between 100 and 100000", checkpointPerfSamplesEnv)
	}
	return value
}

func checkpointPerfBatchSize(tb testing.TB) int {
	tb.Helper()
	raw := os.Getenv(checkpointPerfBatchEnv)
	if raw == "" {
		return 32
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 2 || value > 1024 {
		tb.Fatalf("%s must be an integer between 2 and 1024", checkpointPerfBatchEnv)
	}
	return value
}
