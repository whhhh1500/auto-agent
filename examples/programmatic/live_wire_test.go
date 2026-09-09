package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

// liveWireTransportMu serializes the one live acceptance test that observes
// the process-wide default transport. The production HTTP provider creates a
// client with a nil Transport, so it resolves http.DefaultTransport at send
// time. This test does not run in parallel.
var liveWireTransportMu sync.Mutex

// TestLiveBatchWireAcceptance records a non-sensitive summary of each actual
// Responses request for the direct eight-item baseline. It is deliberately
// opt-in: it makes real requests only when the existing live-test guard is
// enabled and Responses was selected explicitly.
func TestLiveBatchWireAcceptance(t *testing.T) {
	if os.Getenv("HARNESS_PROGRAMMATIC_LIVE") != "1" && os.Getenv("HARNESS_ACCEPTANCE_LIVE_PROGRAMMATIC") != "1" {
		t.Skip("set HARNESS_PROGRAMMATIC_LIVE=1 to make intentional live model requests")
	}
	if liveProtocol() != "responses" {
		t.Skip("set HARNESS_PROGRAMMATIC_PROTOCOL=responses to audit Responses wire requests")
	}

	liveWireTransportMu.Lock()
	previous := http.DefaultTransport
	if previous == nil {
		liveWireTransportMu.Unlock()
		t.Fatal("default HTTP transport is unavailable")
	}
	transport := &liveWireAuditTransport{next: previous}
	http.DefaultTransport = transport
	t.Cleanup(func() {
		http.DefaultTransport = previous
		liveWireTransportMu.Unlock()
	})

	modelID := strings.TrimSpace(os.Getenv("HARNESS_LLM_MODEL"))
	if modelID == "" {
		t.Fatal("live model configuration is incomplete or invalid")
	}
	interval, err := liveMinRequestInterval(os.Getenv("HARNESS_PROGRAMMATIC_MIN_REQUEST_INTERVAL"))
	if err != nil {
		t.Fatal("live request interval is invalid")
	}
	for _, scenario := range []struct {
		name                string
		includeProgrammatic bool
		wantToolCount       int
	}{
		{name: "batch_n8_batched_direct_wire_full_menu", includeProgrammatic: true, wantToolCount: 4},
		{name: "batch_n8_batched_direct_wire_direct_menu", includeProgrammatic: false, wantToolCount: 2},
	} {
		stopAfterTransportOrProtocolFailure := false
		passed := t.Run(scenario.name, func(t *testing.T) {
			caseDef := liveCase{
				name:                 scenario.name,
				activeRows:           8,
				maxModelRounds:       3,
				maxToolCalls:         9,
				requireBatchedDirect: true,
				prompt:               liveBatchedDirectPrompt(8),
			}
			runID := "live-" + caseDef.name
			startRequest := transport.count()
			var inner *liveModel
			var model *liveWireCallAuditModel
			var result core.TurnResult
			var events []core.SessionEvent
			var elapsed time.Duration
			t.Cleanup(func() {
				acceptancePassed := !t.Failed() && result.Status == core.RunCompleted
				standard := finalizedLiveEvidence(caseDef, result, modelID, liveProtocol(), liveSourceRevision(), caseDef.prompt, events, inner, elapsed, acceptancePassed)
				if standard.RunID == "" {
					standard.RunID = runID
				}
				if err := writeLiveEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), standard); err != nil {
					t.Error("could not write live experiment evidence")
				}
				wire := liveWireEvidenceRecord{
					Schema:           "harness.programmatic.live-wire-evidence/v1",
					CaseID:           caseDef.name,
					RunID:            standard.RunID,
					RequestedModel:   modelID,
					Protocol:         liveProtocol(),
					SourceRevision:   liveSourceRevision(),
					RuntimeStatus:    liveRunStatus(result),
					AcceptancePassed: acceptancePassed,
					TotalElapsedMS:   elapsed.Milliseconds(),
					HTTPRequests:     transport.snapshotSince(startRequest),
					ToolCallSequence: model.sequence(),
				}
				if err := writeLiveWireEvidence(os.Getenv("HARNESS_PROGRAMMATIC_EVIDENCE_PATH"), wire); err != nil {
					t.Error("could not write live wire experiment evidence")
				}
			})
			adapter, err := newLiveAdapter(context.Background())
			if err != nil {
				t.Fatal("live model configuration is incomplete or invalid")
			}
			inner = newLiveModel(t, adapter, caseDef.name, caseDef.maxModelRounds, &liveRequestPacer{interval: interval})
			model = &liveWireCallAuditModel{inner: inner, seen: make(map[string]struct{})}

			fixture, err := newFixtureWithOutputContracts(model, scenario.includeProgrammatic, caseDef.activeRows, false)
			if err != nil {
				t.Fatal("could not construct live programmatic fixture")
			}
			if err := configureLiveProfile(fixture, adapter.Provider(), modelID, caseDef.maxModelRounds, caseDef.maxToolCalls); err != nil {
				t.Fatal("could not configure live programmatic fixture profile")
			}
			started := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			result, err = fixture.runtime.RunTurn(ctx, fixture.principal, fixture.session, core.TurnInput{RunID: runID, Text: caseDef.prompt}, nil)
			elapsed = time.Since(started)
			events = fixture.session.Events()
			if err != nil || result.Status != core.RunCompleted {
				stopAfterTransportOrProtocolFailure = inner.stopSubsequentCases()
				t.Fatalf("live wire case did not complete (status=%s, model_rounds=%d)", result.Status, inner.rounds())
			}
			assertLiveEvidence(t, fixture, caseDef, inner, result)
			assertLiveWireRequests(t, transport.snapshotSince(startRequest), inner.rounds(), scenario.wantToolCount)
			assertLiveWireCallSequence(t, model.sequence(), caseDef.activeRows)
		})
		if !passed && stopAfterTransportOrProtocolFailure {
			t.Log("live model HTTP, transport, or responses protocol failure: remaining wire cases skipped")
			break
		}
	}
}

type liveWireAuditTransport struct {
	next http.RoundTripper

	mu       sync.Mutex
	requests []liveWireRequestEvidence
}

func (t *liveWireAuditTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if t == nil || t.next == nil {
		return nil, errors.New("live wire transport is invalid")
	}
	if request == nil || request.GetBody == nil {
		t.record(liveWireRequestEvidence{})
		return t.next.RoundTrip(request)
	}
	copyBody, err := request.GetBody()
	if err != nil {
		t.record(liveWireRequestEvidence{})
		return t.next.RoundTrip(request)
	}
	body, err := io.ReadAll(copyBody)
	closeErr := copyBody.Close()
	if err != nil || closeErr != nil {
		t.record(liveWireRequestEvidence{})
		return t.next.RoundTrip(request)
	}
	// GetBody provides an independent reader. The request body and headers are
	// never consumed, reset, inspected, or persisted by the audit.
	t.record(liveWireRequestFromBody(body))
	clear(body)
	return t.next.RoundTrip(request)
}

func (t *liveWireAuditTransport) record(record liveWireRequestEvidence) {
	t.mu.Lock()
	defer t.mu.Unlock()
	record.Request = len(t.requests) + 1
	t.requests = append(t.requests, record)
}

func (t *liveWireAuditTransport) snapshot() []liveWireRequestEvidence {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]liveWireRequestEvidence(nil), t.requests...)
}

func (t *liveWireAuditTransport) count() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.requests)
}

func (t *liveWireAuditTransport) snapshotSince(start int) []liveWireRequestEvidence {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if start < 0 || start > len(t.requests) {
		return nil
	}
	return append([]liveWireRequestEvidence(nil), t.requests[start:]...)
}

// liveWireRequestEvidence contains a structural audit only. It never retains
// request input, tool schemas, arguments, headers, endpoint, or response body.
type liveWireRequestEvidence struct {
	Request                  int    `json:"request"`
	BodyObserved             bool   `json:"body_observed"`
	RequestBytes             int    `json:"request_bytes"`
	RequestSHA256            string `json:"request_sha256"`
	JSONValid                bool   `json:"json_valid"`
	InstructionsPresent      bool   `json:"instructions_present"`
	InstructionsBytes        int    `json:"instructions_bytes"`
	ParallelToolCallsPresent bool   `json:"parallel_tool_calls_present"`
	ParallelToolCalls        bool   `json:"parallel_tool_calls"`
	ToolCount                int    `json:"tool_count"`
	StreamPresent            bool   `json:"stream_present"`
	Stream                   bool   `json:"stream"`
	StorePresent             bool   `json:"store_present"`
	Store                    bool   `json:"store"`
	ToolChoicePresent        bool   `json:"tool_choice_present"`
}

func liveWireRequestFromBody(body []byte) liveWireRequestEvidence {
	digest := sha256.Sum256(body)
	record := liveWireRequestEvidence{BodyObserved: true, RequestBytes: len(body), RequestSHA256: fmt.Sprintf("%x", digest)}
	var values map[string]json.RawMessage
	if json.Unmarshal(body, &values) != nil {
		return record
	}
	record.JSONValid = true
	if instructions, exists := values["instructions"]; exists {
		record.InstructionsPresent = true
		var text string
		if json.Unmarshal(instructions, &text) == nil {
			record.InstructionsBytes = len([]byte(text))
		}
	}
	record.ParallelToolCallsPresent, record.ParallelToolCalls = liveWireBoolean(values, "parallel_tool_calls")
	record.StreamPresent, record.Stream = liveWireBoolean(values, "stream")
	record.StorePresent, record.Store = liveWireBoolean(values, "store")
	_, record.ToolChoicePresent = values["tool_choice"]
	if tools, exists := values["tools"]; exists {
		var values []json.RawMessage
		if json.Unmarshal(tools, &values) == nil {
			record.ToolCount = len(values)
		}
	}
	return record
}

func liveWireBoolean(values map[string]json.RawMessage, name string) (present bool, value bool) {
	raw, present := values[name]
	if !present || json.Unmarshal(raw, &value) != nil {
		return present, false
	}
	return true, value
}

type liveWireCallAuditModel struct {
	inner core.LlmAdapter

	mu    sync.Mutex
	seen  map[string]struct{}
	calls []string
}

func (m *liveWireCallAuditModel) Provider() string { return m.inner.Provider() }

func (m *liveWireCallAuditModel) Stream(ctx context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	if m == nil || m.inner == nil {
		return errors.New("live wire call audit model is invalid")
	}
	return m.inner.Stream(ctx, options, func(chunk core.StreamChunk) {
		m.observe(chunk.ToolCall)
		for _, call := range chunk.ToolCalls {
			m.observe(&call)
		}
		emit(chunk)
	})
}

func (m *liveWireCallAuditModel) observe(call *core.ToolCall) {
	if call == nil || call.Name == "" {
		return
	}
	key := call.ID
	if key == "" {
		// The production stream provides call IDs. An anonymous call remains
		// visible without recording arguments, but cannot be de-duplicated.
		key = fmt.Sprintf("anonymous-%d", time.Now().UnixNano())
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.seen[key]; exists {
		return
	}
	m.seen[key] = struct{}{}
	m.calls = append(m.calls, call.Name)
}

func (m *liveWireCallAuditModel) sequence() []string {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.calls...)
}

func assertLiveWireRequests(t *testing.T, requests []liveWireRequestEvidence, rounds, wantToolCount int) {
	t.Helper()
	if len(requests) != rounds || rounds != 3 {
		t.Fatalf("wire request count=%d, model rounds=%d, want 3", len(requests), rounds)
	}
	for _, request := range requests {
		if !request.BodyObserved || !request.JSONValid || !request.InstructionsPresent || request.InstructionsBytes == 0 || !request.ParallelToolCallsPresent || !request.ParallelToolCalls || request.ToolCount != wantToolCount || !request.StreamPresent || !request.Stream || !request.StorePresent || request.Store || request.ToolChoicePresent {
			t.Fatalf("unexpected Responses request structural audit: %+v", request)
		}
	}
}

func assertLiveWireCallSequence(t *testing.T, sequence []string, activeRows int) {
	t.Helper()
	if len(sequence) != activeRows+1 || sequence[0] != listToolID {
		t.Fatalf("model tool-call sequence does not start with one inventory call: %v", sequence)
	}
	for _, name := range sequence[1:] {
		if name != detailToolID {
			t.Fatalf("model tool-call sequence includes a non-detail call: %v", sequence)
		}
	}
}

type liveWireEvidenceRecord struct {
	Schema           string                    `json:"schema"`
	CaseID           string                    `json:"case_id"`
	RunID            string                    `json:"run_id"`
	RequestedModel   string                    `json:"requested_model"`
	Protocol         string                    `json:"protocol"`
	SourceRevision   string                    `json:"source_revision"`
	RuntimeStatus    string                    `json:"runtime_status"`
	AcceptancePassed bool                      `json:"acceptance_passed"`
	TotalElapsedMS   int64                     `json:"total_elapsed_ms"`
	HTTPRequests     []liveWireRequestEvidence `json:"http_requests"`
	ToolCallSequence []string                  `json:"tool_call_sequence"`
}

func writeLiveWireEvidence(directory string, record liveWireEvidenceRecord) error {
	if strings.TrimSpace(directory) == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("live wire evidence directory is unavailable")
	}
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return errors.New("live wire evidence encoding failed")
	}
	identity := sha256.Sum256([]byte(record.RunID + "\x00" + record.CaseID + "\x00" + time.Now().UTC().Format(time.RFC3339Nano)))
	path := filepath.Join(directory, "programmatic-live-wire-"+fmt.Sprintf("%x", identity[:12])+".json")
	return writeLiveEvidenceFile(path, append(payload, '\n'))
}

type liveWireFakeRoundTripper struct{ body []byte }

func (t *liveWireFakeRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || request.Body == nil {
		return nil, errors.New("fake transport received no body")
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	t.body = append([]byte(nil), body...)
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}")), Request: request}, nil
}

type liveWireTrackingBody struct {
	reader *bytes.Reader
	reads  int
}

func (b *liveWireTrackingBody) Read(value []byte) (int, error) {
	n, err := b.reader.Read(value)
	b.reads += n
	return n, err
}

func (*liveWireTrackingBody) Close() error { return nil }

func TestLiveWireAuditForwardsExactBodyAndRedactsAuthorization(t *testing.T) {
	body := []byte(`{"model":"test","instructions":"test instruction","parallel_tool_calls":true,"stream":true,"store":false,"tools":[{"type":"function"}]}`)
	fake := &liveWireFakeRoundTripper{}
	audit := &liveWireAuditTransport{next: fake}
	request, err := http.NewRequest(http.MethodPost, "https://example.test/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	original := &liveWireTrackingBody{reader: bytes.NewReader(body)}
	request.Body = original
	request.ContentLength = int64(len(body))
	request.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	request.Header.Set("Authorization", "Bearer offline-test-secret")
	response, err := audit.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if !bytes.Equal(fake.body, body) {
		t.Fatal("wire audit changed request body before forwarding")
	}
	if request.Body != original || original.reads != len(body) {
		t.Fatal("wire audit replaced or consumed the original request body")
	}
	records := audit.snapshot()
	if len(records) != 1 || !records[0].BodyObserved || !records[0].JSONValid || !records[0].InstructionsPresent || records[0].InstructionsBytes != len("test instruction") || !records[0].ParallelToolCallsPresent || !records[0].ParallelToolCalls || records[0].ToolCount != 1 || !records[0].Stream || records[0].Store || records[0].ToolChoicePresent {
		t.Fatalf("wire audit did not produce expected structural summary: %+v", records)
	}
	evidence, err := json.Marshal(liveWireEvidenceRecord{Schema: "test", HTTPRequests: records})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(evidence), "offline-test-secret") || strings.Contains(string(evidence), "Authorization") || strings.Contains(string(evidence), "Bearer") || strings.Contains(string(evidence), string(body)) {
		t.Fatal("wire evidence retained secret header or raw request body")
	}
}

func TestLiveWireRequestSummaryOmitsInstructionsWhenAbsent(t *testing.T) {
	body := []byte(`{"model":"test","stream":true}`)
	record := liveWireRequestFromBody(body)
	if !record.BodyObserved || !record.JSONValid || record.InstructionsPresent || record.InstructionsBytes != 0 {
		t.Fatalf("instruction-free request summary=%+v", record)
	}
}

func TestLiveWireAuditMarksRequestUnobservedWithoutGetBody(t *testing.T) {
	body := []byte(`{"stream":true}`)
	fake := &liveWireFakeRoundTripper{}
	audit := &liveWireAuditTransport{next: fake}
	request, err := http.NewRequest(http.MethodPost, "https://example.test/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	original := &liveWireTrackingBody{reader: bytes.NewReader(body)}
	request.Body = original
	response, err := audit.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if request.Body != original || !bytes.Equal(fake.body, body) {
		t.Fatal("unobserved request body was changed before forwarding")
	}
	records := audit.snapshot()
	if len(records) != 1 || records[0].BodyObserved || records[0].RequestBytes != 0 || records[0].RequestSHA256 != "" {
		t.Fatalf("request without GetBody was represented as observed: %+v", records)
	}
}
