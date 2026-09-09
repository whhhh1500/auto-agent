package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func routeEvidenceReceiptInput(sequence, sourceVersion int64) RouteEvidenceReceiptInput {
	payload := []byte(`{"programmatic.route.identity.status":"unavailable","run.id":"route-evidence-run","run.status":"completed","session.id":"route-evidence-session"}`)
	sum := sha256.Sum256(payload)
	return RouteEvidenceReceiptInput{
		Protocol: RouteEvidenceReceiptProtocol, SessionID: "route-evidence-session", RunID: "route-evidence-run",
		TerminalEventSeq: sequence, SourceSessionVersion: sourceVersion, TerminalStatus: "completed",
		Payload: payload, PayloadSHA256: hex.EncodeToString(sum[:]), CreatedAt: time.Unix(1700000000, 0).UTC(),
	}
}

func TestSQLRouteEvidenceReceiptCreateOrLoadIsImmutable(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	input := routeEvidenceReceiptInput(8, 9)

	first, created, err := store.CreateOrLoadRouteEvidenceReceipt(ctx, input)
	if err != nil || !created {
		t.Fatalf("first receipt=%+v created=%t err=%v", first, created, err)
	}
	if first.ReceiptID != RouteEvidenceReceiptID(input) || first.SourceSessionVersion != 9 {
		t.Fatalf("receipt identity=%+v", first)
	}
	second, created, err := store.CreateOrLoadRouteEvidenceReceipt(ctx, input)
	if err != nil || created || second.ReceiptID != first.ReceiptID {
		t.Fatalf("same receipt=%+v created=%t err=%v", second, created, err)
	}

	changedPayload := input
	changedPayload.Payload = []byte(`{"programmatic.route.identity.status":"unavailable","run.id":"route-evidence-run","run.status":"failed","session.id":"route-evidence-session"}`)
	sum := sha256.Sum256(changedPayload.Payload)
	changedPayload.PayloadSHA256 = hex.EncodeToString(sum[:])
	if _, _, err := store.CreateOrLoadRouteEvidenceReceipt(ctx, changedPayload); err == nil {
		t.Fatal("payload conflict was accepted")
	}
	changedSource := input
	changedSource.SourceSessionVersion++
	if _, _, err := store.CreateOrLoadRouteEvidenceReceipt(ctx, changedSource); err == nil {
		t.Fatal("source cursor conflict was accepted")
	}
	badDigest := input
	badDigest.PayloadSHA256 = strings.Repeat("0", 64)
	if _, _, err := store.CreateOrLoadRouteEvidenceReceipt(ctx, badDigest); err == nil {
		t.Fatal("payload digest mismatch was accepted")
	}

	var receipts, outbox int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM route_evidence_receipts").Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("receipt count=%d err=%v", receipts, err)
	}
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM route_evidence_outbox").Scan(&outbox); err != nil || outbox != 1 {
		t.Fatalf("outbox count=%d err=%v", outbox, err)
	}
}

func TestSQLRouteEvidenceReceiptRejectsNonObjectAndOversizedPayload(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	nonObject := routeEvidenceReceiptInput(8, 9)
	nonObject.Payload = []byte(`[]`)
	nonObject.PayloadSHA256 = routeEvidencePayloadDigest(nonObject.Payload)
	if _, _, err := store.CreateOrLoadRouteEvidenceReceipt(ctx, nonObject); err == nil {
		t.Fatal("non-object route evidence payload was accepted")
	}
	overSize := routeEvidenceReceiptInput(8, 9)
	overSize.Payload = append([]byte(`{"facts":"`), make([]byte, MaxRouteEvidencePayloadBytes)...)
	overSize.Payload = append(overSize.Payload, []byte(`"}`)...)
	overSize.PayloadSHA256 = routeEvidencePayloadDigest(overSize.Payload)
	if _, _, err := store.CreateOrLoadRouteEvidenceReceipt(ctx, overSize); err == nil {
		t.Fatal("oversized route evidence payload was accepted")
	}
}

func TestSQLRouteEvidenceReceiptEnforcesContentFreeV1PayloadBoundary(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	for name, payload := range map[string][]byte{
		"unknown":   []byte(`{"programmatic.route.identity.status":"unavailable","run.id":"route-evidence-run","run.status":"completed","session.id":"route-evidence-session","prompt":"secret"}`),
		"secret":    []byte(`{"programmatic.route.identity.status":"unavailable","run.id":"route-evidence-run","run.status":"completed","session.id":"route-evidence-session","programmatic.route.selection.status":"secret"}`),
		"mismatch":  []byte(`{"programmatic.route.identity.status":"unavailable","run.id":"other-run","run.status":"completed","session.id":"route-evidence-session"}`),
		"nonstring": []byte(`{"programmatic.route.identity.status":false,"run.id":"route-evidence-run","run.status":"completed","session.id":"route-evidence-session"}`),
	} {
		t.Run(name, func(t *testing.T) {
			input := routeEvidenceReceiptInput(8, 9)
			input.Payload = payload
			input.PayloadSHA256 = routeEvidencePayloadDigest(payload)
			if _, _, err := store.CreateOrLoadRouteEvidenceReceipt(ctx, input); err == nil {
				t.Fatalf("%s payload was accepted", name)
			}
		})
	}
	canonical := routeEvidenceReceiptInput(10, 11)
	canonical.Payload = []byte("{ \n \t\"session.id\": \"route-evidence-session\", \"run.status\": \"completed\", \"programmatic.route.identity.status\": \"unavailable\", \"run.id\": \"route-evidence-run\" }")
	canonical.PayloadSHA256 = routeEvidencePayloadDigest([]byte(`{"programmatic.route.identity.status":"unavailable","run.id":"route-evidence-run","run.status":"completed","session.id":"route-evidence-session"}`))
	receipt, created, err := store.CreateOrLoadRouteEvidenceReceipt(ctx, canonical)
	if err != nil || !created {
		t.Fatalf("canonical receipt=%+v created=%t err=%v", receipt, created, err)
	}
	want := `{"programmatic.route.identity.status":"unavailable","run.id":"route-evidence-run","run.status":"completed","session.id":"route-evidence-session"}`
	if string(receipt.Payload) != want || receipt.PayloadSHA256 != routeEvidencePayloadDigest([]byte(want)) {
		t.Fatalf("canonical receipt payload=%s digest=%s", receipt.Payload, receipt.PayloadSHA256)
	}
}

func TestSQLRouteEvidenceReceiptAcceptsExactVerifiedV1Payload(t *testing.T) {
	store := newTestSQLStore(t)
	values := map[string]string{
		"run.id": "route-evidence-run", "session.id": "route-evidence-session", "run.status": "completed",
		"programmatic.route.identity.status": "verified", "programmatic.route.version": "2", "programmatic.route.mode": "auto_probe_once", "programmatic.route.implementation": routeEvidenceV2Implementation,
		"programmatic.route.selection": "unavailable", "programmatic.route.selection.status": "unavailable",
		"programmatic.route.coverage.status": "unavailable", "programmatic.route.coverage.candidates": "0", "programmatic.route.coverage.selected": "0", "programmatic.route.coverage.executed": "0", "programmatic.route.coverage.duplicate": "false", "programmatic.route.coverage.exact_once": "false",
		"programmatic.route.effects.journal.status": "unavailable", "programmatic.route.effects.journal.completed": "0", "programmatic.route.effects.external.status": "unavailable",
		"programmatic.route.accounting.status": "unavailable", "programmatic.route.accounting.model_calls": "0",
		"programmatic.route.context.archive.status": "unavailable", "programmatic.route.context.assembly.status": "unavailable", "programmatic.route.context.summary_events": "0",
	}
	payload, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	input := routeEvidenceReceiptInput(12, 13)
	input.Payload = payload
	input.PayloadSHA256 = routeEvidencePayloadDigest(payload)
	if _, created, err := store.CreateOrLoadRouteEvidenceReceipt(context.Background(), input); err != nil || !created {
		t.Fatalf("verified v1 receipt created=%t err=%v", created, err)
	}
}

func TestSQLRouteEvidenceOutboxFencesClaimAckAndRetry(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	receipt, created, err := store.CreateOrLoadRouteEvidenceReceipt(ctx, routeEvidenceReceiptInput(8, 9))
	if err != nil || !created {
		t.Fatalf("create receipt=%+v created=%t err=%v", receipt, created, err)
	}
	first, claimed, err := store.ClaimRouteEvidenceReceipt(ctx, "route-worker-a", time.Minute)
	if err != nil || !claimed || first.LeaseGeneration != 1 || first.Attempts != 1 || first.Receipt.ReceiptID != receipt.ReceiptID {
		t.Fatalf("first claim=%+v claimed=%t err=%v", first, claimed, err)
	}
	if _, claimed, err := store.ClaimRouteEvidenceReceipt(ctx, "route-worker-b", time.Minute); err != nil || claimed {
		t.Fatalf("live lease was claimed again: claimed=%t err=%v", claimed, err)
	}
	if ok, err := store.AckRouteEvidenceReceipt(ctx, receipt.ReceiptID, "route-worker-a", 2); err != nil || ok {
		t.Fatalf("wrong generation ack ok=%t err=%v", ok, err)
	}
	if ok, err := store.RetryRouteEvidenceReceipt(ctx, receipt.ReceiptID, "route-worker-a", first.LeaseGeneration, time.Now().UTC(), "exporter.timeout"); err != nil || !ok {
		t.Fatalf("retry ok=%t err=%v", ok, err)
	}
	second, claimed, err := store.ClaimRouteEvidenceReceipt(ctx, "route-worker-b", time.Minute)
	if err != nil || !claimed || second.LeaseGeneration != 2 || second.Attempts != 2 {
		t.Fatalf("retry claim=%+v claimed=%t err=%v", second, claimed, err)
	}
	if ok, err := store.AckRouteEvidenceReceipt(ctx, receipt.ReceiptID, "route-worker-a", first.LeaseGeneration); err != nil || ok {
		t.Fatalf("stale ack ok=%t err=%v", ok, err)
	}
	if ok, err := store.AckRouteEvidenceReceipt(ctx, receipt.ReceiptID, "route-worker-b", second.LeaseGeneration); err != nil || !ok {
		t.Fatalf("current ack ok=%t err=%v", ok, err)
	}
	if _, claimed, err := store.ClaimRouteEvidenceReceipt(ctx, "route-worker-c", time.Minute); err != nil || claimed {
		t.Fatalf("delivered receipt was claimed: claimed=%t err=%v", claimed, err)
	}
}

func TestSQLRouteEvidenceOutboxExpiredLeaseRejectsStaleAck(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	receipt, _, err := store.CreateOrLoadRouteEvidenceReceipt(ctx, routeEvidenceReceiptInput(8, 9))
	if err != nil {
		t.Fatal(err)
	}
	first, claimed, err := store.ClaimRouteEvidenceReceipt(ctx, "route-worker-a", time.Hour)
	if err != nil || !claimed {
		t.Fatalf("first claim=%+v claimed=%t err=%v", first, claimed, err)
	}
	if _, err := store.db.ExecContext(ctx, "UPDATE route_evidence_outbox SET lease_expires_at = 0 WHERE receipt_id = ?", receipt.ReceiptID); err != nil {
		t.Fatal(err)
	}
	second, claimed, err := store.ClaimRouteEvidenceReceipt(ctx, "route-worker-b", time.Hour)
	if err != nil || !claimed || second.LeaseGeneration != first.LeaseGeneration+1 {
		t.Fatalf("expired lease claim=%+v claimed=%t err=%v", second, claimed, err)
	}
	if ok, err := store.AckRouteEvidenceReceipt(ctx, receipt.ReceiptID, "route-worker-a", first.LeaseGeneration); err != nil || ok {
		t.Fatalf("expired stale ack ok=%t err=%v", ok, err)
	}
}

func TestSQLRouteEvidenceOutboxConcurrentClaimHasOneWinner(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	if _, _, err := store.CreateOrLoadRouteEvidenceReceipt(ctx, routeEvidenceReceiptInput(8, 9)); err != nil {
		t.Fatal(err)
	}
	const workers = 8
	start := make(chan struct{})
	errs := make(chan error, workers)
	claims := make(chan RouteEvidenceOutboxClaim, workers)
	var wg sync.WaitGroup
	for index := 0; index < workers; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			claim, claimed, err := store.ClaimRouteEvidenceReceipt(ctx, fmt.Sprintf("route-worker-%d", index), time.Minute)
			if err != nil {
				errs <- err
				return
			}
			if claimed {
				claims <- claim
			}
		}(index)
	}
	close(start)
	wg.Wait()
	close(errs)
	close(claims)
	for err := range errs {
		t.Errorf("concurrent claim: %v", err)
	}
	var got []RouteEvidenceOutboxClaim
	for claim := range claims {
		got = append(got, claim)
	}
	if len(got) != 1 || got[0].LeaseGeneration != 1 || got[0].Attempts != 1 {
		t.Fatalf("concurrent claims=%+v", got)
	}
}

func TestSQLRouteEvidenceReceiptConcurrentCreateOrLoadConverges(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/route-evidence-concurrent.db"
	firstDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = firstDB.Close() })
	first, err := OpenSQLSessionStore(ctx, firstDB, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	secondDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondDB.Close() })
	second, err := OpenSQLSessionStore(ctx, secondDB, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	stores := []*SQLSessionStore{first, second}
	const writers = 8
	start := make(chan struct{})
	results := make(chan struct {
		receipt RouteEvidenceReceipt
		created bool
		err     error
	}, writers)
	var wg sync.WaitGroup
	for index := 0; index < writers; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			receipt, created, err := stores[index%len(stores)].CreateOrLoadRouteEvidenceReceipt(ctx, routeEvidenceReceiptInput(8, 9))
			results <- struct {
				receipt RouteEvidenceReceipt
				created bool
				err     error
			}{receipt, created, err}
		}(index)
	}
	close(start)
	wg.Wait()
	close(results)
	var created int
	var receiptID string
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent create/load: %v", result.err)
		}
		if result.created {
			created++
		}
		if receiptID == "" {
			receiptID = result.receipt.ReceiptID
		} else if result.receipt.ReceiptID != receiptID {
			t.Fatalf("receipt ids diverged: %q and %q", receiptID, result.receipt.ReceiptID)
		}
	}
	if created != 1 {
		t.Fatalf("created=%d, want one immutable receipt", created)
	}
	var receipts, outbox int
	if err := firstDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM route_evidence_receipts").Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("receipt count=%d err=%v", receipts, err)
	}
	if err := firstDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM route_evidence_outbox").Scan(&outbox); err != nil || outbox != 1 {
		t.Fatalf("outbox count=%d err=%v", outbox, err)
	}
}

func TestSQLRouteEvidenceTerminalCandidatesAreDiscoveryOnly(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	control, err := NewSQLRunControlStore(store.db, SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	for _, runID := range []string{"route-candidate-a", "route-candidate-b"} {
		if err := control.CreateRun(ctx, RunRecord{RunID: runID, SessionID: "route-candidate-session-" + runID, TenantID: "tenant-a", SubjectID: "subject-a"}); err != nil {
			t.Fatal(err)
		}
		if err := control.FinishRun(ctx, runID, core.RunCompleted, ""); err != nil {
			t.Fatal(err)
		}
	}
	page, next, err := store.ListTerminalRouteEvidenceCandidates(ctx, RouteEvidenceTerminalCursor{}, 1)
	if err != nil || len(page) != 1 || next.RunID != page[0].RunID || page[0].Status != string(core.RunCompleted) {
		t.Fatalf("first candidates=%+v next=%+v err=%v", page, next, err)
	}
	page, _, err = store.ListTerminalRouteEvidenceCandidates(ctx, next, 2)
	if err != nil || len(page) != 1 {
		t.Fatalf("second candidates=%+v err=%v", page, err)
	}
	payload, err := json.Marshal(map[string]string{"programmatic.route.identity.status": "unavailable", "run.id": page[0].RunID, "run.status": page[0].Status, "session.id": page[0].SessionID})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateOrLoadRouteEvidenceReceipt(ctx, RouteEvidenceReceiptInput{
		Protocol: RouteEvidenceReceiptProtocol, SessionID: page[0].SessionID, RunID: page[0].RunID,
		TerminalEventSeq: 5, SourceSessionVersion: 6, TerminalStatus: page[0].Status,
		Payload: payload, PayloadSHA256: routeEvidencePayloadDigest(payload),
	}); err != nil {
		t.Fatal(err)
	}
	all, _, err := store.ListTerminalRouteEvidenceCandidates(ctx, RouteEvidenceTerminalCursor{}, 2)
	if err != nil || len(all) != 1 {
		t.Fatalf("receipt did not suppress discovery candidate: candidates=%+v err=%v", all, err)
	}
}

func TestSQLSchemaV49RouteEvidenceOutboxMigration(t *testing.T) {
	store := newTestSQLStore(t)
	ctx := context.Background()
	for _, statement := range []string{
		"DROP TABLE route_evidence_outbox",
		"DROP TABLE route_evidence_receipts",
		"UPDATE store_meta SET value = '48' WHERE key = 'schema_version'",
	} {
		if _, err := store.db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := OpenSQLSessionStore(ctx, store.db, SQLDialectSQLite); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"route_evidence_receipts", "route_evidence_outbox"} {
		var count int
		if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&count); err != nil || count != 1 {
			t.Fatalf("migrated table %s count=%d err=%v", table, count, err)
		}
	}
	var version string
	if err := store.db.QueryRowContext(ctx, "SELECT value FROM store_meta WHERE key = 'schema_version'").Scan(&version); err != nil || version != "50" {
		t.Fatalf("schema version=%q err=%v", version, err)
	}
}

func TestPostgresRouteEvidenceOutbox(t *testing.T) {
	db := newPostgresTestDB(t)
	store, err := OpenSQLSessionStore(context.Background(), db, SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	receipt, created, err := store.CreateOrLoadRouteEvidenceReceipt(context.Background(), routeEvidenceReceiptInput(8, 9))
	if err != nil || !created {
		t.Fatalf("create receipt=%+v created=%t err=%v", receipt, created, err)
	}
	claim, claimed, err := store.ClaimRouteEvidenceReceipt(context.Background(), "route-worker-pg", time.Minute)
	if err != nil || !claimed || claim.LeaseGeneration != 1 {
		t.Fatalf("claim=%+v claimed=%t err=%v", claim, claimed, err)
	}
	if ok, err := store.AckRouteEvidenceReceipt(context.Background(), receipt.ReceiptID, claim.WorkerID, claim.LeaseGeneration); err != nil || !ok {
		t.Fatalf("ack ok=%t err=%v", ok, err)
	}
}

func routeEvidencePayloadDigest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
