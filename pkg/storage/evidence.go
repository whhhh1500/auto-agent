package storage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

var ErrEvidenceCursorInvalid = errors.New("invalid evidence cursor")

const evidenceCursorVersion = 1

const MaxEvidenceCursorBytes = 4096

const MaxEvidenceRecords = 8192

const runEvidenceSidecarVersion = 2

// EvidenceKind identifies one durable object participating in Composition or
// Assignment correlation.
type EvidenceKind string

type AssignmentVariant string

const (
	EvidenceRun        EvidenceKind = "run"
	EvidenceBacktest   EvidenceKind = "backtest"
	EvidenceEvaluation EvidenceKind = "evaluation"
	EvidenceCanary     EvidenceKind = "canary"
	EvidenceRelease    EvidenceKind = "release"

	AssignmentCandidate  AssignmentVariant = "candidate"
	AssignmentLive       AssignmentVariant = "live"
	AssignmentUnassigned AssignmentVariant = "unassigned"
)

// EvidenceRecord is a bounded cross-object audit reference. Canonical event,
// Evaluation, Canary and Release payloads remain in their owning stores.
type EvidenceRecord struct {
	Kind                 EvidenceKind      `json:"kind"`
	ID                   string            `json:"id"`
	SessionID            string            `json:"session_id,omitempty"`
	SegmentSeq           int64             `json:"segment_seq,omitempty"`
	ProfileID            string            `json:"profile_id,omitempty"`
	TenantID             string            `json:"tenant_id,omitempty"`
	SubjectID            string            `json:"subject_id,omitempty"`
	Scope                string            `json:"scope,omitempty"`
	CompositionRevision  string            `json:"composition_revision,omitempty"`
	AssignmentRevision   string            `json:"assignment_revision,omitempty"`
	AssignmentVariant    AssignmentVariant `json:"assignment_variant,omitempty"`
	ArtifactRevision     string            `json:"artifact_revision,omitempty"`
	Status               string            `json:"status,omitempty"`
	RelatedEvaluationRun string            `json:"related_evaluation_run_id,omitempty"`
	RelatedCanaryID      string            `json:"related_canary_id,omitempty"`
	RolledBack           bool              `json:"rolled_back,omitempty"`
	Version              int               `json:"version,omitempty"`
	CreatedAt            time.Time         `json:"created_at"`
}

// EvidenceQuery is the cross-object, bounded query contract.
type EvidenceQuery struct {
	CompositionRevision string
	AssignmentRevision  string
	TenantID            string
	SubjectID           string
	ProfileID           string
	Kinds               []EvidenceKind
	Statuses            []string
	CreatedAfter        time.Time
	CreatedBefore       time.Time
	Limit               int
	Cursor              string
}

func (q EvidenceQuery) Validate() error {
	if _, err := normalizeEvidenceKinds(q.Kinds); err != nil {
		return err
	}
	if _, err := normalizeEvidenceStatuses(q.Statuses); err != nil {
		return err
	}
	if !q.CreatedAfter.IsZero() && !q.CreatedBefore.IsZero() && q.CreatedAfter.After(q.CreatedBefore) {
		return fmt.Errorf("evidence created_after must not be after created_before")
	}
	for name, value := range map[string]string{
		"composition revision": q.CompositionRevision,
		"assignment revision":  q.AssignmentRevision,
	} {
		if value == "" {
			continue
		}
		if len(value) != 64 {
			return fmt.Errorf("evidence %s must be a SHA-256 digest", name)
		}
		for _, char := range value {
			if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
				return fmt.Errorf("evidence %s must be hexadecimal", name)
			}
		}
	}
	for name, value := range map[string]string{
		"tenant_id":  q.TenantID,
		"subject_id": q.SubjectID,
		"profile_id": q.ProfileID,
	} {
		if err := validateSQLTextFilter("evidence "+name, value); err != nil {
			return err
		}
	}
	if q.Limit < 0 || q.Limit > 500 {
		return fmt.Errorf("evidence limit must be between 0 and 500")
	}
	if len(q.Cursor) > MaxEvidenceCursorBytes {
		return fmt.Errorf("%w: too large", ErrEvidenceCursorInvalid)
	}
	return nil
}

func normalizeEvidenceStatuses(values []string) ([]string, error) {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if len(value) > 64 {
			return nil, fmt.Errorf("evidence status %q is too long", value)
		}
		for _, char := range value {
			if !((char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || strings.ContainsRune("._:-", char)) {
				return nil, fmt.Errorf("evidence status %q contains invalid characters", value)
			}
		}
		if !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	if len(out) > 16 {
		return nil, fmt.Errorf("evidence status filter exceeds 16 values")
	}
	sort.Strings(out)
	return out, nil
}

func evidenceStatusEnabled(query EvidenceQuery, status string) bool {
	if len(query.Statuses) == 0 {
		return true
	}
	status = strings.ToLower(strings.TrimSpace(status))
	for _, selected := range query.Statuses {
		if strings.ToLower(strings.TrimSpace(selected)) == status {
			return true
		}
	}
	return false
}

func normalizeEvidenceKinds(values []EvidenceKind) ([]EvidenceKind, error) {
	seen := map[EvidenceKind]bool{}
	out := []EvidenceKind{}
	for _, value := range values {
		switch value {
		case EvidenceRun, EvidenceBacktest, EvidenceEvaluation, EvidenceCanary, EvidenceRelease:
		default:
			return nil, fmt.Errorf("unknown evidence kind %q", value)
		}
		if !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func evidenceKindEnabled(query EvidenceQuery, kind EvidenceKind) bool {
	if len(query.Kinds) == 0 {
		return true
	}
	for _, selected := range query.Kinds {
		if selected == kind {
			return true
		}
	}
	return false
}

func selectedRunEvidenceKinds(query EvidenceQuery) []EvidenceKind {
	out := []EvidenceKind{}
	for _, kind := range []EvidenceKind{EvidenceRun, EvidenceBacktest} {
		if evidenceKindEnabled(query, kind) {
			out = append(out, kind)
		}
	}
	return out
}

type EvidencePage struct {
	Records    []EvidenceRecord `json:"evidence"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

type EvidenceVariantStats struct {
	Total          int64            `json:"total"`
	Terminal       int64            `json:"terminal"`
	Completed      int64            `json:"completed"`
	Failed         int64            `json:"failed"`
	CompletionRate float64          `json:"completion_rate"`
	ByStatus       map[string]int64 `json:"by_status"`
}

type RunEvidenceStats struct {
	Total          int64                           `json:"total"`
	Terminal       int64                           `json:"terminal"`
	Completed      int64                           `json:"completed"`
	Failed         int64                           `json:"failed"`
	CompletionRate float64                         `json:"completion_rate"`
	ByStatus       map[string]int64                `json:"by_status"`
	ByVariant      map[string]EvidenceVariantStats `json:"by_variant"`
}

type EvidenceStatsStore interface {
	RunEvidenceStats(ctx context.Context, query EvidenceQuery) (RunEvidenceStats, error)
}

// EvidencePager is optional so existing EvidenceStore integrations remain
// source-compatible while indexed stores opt into cursor pagination.
type EvidencePager interface {
	EvidenceStore
	QueryEvidencePage(ctx context.Context, query EvidenceQuery) (EvidencePage, error)
}

type evidenceCursor struct {
	Version     int            `json:"version"`
	Fingerprint string         `json:"fingerprint"`
	Offsets     map[string]int `json:"offsets"`
	SnapshotMS  int64          `json:"snapshot_millis"`
	Checksum    string         `json:"checksum"`
}

func evidenceQueryFingerprint(query EvidenceQuery) string {
	value := struct {
		CompositionRevision string
		AssignmentRevision  string
		TenantID            string
		SubjectID           string
		ProfileID           string
		Kinds               []EvidenceKind
		Statuses            []string
		CreatedAfterMS      int64
		CreatedBeforeMS     int64
		Limit               int
	}{query.CompositionRevision, query.AssignmentRevision, query.TenantID, query.SubjectID,
		query.ProfileID, func() []EvidenceKind { values, _ := normalizeEvidenceKinds(query.Kinds); return values }(),
		func() []string { values, _ := normalizeEvidenceStatuses(query.Statuses); return values }(),
		timeMillis(query.CreatedAfter), timeMillis(query.CreatedBefore), query.Limit}
	encoded, _ := json.Marshal(value)
	sum := sha256.Sum256(encoded)
	return fmt.Sprintf("%x", sum[:])
}

func encodeEvidenceCursor(query EvidenceQuery, offsets map[string]int, snapshotMillis int64) string {
	cursor := evidenceCursor{
		Version: evidenceCursorVersion, Fingerprint: evidenceQueryFingerprint(query), Offsets: offsets, SnapshotMS: snapshotMillis,
	}
	cursor.Checksum = evidenceCursorChecksum(cursor)
	payload, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(payload)
}

func evidenceCursorChecksum(cursor evidenceCursor) string {
	value := struct {
		Version     int
		Fingerprint string
		Offsets     map[string]int
		SnapshotMS  int64
	}{cursor.Version, cursor.Fingerprint, cursor.Offsets, cursor.SnapshotMS}
	encoded, _ := json.Marshal(value)
	sum := sha256.Sum256(append([]byte("harness-evidence-cursor-v1\x00"), encoded...))
	return fmt.Sprintf("%x", sum[:])
}

func decodeEvidenceCursor(query EvidenceQuery, encoded string) (map[string]int, int64, error) {
	if encoded == "" {
		return map[string]int{}, time.Now().UTC().UnixMilli(), nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: encoding", ErrEvidenceCursorInvalid)
	}
	var cursor evidenceCursor
	if json.Unmarshal(payload, &cursor) != nil || cursor.Version != evidenceCursorVersion ||
		cursor.Fingerprint != evidenceQueryFingerprint(query) || len(cursor.Offsets) > 8 ||
		cursor.SnapshotMS <= 0 || cursor.Checksum != evidenceCursorChecksum(cursor) {
		return nil, 0, ErrEvidenceCursorInvalid
	}
	for key, offset := range cursor.Offsets {
		if (key != "run" && key != "evaluation" && key != "canary" && key != "release") ||
			offset < 0 || offset > 1_000_000_000 {
			return nil, 0, ErrEvidenceCursorInvalid
		}
	}
	return cursor.Offsets, cursor.SnapshotMS, nil
}

func evidenceSource(kind EvidenceKind) string {
	if kind == EvidenceRun || kind == EvidenceBacktest {
		return "run"
	}
	return string(kind)
}

func sortEvidence(records []EvidenceRecord) {
	sort.SliceStable(records, func(i, j int) bool {
		left, right := records[i], records[j]
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.After(right.CreatedAt)
		}
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		if left.Kind == EvidenceRelease && (left.ProfileID != right.ProfileID || left.Version != right.Version) {
			if left.ProfileID != right.ProfileID {
				return left.ProfileID < right.ProfileID
			}
			return left.Version < right.Version
		}
		if left.ID != right.ID {
			return left.ID < right.ID
		}
		if left.SegmentSeq != right.SegmentSeq {
			return left.SegmentSeq < right.SegmentSeq
		}
		return left.SessionID < right.SessionID
	})
}

func paginateEvidenceRecords(query EvidenceQuery, sources map[string][]EvidenceRecord) (EvidencePage, error) {
	if err := query.Validate(); err != nil {
		return EvidencePage{}, err
	}
	offsets, snapshotMillis, err := decodeEvidenceCursor(query, query.Cursor)
	if err != nil {
		return EvidencePage{}, err
	}
	positioned := map[string][]EvidenceRecord{}
	for source, records := range sources {
		eligible := records[:0]
		for _, record := range records {
			if record.CreatedAt.UnixMilli() <= snapshotMillis {
				eligible = append(eligible, record)
			}
		}
		records = eligible
		offset := offsets[source]
		if offset > len(records) {
			offset = len(records)
		}
		positioned[source] = records[offset:]
	}
	return paginateEvidenceRecordsAtOffsets(query, positioned, offsets, snapshotMillis)
}

func paginateEvidenceRecordsAtOffsets(query EvidenceQuery, sources map[string][]EvidenceRecord, offsets map[string]int, snapshotMillis int64) (EvidencePage, error) {
	all := []EvidenceRecord{}
	for _, records := range sources {
		all = append(all, records...)
	}
	sortEvidence(all)
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}
	page := EvidencePage{Records: all}
	if len(page.Records) > limit {
		page.Records = page.Records[:limit]
	}
	if len(all) > len(page.Records) {
		consumed := map[string]int{}
		for _, record := range page.Records {
			consumed[evidenceSource(record.Kind)]++
		}
		nextOffsets := map[string]int{}
		for source, offset := range offsets {
			nextOffsets[source] = offset
		}
		for source, count := range consumed {
			nextOffsets[source] += count
		}
		page.NextCursor = encodeEvidenceCursor(query, nextOffsets, snapshotMillis)
	}
	return page, nil
}

// EvidenceStore is an optional indexed cross-object query surface.
type EvidenceStore interface {
	QueryEvidence(ctx context.Context, query EvidenceQuery) ([]EvidenceRecord, error)
}

type persistedRunEvidence struct {
	FormatVersion int              `json:"format_version"`
	Version       int64            `json:"version"`
	Records       []EvidenceRecord `json:"records"`
}

func deriveRunEvidence(sessionID string, header core.SessionOptions, events []core.SessionEvent) ([]EvidenceRecord, error) {
	kind := EvidenceRun
	if header.Metadata["backtest_of"] != "" {
		kind = EvidenceBacktest
	}
	out := []EvidenceRecord{}
	statuses := evidenceStatusTransitions(events)
	for _, event := range events {
		var composition *core.RunCompositionData
		var compositionRevision, assignmentRevision string
		switch event.Type {
		case core.EvRunStart:
			var data core.RunStartData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return nil, fmt.Errorf("decode run/start evidence: %w", err)
			}
			composition, compositionRevision, assignmentRevision = data.Composition, data.CompositionRevision, data.AssignmentRevision
		case core.EvRunResume:
			var data core.RunResumeData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return nil, fmt.Errorf("decode run/resume evidence: %w", err)
			}
			composition, compositionRevision, assignmentRevision = data.Composition, data.CompositionRevision, data.AssignmentRevision
		default:
			continue
		}
		if composition != nil {
			calculated, err := core.CompositionRevision(composition)
			if err != nil {
				return nil, err
			}
			if compositionRevision == "" {
				compositionRevision = calculated
			}
			calculatedAssignment, err := core.CompositionMetadataRevision(composition.Metadata)
			if err != nil {
				return nil, err
			}
			if assignmentRevision == "" {
				assignmentRevision = calculatedAssignment
			}
		}
		if compositionRevision == "" && assignmentRevision == "" {
			continue
		}
		profileID := header.ProfileID
		if composition != nil && composition.Profile.ProfileID != "" {
			profileID = composition.Profile.ProfileID
		}
		createdAt := event.Time
		if createdAt.IsZero() {
			createdAt = time.UnixMilli(0).UTC()
		}
		out = append(out, EvidenceRecord{
			Kind: kind, ID: event.RunID, SessionID: sessionID, SegmentSeq: event.Seq,
			ProfileID: profileID, TenantID: header.Principal.TenantID, SubjectID: header.Principal.SubjectID,
			CompositionRevision: compositionRevision, AssignmentRevision: assignmentRevision,
			AssignmentVariant: assignmentVariant(compositionMetadataValue(composition)),
			Status:            statuses[event.RunID], CreatedAt: createdAt.UTC(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SegmentSeq != out[j].SegmentSeq {
			return out[i].SegmentSeq < out[j].SegmentSeq
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func compositionMetadataValue(composition *core.RunCompositionData) map[string]string {
	if composition == nil {
		return nil
	}
	return composition.Metadata
}

func assignmentVariant(metadata map[string]string) AssignmentVariant {
	if metadata == nil {
		return AssignmentUnassigned
	}
	if value := strings.ToLower(strings.TrimSpace(metadata["harness.assignment.variant"])); value != "" {
		switch AssignmentVariant(value) {
		case AssignmentCandidate, AssignmentLive, AssignmentUnassigned:
			return AssignmentVariant(value)
		}
	}
	for _, key := range []string{"harness.canary.candidate", "harness.assignment.candidate"} {
		if raw, exists := metadata[key]; exists {
			if strings.EqualFold(strings.TrimSpace(raw), "true") {
				return AssignmentCandidate
			}
			return AssignmentLive
		}
	}
	return AssignmentUnassigned
}

func evidenceStatusTransitions(events []core.SessionEvent) map[string]string {
	statuses := map[string]string{}
	for _, event := range events {
		switch event.Type {
		case core.EvRunStart, core.EvRunResume, core.EvApprovalResolved:
			statuses[event.RunID] = "running"
		case core.EvApprovalRequested:
			statuses[event.RunID] = string(core.RunWaitingApproval)
		case core.EvRunEnd:
			var data core.RunEndData
			if json.Unmarshal(event.Data, &data) == nil {
				statuses[event.RunID] = string(data.Status)
			}
		}
	}
	return statuses
}

func newRunEvidenceStats() RunEvidenceStats {
	return RunEvidenceStats{ByStatus: map[string]int64{}, ByVariant: map[string]EvidenceVariantStats{}}
}

func addRunEvidenceStat(stats *RunEvidenceStats, variant AssignmentVariant, status string, count int64) {
	if stats == nil || count <= 0 {
		return
	}
	if variant == "" {
		variant = AssignmentUnassigned
	}
	status = strings.ToLower(strings.TrimSpace(status))
	if status == "" {
		status = "unknown"
	}
	stats.Total += count
	stats.ByStatus[status] += count
	variantStats := stats.ByVariant[string(variant)]
	if variantStats.ByStatus == nil {
		variantStats.ByStatus = map[string]int64{}
	}
	variantStats.Total += count
	variantStats.ByStatus[status] += count
	if isTerminalEvidenceStatus(status) {
		stats.Terminal += count
		variantStats.Terminal += count
	}
	if status == string(core.RunCompleted) {
		stats.Completed += count
		variantStats.Completed += count
	}
	if status == string(core.RunFailed) {
		stats.Failed += count
		variantStats.Failed += count
	}
	stats.ByVariant[string(variant)] = variantStats
}

func finalizeRunEvidenceStats(stats *RunEvidenceStats) {
	if stats == nil {
		return
	}
	if stats.Terminal > 0 {
		stats.CompletionRate = float64(stats.Completed) / float64(stats.Terminal)
	}
	for key, value := range stats.ByVariant {
		if value.Terminal > 0 {
			value.CompletionRate = float64(value.Completed) / float64(value.Terminal)
		}
		stats.ByVariant[key] = value
	}
}

func isTerminalEvidenceStatus(status string) bool {
	switch status {
	case string(core.RunCompleted), string(core.RunFailed), string(core.RunCancelled), string(core.RunLimited):
		return true
	default:
		return false
	}
}

func latestRunEvidence(records []EvidenceRecord) []EvidenceRecord {
	latest := map[string]EvidenceRecord{}
	for _, record := range records {
		if record.Kind != EvidenceRun && record.Kind != EvidenceBacktest {
			continue
		}
		key := record.SessionID + "\x00" + record.ID
		current, exists := latest[key]
		if !exists || record.SegmentSeq > current.SegmentSeq {
			latest[key] = record
		}
	}
	out := make([]EvidenceRecord, 0, len(latest))
	for _, record := range latest {
		out = append(out, record)
	}
	return out
}

func aggregateRunEvidenceStats(records []EvidenceRecord, query EvidenceQuery) (RunEvidenceStats, error) {
	if err := query.Validate(); err != nil {
		return RunEvidenceStats{}, err
	}
	stats := newRunEvidenceStats()
	for _, record := range filterRunEvidence(latestRunEvidence(records), query, len(records)+1) {
		addRunEvidenceStat(&stats, record.AssignmentVariant, record.Status, 1)
	}
	finalizeRunEvidenceStats(&stats)
	return stats, nil
}

func filterRunEvidence(records []EvidenceRecord, query EvidenceQuery, limit int) []EvidenceRecord {
	out := make([]EvidenceRecord, 0, len(records))
	for _, record := range records {
		if !evidenceKindEnabled(query, record.Kind) {
			continue
		}
		if !evidenceStatusEnabled(query, record.Status) {
			continue
		}
		if !query.CreatedAfter.IsZero() && record.CreatedAt.Before(query.CreatedAfter) {
			continue
		}
		if !query.CreatedBefore.IsZero() && record.CreatedAt.After(query.CreatedBefore) {
			continue
		}
		if query.CompositionRevision != "" && record.CompositionRevision != query.CompositionRevision {
			continue
		}
		if query.AssignmentRevision != "" && record.AssignmentRevision != query.AssignmentRevision {
			continue
		}
		if query.TenantID != "" && record.TenantID != query.TenantID {
			continue
		}
		if query.SubjectID != "" && record.SubjectID != query.SubjectID {
			continue
		}
		if query.ProfileID != "" && record.ProfileID != query.ProfileID {
			continue
		}
		out = append(out, record)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

var (
	sqlQueryRunEvidence = sqlQuery{`SELECT session_id, run_id, segment_seq, kind,
		tenant_id, subject_id, profile_id, composition_revision,
		assignment_revision, assignment_variant, status, created_at FROM run_evidence`}
	sqlQueryEvaluationEvidence = sqlQuery{`SELECT r.id, r.profile_id, r.tenant_id,
		r.subject_id, r.assignment_revision, r.status, r.created_at,
		COALESCE((SELECT c.composition_revision FROM evaluation_case_results c
			WHERE c.run_id = r.id AND c.composition_revision <> ''
			ORDER BY c.completed_at DESC, c.case_id DESC LIMIT 1), ''),
		r.metadata_json FROM evaluation_runs r`}
	sqlQueryCanaryEvidence = sqlQuery{`SELECT c.id, c.profile_id, c.scope, c.revision,
		c.status, c.created_at, c.candidate_evaluation_run_id,
		COALESCE(e.assignment_revision, ''), COALESCE(e.tenant_id, ''), COALESCE(e.subject_id, ''),
		COALESCE((SELECT cr.composition_revision FROM evaluation_case_results cr
			WHERE cr.run_id = c.candidate_evaluation_run_id AND cr.composition_revision <> ''
			ORDER BY cr.completed_at DESC, cr.case_id DESC LIMIT 1), '')
		FROM profile_canaries c LEFT JOIN evaluation_runs e
			ON e.id = c.candidate_evaluation_run_id`}
	sqlQueryReleaseEvidence = sqlQuery{`SELECT r.profile_id, r.version, r.operation_id,
		r.scope, r.revision, r.created_at, r.rolled_back,
		COALESCE(c.id, ''), COALESCE(e.id, ''), COALESCE(e.assignment_revision, ''),
		COALESCE(e.tenant_id, ''), COALESCE(e.subject_id, ''),
		COALESCE((SELECT cr.composition_revision FROM evaluation_case_results cr
			WHERE cr.run_id = e.id AND cr.composition_revision <> ''
			ORDER BY cr.completed_at DESC, cr.case_id DESC LIMIT 1), '')
		FROM profile_releases r LEFT JOIN profile_canaries c
			ON c.id = r.operation_id LEFT JOIN evaluation_runs e
			ON e.id = c.candidate_evaluation_run_id`}
)

func (s *SQLSessionStore) QueryEvidence(ctx context.Context, query EvidenceQuery) ([]EvidenceRecord, error) {
	query.Cursor = ""
	page, err := s.QueryEvidencePage(ctx, query)
	return page.Records, err
}

func (s *SQLSessionStore) QueryEvidencePage(ctx context.Context, query EvidenceQuery) (EvidencePage, error) {
	if err := query.Validate(); err != nil {
		return EvidencePage{}, err
	}
	offsets, snapshotMillis, err := decodeEvidenceCursor(query, query.Cursor)
	if err != nil {
		return EvidencePage{}, err
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	sources := map[string][]EvidenceRecord{}
	if runKinds := selectedRunEvidenceKinds(query); len(runKinds) > 0 {
		if sources["run"], err = s.queryRunEvidence(ctx, query, runKinds, limit+1, offsets["run"], snapshotMillis); err != nil {
			return EvidencePage{}, err
		}
	}
	if evidenceKindEnabled(query, EvidenceEvaluation) {
		if sources["evaluation"], err = s.queryEvaluationEvidence(ctx, query, limit+1, offsets["evaluation"], snapshotMillis); err != nil {
			return EvidencePage{}, err
		}
	}
	if evidenceKindEnabled(query, EvidenceCanary) {
		if sources["canary"], err = s.queryCanaryEvidence(ctx, query, limit+1, offsets["canary"], snapshotMillis); err != nil {
			return EvidencePage{}, err
		}
	}
	if evidenceKindEnabled(query, EvidenceRelease) {
		if sources["release"], err = s.queryReleaseEvidence(ctx, query, limit+1, offsets["release"], snapshotMillis); err != nil {
			return EvidencePage{}, err
		}
	}
	return paginateEvidenceRecordsAtOffsets(query, sources, offsets, snapshotMillis)
}

func (s *SQLSessionStore) queryRunEvidence(ctx context.Context, query EvidenceQuery, kinds []EvidenceKind, limit, offset int, snapshotMillis int64) ([]EvidenceRecord, error) {
	text, args := sqlQueryRunEvidence.text, []any{}
	where := []string{}
	where = append(where, "created_at <= ?")
	args = append(args, snapshotMillis)
	if len(kinds) > 0 {
		placeholders := make([]string, 0, len(kinds))
		for _, kind := range kinds {
			placeholders = append(placeholders, "?")
			args = append(args, string(kind))
		}
		where = append(where, "kind IN ("+strings.Join(placeholders, ",")+")")
	}
	if query.CompositionRevision != "" {
		where = append(where, "composition_revision = ?")
		args = append(args, query.CompositionRevision)
	}
	if query.AssignmentRevision != "" {
		where = append(where, "assignment_revision = ?")
		args = append(args, query.AssignmentRevision)
	}
	if query.TenantID != "" {
		where = append(where, "tenant_id = ?")
		args = append(args, query.TenantID)
	}
	if query.SubjectID != "" {
		where = append(where, "subject_id = ?")
		args = append(args, query.SubjectID)
	}
	if query.ProfileID != "" {
		where = append(where, "profile_id = ?")
		args = append(args, query.ProfileID)
	}
	appendEvidenceTimeWhere(&where, &args, "created_at", query)
	appendEvidenceStatusWhere(&where, &args, "status", query)
	if len(where) > 0 {
		text += " WHERE " + strings.Join(where, " AND ")
	}
	text += " ORDER BY created_at DESC, kind ASC, run_id ASC, segment_seq ASC, session_id ASC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, (sqlQuery{text}).bind(s.dialect), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []EvidenceRecord{}
	for rows.Next() {
		var record EvidenceRecord
		var kind string
		var createdMillis int64
		if err := rows.Scan(&record.SessionID, &record.ID, &record.SegmentSeq, &kind,
			&record.TenantID, &record.SubjectID, &record.ProfileID,
			&record.CompositionRevision, &record.AssignmentRevision, &record.AssignmentVariant,
			&record.Status, &createdMillis); err != nil {
			return nil, err
		}
		record.Kind = EvidenceKind(kind)
		record.CreatedAt = time.UnixMilli(createdMillis).UTC()
		out = append(out, record)
	}
	return out, rows.Err()
}

func (s *SQLSessionStore) RunEvidenceStats(ctx context.Context, query EvidenceQuery) (RunEvidenceStats, error) {
	query.Cursor = ""
	if err := query.Validate(); err != nil {
		return RunEvidenceStats{}, err
	}
	runKinds := selectedRunEvidenceKinds(query)
	stats := newRunEvidenceStats()
	if len(runKinds) == 0 {
		return stats, nil
	}
	where := []string{`NOT EXISTS (
		SELECT 1 FROM run_evidence newer
		WHERE newer.session_id = current.session_id
		  AND newer.run_id = current.run_id
		  AND newer.segment_seq > current.segment_seq)`}
	args := []any{}
	placeholders := make([]string, 0, len(runKinds))
	for _, kind := range runKinds {
		placeholders = append(placeholders, "?")
		args = append(args, string(kind))
	}
	where = append(where, "current.kind IN ("+strings.Join(placeholders, ",")+")")
	if query.CompositionRevision != "" {
		where = append(where, "current.composition_revision = ?")
		args = append(args, query.CompositionRevision)
	}
	if query.AssignmentRevision != "" {
		where = append(where, "current.assignment_revision = ?")
		args = append(args, query.AssignmentRevision)
	}
	if query.TenantID != "" {
		where = append(where, "current.tenant_id = ?")
		args = append(args, query.TenantID)
	}
	if query.SubjectID != "" {
		where = append(where, "current.subject_id = ?")
		args = append(args, query.SubjectID)
	}
	if query.ProfileID != "" {
		where = append(where, "current.profile_id = ?")
		args = append(args, query.ProfileID)
	}
	appendEvidenceTimeWhere(&where, &args, "current.created_at", query)
	appendEvidenceStatusWhere(&where, &args, "current.status", query)
	text := `SELECT current.assignment_variant, current.status, COUNT(*)
		FROM run_evidence current WHERE ` + strings.Join(where, " AND ") + `
		GROUP BY current.assignment_variant, current.status`
	rows, err := s.db.QueryContext(ctx, (sqlQuery{text}).bind(s.dialect), args...)
	if err != nil {
		return RunEvidenceStats{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var variant AssignmentVariant
		var status string
		var count int64
		if err := rows.Scan(&variant, &status, &count); err != nil {
			return RunEvidenceStats{}, err
		}
		addRunEvidenceStat(&stats, variant, status, count)
	}
	if err := rows.Err(); err != nil {
		return RunEvidenceStats{}, err
	}
	finalizeRunEvidenceStats(&stats)
	return stats, nil
}

func (s *SQLSessionStore) queryEvaluationEvidence(ctx context.Context, query EvidenceQuery, limit, offset int, snapshotMillis int64) ([]EvidenceRecord, error) {
	text, args := sqlQueryEvaluationEvidence.text, []any{}
	where := []string{}
	where = append(where, "r.created_at <= ?")
	args = append(args, snapshotMillis)
	if query.TenantID != "" {
		where = append(where, "r.tenant_id = ?")
		args = append(args, query.TenantID)
	}
	if query.SubjectID != "" {
		where = append(where, "r.subject_id = ?")
		args = append(args, query.SubjectID)
	}
	if query.ProfileID != "" {
		where = append(where, "r.profile_id = ?")
		args = append(args, query.ProfileID)
	}
	if query.CompositionRevision != "" {
		where = append(where, "EXISTS (SELECT 1 FROM evaluation_case_results cx WHERE cx.run_id = r.id AND cx.composition_revision = ?)")
		args = append(args, query.CompositionRevision)
	}
	if query.AssignmentRevision != "" {
		where = append(where, "(r.assignment_revision = ? OR EXISTS (SELECT 1 FROM evaluation_case_results ax WHERE ax.run_id = r.id AND ax.assignment_revision = ?))")
		args = append(args, query.AssignmentRevision, query.AssignmentRevision)
	}
	appendEvidenceTimeWhere(&where, &args, "r.created_at", query)
	appendEvidenceStatusWhere(&where, &args, "r.status", query)
	if len(where) > 0 {
		text += " WHERE " + strings.Join(where, " AND ")
	}
	text += " ORDER BY r.created_at DESC, r.id ASC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, (sqlQuery{text}).bind(s.dialect), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []EvidenceRecord{}
	for rows.Next() {
		var record EvidenceRecord
		var assignmentRevision, status, compositionRevision, metadataJSON string
		var createdMillis int64
		if err := rows.Scan(&record.ID, &record.ProfileID, &record.TenantID, &record.SubjectID,
			&assignmentRevision, &status, &createdMillis, &compositionRevision, &metadataJSON); err != nil {
			return nil, err
		}
		record.Kind, record.AssignmentRevision = EvidenceEvaluation, assignmentRevision
		record.CompositionRevision, record.Status = compositionRevision, status
		record.RelatedEvaluationRun = record.ID
		record.CreatedAt = time.UnixMilli(createdMillis).UTC()
		var metadata map[string]string
		if metadataJSON != "" {
			_ = json.Unmarshal([]byte(metadataJSON), &metadata)
			record.Scope = metadata["evaluation.principal_scope"]
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

func (s *SQLSessionStore) queryCanaryEvidence(ctx context.Context, query EvidenceQuery, limit, offset int, snapshotMillis int64) ([]EvidenceRecord, error) {
	text, args := sqlQueryCanaryEvidence.text, []any{}
	where := []string{}
	where = append(where, "c.created_at <= ?")
	args = append(args, snapshotMillis)
	if query.ProfileID != "" {
		where = append(where, "c.profile_id = ?")
		args = append(args, query.ProfileID)
	}
	if query.CompositionRevision != "" {
		where = append(where, "EXISTS (SELECT 1 FROM evaluation_case_results cx WHERE cx.run_id = c.candidate_evaluation_run_id AND cx.composition_revision = ?)")
		args = append(args, query.CompositionRevision)
	}
	if query.AssignmentRevision != "" {
		where = append(where, "(e.assignment_revision = ? OR EXISTS (SELECT 1 FROM evaluation_case_results ax WHERE ax.run_id = c.candidate_evaluation_run_id AND ax.assignment_revision = ?))")
		args = append(args, query.AssignmentRevision, query.AssignmentRevision)
	}
	if query.TenantID != "" {
		where = append(where, "(e.tenant_id = ? OR e.tenant_id IS NULL)")
		args = append(args, query.TenantID)
	}
	if query.SubjectID != "" {
		where = append(where, "(e.subject_id = ? OR e.subject_id IS NULL)")
		args = append(args, query.SubjectID)
	}
	appendEvidenceTimeWhere(&where, &args, "c.created_at", query)
	appendEvidenceStatusWhere(&where, &args, "c.status", query)
	if len(where) > 0 {
		text += " WHERE " + strings.Join(where, " AND ")
	}
	text += " ORDER BY c.created_at DESC, c.id ASC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, (sqlQuery{text}).bind(s.dialect), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []EvidenceRecord{}
	for rows.Next() {
		var record EvidenceRecord
		var createdMillis int64
		var candidateEvaluationRunID, assignmentRevision, tenantID, subjectID, compositionRevision string
		if err := rows.Scan(&record.ID, &record.ProfileID, &record.Scope, &record.ArtifactRevision,
			&record.Status, &createdMillis, &candidateEvaluationRunID, &assignmentRevision, &tenantID, &subjectID, &compositionRevision); err != nil {
			return nil, err
		}
		record.Kind, record.AssignmentRevision = EvidenceCanary, assignmentRevision
		record.CompositionRevision = compositionRevision
		record.RelatedEvaluationRun = candidateEvaluationRunID
		record.TenantID, record.SubjectID = tenantID, subjectID
		record.CreatedAt = time.UnixMilli(createdMillis).UTC()
		out = append(out, record)
	}
	return out, rows.Err()
}

func (s *SQLSessionStore) queryReleaseEvidence(ctx context.Context, query EvidenceQuery, limit, offset int, snapshotMillis int64) ([]EvidenceRecord, error) {
	text, args := sqlQueryReleaseEvidence.text, []any{}
	where := []string{}
	where = append(where, "r.created_at <= ?")
	args = append(args, snapshotMillis)
	if query.ProfileID != "" {
		where = append(where, "r.profile_id = ?")
		args = append(args, query.ProfileID)
	}
	if query.TenantID != "" {
		where = append(where, "(e.tenant_id = ? OR e.tenant_id IS NULL)")
		args = append(args, query.TenantID)
	}
	if query.SubjectID != "" {
		where = append(where, "(e.subject_id = ? OR e.subject_id IS NULL)")
		args = append(args, query.SubjectID)
	}
	if query.CompositionRevision != "" {
		where = append(where, "EXISTS (SELECT 1 FROM evaluation_case_results cx WHERE cx.run_id = e.id AND cx.composition_revision = ?)")
		args = append(args, query.CompositionRevision)
	}
	if query.AssignmentRevision != "" {
		where = append(where, "(e.assignment_revision = ? OR EXISTS (SELECT 1 FROM evaluation_case_results ax WHERE ax.run_id = e.id AND ax.assignment_revision = ?))")
		args = append(args, query.AssignmentRevision, query.AssignmentRevision)
	}
	appendEvidenceTimeWhere(&where, &args, "r.created_at", query)
	appendEvidenceStatusWhere(&where, &args, "CASE WHEN r.rolled_back = 1 THEN 'rolled_back' ELSE 'active' END", query)
	if len(where) > 0 {
		text += " WHERE " + strings.Join(where, " AND ")
	}
	text += " ORDER BY r.created_at DESC, r.profile_id ASC, r.version ASC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, (sqlQuery{text}).bind(s.dialect), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []EvidenceRecord{}
	for rows.Next() {
		var record EvidenceRecord
		var version int
		var operationID, canaryID, evaluationID, assignmentRevision, tenantID, subjectID, compositionRevision string
		var createdMillis int64
		var rolledBack int
		if err := rows.Scan(&record.ProfileID, &version, &operationID, &record.Scope,
			&record.ArtifactRevision, &createdMillis, &rolledBack, &canaryID, &evaluationID,
			&assignmentRevision, &tenantID, &subjectID, &compositionRevision); err != nil {
			return nil, err
		}
		record.Kind, record.ID = EvidenceRelease, fmt.Sprintf("%s:v%d", record.ProfileID, version)
		record.Version = version
		record.RelatedCanaryID, record.RelatedEvaluationRun = canaryID, evaluationID
		record.TenantID, record.SubjectID = tenantID, subjectID
		record.AssignmentRevision, record.CompositionRevision = assignmentRevision, compositionRevision
		record.RolledBack = rolledBack != 0
		if record.RolledBack {
			record.Status = "rolled_back"
		} else {
			record.Status = "active"
		}
		record.CreatedAt = time.UnixMilli(createdMillis).UTC()
		out = append(out, record)
	}
	return out, rows.Err()
}

func appendEvidenceTimeWhere(where *[]string, args *[]any, expression string, query EvidenceQuery) {
	if !query.CreatedAfter.IsZero() {
		*where = append(*where, expression+" >= ?")
		*args = append(*args, query.CreatedAfter.UTC().UnixMilli())
	}
	if !query.CreatedBefore.IsZero() {
		*where = append(*where, expression+" <= ?")
		*args = append(*args, query.CreatedBefore.UTC().UnixMilli())
	}
}

func appendEvidenceStatusWhere(where *[]string, args *[]any, expression string, query EvidenceQuery) {
	statuses, _ := normalizeEvidenceStatuses(query.Statuses)
	if len(statuses) == 0 {
		return
	}
	placeholders := make([]string, 0, len(statuses))
	for _, status := range statuses {
		placeholders = append(placeholders, "?")
		*args = append(*args, status)
	}
	*where = append(*where, expression+" IN ("+strings.Join(placeholders, ",")+")")
}

var _ EvidenceStore = (*SQLSessionStore)(nil)
var _ EvidencePager = (*SQLSessionStore)(nil)
var _ EvidenceStatsStore = (*SQLSessionStore)(nil)
