package execution

import (
	"context"
	"sync"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

const MaxLibraryObservations = 8192

// ToolLibraryObservation is one disclosure-flow record: query, explore, or
// choose. It is intentionally schema-free of full tool bodies so a later
// ranking/eval pass can read the path without storing prompts.
type ToolLibraryObservation struct {
	Time     time.Time `json:"time"`
	Action   string    `json:"action"`
	Query    string    `json:"query,omitempty"`
	ToolID   string    `json:"tool_id,omitempty"`
	Library  string    `json:"library,omitempty"`
	Name     string    `json:"name,omitempty"`
	HitIDs   []string  `json:"hit_ids,omitempty"`
	Hits     int       `json:"hits"`
	Explore  bool      `json:"explore,omitempty"`
	CallID   string    `json:"call_id,omitempty"`
	Scope    string    `json:"scope,omitempty"`
	TenantID string    `json:"tenant_id,omitempty"`
}

// ToolLibraryObserver records disclosure-flow observations. Implementations
// must be best-effort: a failure must not fail the tool call.
type ToolLibraryObserver interface {
	Observe(ctx context.Context, observation ToolLibraryObservation) error
}

// MemoryLibraryObserver is the in-process ring buffer used by tests and the
// embedded demo. Oldest records are dropped at MaxLibraryObservations.
type MemoryLibraryObserver struct {
	mu      sync.Mutex
	max     int
	records []ToolLibraryObservation
}

func NewMemoryLibraryObserver() *MemoryLibraryObserver {
	return &MemoryLibraryObserver{max: MaxLibraryObservations}
}

func (o *MemoryLibraryObserver) Observe(_ context.Context, observation ToolLibraryObservation) error {
	if o == nil {
		return nil
	}
	if observation.Time.IsZero() {
		observation.Time = time.Now().UTC()
	}
	if len(observation.HitIDs) > maxMCPSearchTopK {
		observation.HitIDs = append([]string(nil), observation.HitIDs[:maxMCPSearchTopK]...)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	limit := o.max
	if limit <= 0 {
		limit = MaxLibraryObservations
	}
	if len(o.records) >= limit {
		o.records = o.records[1:]
	}
	o.records = append(o.records, observation)
	return nil
}

func (o *MemoryLibraryObserver) Snapshot() []ToolLibraryObservation {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]ToolLibraryObservation, len(o.records))
	copy(out, o.records)
	return out
}

func (p *mcpGateway) observe(ctx context.Context, request core.CapabilityRequest, observation ToolLibraryObservation) {
	if observation.Time.IsZero() {
		observation.Time = time.Now().UTC()
	}
	observation.CallID = request.CallID
	observation.Scope = request.Context.Scope.String()
	observation.TenantID = request.Context.Principal.TenantID
	if p == nil || p.observer == nil {
		return
	}
	_ = p.observer.Observe(ctx, observation)
}

func libraryMetadata(observation ToolLibraryObservation) map[string]any {
	meta := map[string]any{
		"library.action": observation.Action,
		"library.hits":   observation.Hits,
	}
	if observation.Query != "" {
		meta["library.query"] = observation.Query
	}
	if observation.ToolID != "" {
		meta["library.id"] = observation.ToolID
	}
	if observation.Library != "" {
		meta["library.library"] = observation.Library
	}
	if observation.Explore {
		meta["library.explore"] = true
	}
	if len(observation.HitIDs) > 0 {
		ids := observation.HitIDs
		if len(ids) > 8 {
			ids = ids[:8]
		}
		meta["library.hit_ids"] = ids
	}
	return meta
}
