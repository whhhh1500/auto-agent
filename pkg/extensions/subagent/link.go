package subagent

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Link records the stable relationship between one guarded parent tool call
// and its child session/run. It intentionally contains no prompt or tool args.
type Link struct {
	ParentSessionID string    `json:"parent_session_id"`
	ParentRunID     string    `json:"parent_run_id"`
	ParentCallID    string    `json:"parent_call_id"`
	ChildSessionID  string    `json:"child_session_id"`
	ChildRunID      string    `json:"child_run_id"`
	TenantID        string    `json:"tenant_id"`
	SubjectID       string    `json:"subject_id"`
	Depth           int       `json:"depth"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func (l Link) Key() string {
	return l.ParentSessionID + "\x00" + l.ParentRunID + "\x00" + l.ParentCallID
}

// DelegationLinkStore is an optional durable idempotency seam for delegation.
// PutIfAbsent returns the existing link when another replay won the race.
type DelegationLinkStore interface {
	Get(context.Context, string) (Link, bool, error)
	GetByChild(context.Context, string) (Link, bool, error)
	PutIfAbsent(context.Context, Link) (Link, bool, error)
}

// DelegationLinkFilter is the bounded, metadata-only catalog query surface.
// Empty identity filters mean "all values visible to the caller"; tenant
// authorization is deliberately enforced by the transport, not this store.
type DelegationLinkFilter struct {
	ParentSessionID string
	ParentRunID     string
	ChildSessionID  string
	TenantID        string
	Limit           int
	Offset          int
}

// DelegationLinkCatalog is optional so local capability-only deployments do
// not need to expose a catalog. Implementations must return links only; event
// content, prompts, arguments, and results are never part of this contract.
type DelegationLinkCatalog interface {
	List(context.Context, DelegationLinkFilter) ([]Link, error)
}

const (
	MaxLinkIDBytes      = 128
	MaxLinkTenantBytes  = 128
	MaxLinkSubjectBytes = 128
	MaxDelegationDepth  = 64
)

func validLinkText(name, value string, max int) error {
	if value == "" || len(value) > max {
		return fmt.Errorf("delegation link %s is empty or too long", name)
	}
	for _, r := range value {
		if r < 0x21 || r == 0x7f || !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-", r) {
			return fmt.Errorf("delegation link %s contains an invalid character", name)
		}
	}
	return nil
}

// ValidateLink applies the same bounded identity and ownership checks to
// memory and SQL implementations. Scope is represented by tenant/subject;
// callers must additionally check their full core.ScopePath ownership.
func ValidateLink(link Link) error {
	for name, value := range map[string]string{
		"parent_session_id": link.ParentSessionID, "parent_run_id": link.ParentRunID,
		"parent_call_id": link.ParentCallID, "child_session_id": link.ChildSessionID,
		"child_run_id": link.ChildRunID,
	} {
		if err := validLinkText(name, value, MaxLinkIDBytes); err != nil {
			return err
		}
	}
	for name, value := range map[string]string{"tenant_id": link.TenantID, "subject_id": link.SubjectID} {
		if err := validLinkText(name, value, MaxLinkTenantBytes); err != nil {
			return err
		}
	}
	if link.Depth < 1 || link.Depth > MaxDelegationDepth {
		return fmt.Errorf("delegation link depth must be between 1 and %d", MaxDelegationDepth)
	}
	return nil
}

func ValidateLinkFilter(filter DelegationLinkFilter) error {
	for name, value := range map[string]string{
		"parent_session_id": filter.ParentSessionID, "parent_run_id": filter.ParentRunID,
		"child_session_id": filter.ChildSessionID, "tenant_id": filter.TenantID,
	} {
		if value != "" {
			if err := validLinkText(name, value, MaxLinkTenantBytes); err != nil {
				return err
			}
		}
	}
	if filter.Limit < 0 || filter.Limit > 500 || filter.Offset < 0 {
		return fmt.Errorf("delegation link pagination is invalid")
	}
	return nil
}

type MemoryDelegationLinkStore struct {
	mu      sync.RWMutex
	byKey   map[string]Link
	byChild map[string]string
}

func NewMemoryDelegationLinkStore() *MemoryDelegationLinkStore {
	return &MemoryDelegationLinkStore{byKey: map[string]Link{}, byChild: map[string]string{}}
}

func (s *MemoryDelegationLinkStore) Get(_ context.Context, key string) (Link, bool, error) {
	if s == nil {
		return Link{}, false, fmt.Errorf("delegation link store is nil")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	link, ok := s.byKey[key]
	return link, ok, nil
}
func (s *MemoryDelegationLinkStore) GetByChild(_ context.Context, child string) (Link, bool, error) {
	if s == nil {
		return Link{}, false, fmt.Errorf("delegation link store is nil")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	key, ok := s.byChild[child]
	if !ok {
		return Link{}, false, nil
	}
	link := s.byKey[key]
	return link, true, nil
}
func (s *MemoryDelegationLinkStore) PutIfAbsent(_ context.Context, link Link) (Link, bool, error) {
	if s == nil {
		return Link{}, false, fmt.Errorf("delegation link store is nil")
	}
	if err := ValidateLink(link); err != nil {
		return Link{}, false, err
	}
	if link.CreatedAt.IsZero() {
		link.CreatedAt = time.Now().UTC()
	}
	if link.UpdatedAt.IsZero() {
		link.UpdatedAt = link.CreatedAt
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.byKey[link.Key()]; ok {
		return existing, false, nil
	}
	if existingKey, ok := s.byChild[link.ChildSessionID]; ok && existingKey != link.Key() {
		return Link{}, false, fmt.Errorf("child session is already linked")
	}
	s.byKey[link.Key()] = link
	s.byChild[link.ChildSessionID] = link.Key()
	return link, true, nil
}

func (s *MemoryDelegationLinkStore) List(_ context.Context, filter DelegationLinkFilter) ([]Link, error) {
	if s == nil {
		return nil, fmt.Errorf("delegation link store is nil")
	}
	if err := ValidateLinkFilter(filter); err != nil {
		return nil, err
	}
	s.mu.RLock()
	links := make([]Link, 0, len(s.byKey))
	for _, link := range s.byKey {
		if filter.ParentSessionID != "" && link.ParentSessionID != filter.ParentSessionID ||
			filter.ParentRunID != "" && link.ParentRunID != filter.ParentRunID ||
			filter.ChildSessionID != "" && link.ChildSessionID != filter.ChildSessionID ||
			filter.TenantID != "" && link.TenantID != filter.TenantID {
			continue
		}
		links = append(links, link)
	}
	s.mu.RUnlock()
	sort.Slice(links, func(i, j int) bool {
		if links[i].CreatedAt.Equal(links[j].CreatedAt) {
			return links[i].Key() > links[j].Key()
		}
		return links[i].CreatedAt.After(links[j].CreatedAt)
	})
	start := filter.Offset
	if start > len(links) {
		start = len(links)
	}
	end := len(links)
	limit := filter.Limit
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	return links[start:end], nil
}
