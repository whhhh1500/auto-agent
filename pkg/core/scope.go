package core

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ScopeKind identifies one ownership level in a capability hierarchy.
type ScopeKind string

const (
	ScopeGlobal     ScopeKind = "global"
	ScopeDeployment ScopeKind = "deployment"
	ScopeProduct    ScopeKind = "product"
	ScopeTenant     ScopeKind = "tenant"
	ScopeWorkspace  ScopeKind = "workspace"
	ScopeUser       ScopeKind = "user"
	ScopeSession    ScopeKind = "session"
	ScopeRun        ScopeKind = "run"
)

var scopeRanks = map[ScopeKind]int{
	ScopeGlobal: 0, ScopeDeployment: 10, ScopeProduct: 20, ScopeTenant: 30,
	ScopeWorkspace: 40, ScopeUser: 50, ScopeSession: 60, ScopeRun: 70,
}

// ScopeRef names one level in a ScopePath.
type ScopeRef struct {
	Kind ScopeKind `json:"kind"`
	ID   string    `json:"id"`
}

// ScopePath is an immutable, root-to-leaf capability ownership path.
type ScopePath struct {
	segments []ScopeRef
}

// NewScopePath validates and copies a root-to-leaf scope path.
func NewScopePath(segments ...ScopeRef) (ScopePath, error) {
	if len(segments) == 0 {
		return ScopePath{}, fmt.Errorf("scope path is empty")
	}
	copyOf := append([]ScopeRef(nil), segments...)
	previous := -1
	for i, segment := range copyOf {
		rank, ok := scopeRanks[segment.Kind]
		if !ok {
			return ScopePath{}, fmt.Errorf("scope segment %d has unknown kind %q", i, segment.Kind)
		}
		if strings.TrimSpace(segment.ID) == "" {
			return ScopePath{}, fmt.Errorf("scope segment %d has an empty id", i)
		}
		if strings.ContainsAny(segment.ID, "/\x00") {
			return ScopePath{}, fmt.Errorf("scope segment %d id %q contains a reserved delimiter", i, segment.ID)
		}
		if rank <= previous {
			return ScopePath{}, fmt.Errorf("scope kind %q is out of order", segment.Kind)
		}
		previous = rank
	}
	return ScopePath{segments: copyOf}, nil
}

// MustScopePath constructs a path and panics when the path is invalid.
func MustScopePath(segments ...ScopeRef) ScopePath {
	path, err := NewScopePath(segments...)
	if err != nil {
		panic(err)
	}
	return path
}

// Child returns a new path with one lower ownership level appended.
func (p ScopePath) Child(ref ScopeRef) (ScopePath, error) {
	segments := append(p.Segments(), ref)
	return NewScopePath(segments...)
}

// Segments returns a defensive copy of the path.
func (p ScopePath) Segments() []ScopeRef {
	return append([]ScopeRef(nil), p.segments...)
}

// Depth returns the number of ownership levels in the path.
func (p ScopePath) Depth() int { return len(p.segments) }

// IsAncestorOf reports whether p is a prefix of other, including equality.
func (p ScopePath) IsAncestorOf(other ScopePath) bool {
	if len(p.segments) > len(other.segments) {
		return false
	}
	for i := range p.segments {
		if p.segments[i] != other.segments[i] {
			return false
		}
	}
	return true
}

// Prefixes returns every path from the root segment through the full path.
func (p ScopePath) Prefixes() []ScopePath {
	out := make([]ScopePath, 0, len(p.segments))
	for i := range p.segments {
		out = append(out, ScopePath{segments: append([]ScopeRef(nil), p.segments[:i+1]...)})
	}
	return out
}

// Equal reports whether two paths contain the same segments.
func (p ScopePath) Equal(other ScopePath) bool {
	return p.IsAncestorOf(other) && len(p.segments) == len(other.segments)
}

// String returns a stable path used by registries and diagnostics.
func (p ScopePath) String() string {
	parts := make([]string, 0, len(p.segments))
	for _, segment := range p.segments {
		parts = append(parts, string(segment.Kind)+":"+segment.ID)
	}
	return strings.Join(parts, "/")
}

// MarshalJSON keeps ScopePath's representation independent from its internals.
func (p ScopePath) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.segments)
}

// UnmarshalJSON validates a serialized ScopePath.
func (p *ScopePath) UnmarshalJSON(data []byte) error {
	var segments []ScopeRef
	if err := json.Unmarshal(data, &segments); err != nil {
		return err
	}
	path, err := NewScopePath(segments...)
	if err != nil {
		return err
	}
	*p = path
	return nil
}
