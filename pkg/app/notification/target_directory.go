package notification

import (
	"context"
	"sort"
	"strings"
)

// TargetDescriptor is the non-secret discovery record for one tenant-scoped
// target. Target is opaque; endpoint, credential, provider response, and
// persistence identifiers are intentionally absent.
type TargetDescriptor struct {
	Target  TargetRef  `json:"target"`
	Channel ChannelRef `json:"channel"`
	Label   string     `json:"label,omitempty"`
	Formats []string   `json:"formats,omitempty"`
}

// Clone returns a defensive copy with formats in canonical lexical order.
func (d TargetDescriptor) Clone() TargetDescriptor {
	d.Formats = append([]string(nil), d.Formats...)
	sort.Strings(d.Formats)
	return d
}

// Validate checks a descriptor against the exact channel references supplied
// by the caller. It never resolves a target and never inspects credentials.
func (d TargetDescriptor) Validate(knownChannels []ChannelRef) error {
	if !validText(d.Target.value, MaxTargetRefBytes) || strings.Contains(d.Target.value, "://") {
		return ErrInvalidTargetDescriptor
	}
	if validateRef(d.Channel) != nil {
		return ErrInvalidTargetDescriptor
	}
	if len(knownChannels) > MaxChannels {
		return ErrTargetDirectoryCapacity
	}
	known := make(map[string]struct{}, len(knownChannels))
	for _, channel := range knownChannels {
		if validateRef(channel) != nil {
			return ErrInvalidTargetDescriptor
		}
		key := refKey(channel)
		if _, exists := known[key]; exists {
			return ErrInvalidTargetDescriptor
		}
		known[key] = struct{}{}
	}
	if _, exists := known[refKey(d.Channel)]; !exists {
		return ErrUnknownTargetChannel
	}
	if d.Label != "" && (!validText(d.Label, MaxTargetLabelBytes) || sensitiveMetadataKey(d.Label) || sensitiveMetadataValue(d.Label)) {
		return ErrInvalidTargetDescriptor
	}
	if len(d.Formats) > MaxTargetFormats {
		return ErrInvalidTargetDescriptor
	}
	seen := make(map[string]struct{}, len(d.Formats))
	for _, format := range d.Formats {
		if !validText(format, MaxTargetFormatBytes) {
			return ErrInvalidTargetDescriptor
		}
		if _, exists := seen[format]; exists {
			return ErrInvalidTargetDescriptor
		}
		seen[format] = struct{}{}
	}
	return nil
}

// TargetDirectory discovers the current tenant-visible targets. Implementors
// must return only bounded, non-secret descriptors and must not rely on a
// process-global registry or background refresh loop.
type TargetDirectory interface {
	List(context.Context, string) ([]TargetDescriptor, error)
}

// SnapshotDirectory is an immutable in-memory TargetDirectory. It is useful
// for embedding and tests; callers provide exact channel references so an
// unknown channel can never enter the snapshot.
type SnapshotDirectory struct {
	targets map[string][]TargetDescriptor
}

// NewSnapshotDirectory builds a defensive, sorted snapshot. The total number
// of target descriptors and tenant buckets is bounded by MaxTargets.
func NewSnapshotDirectory(channels []ChannelRef, targets map[string][]TargetDescriptor) (*SnapshotDirectory, error) {
	if len(channels) > MaxChannels || len(targets) > MaxTargets {
		return nil, ErrTargetDirectoryCapacity
	}
	known := make([]ChannelRef, len(channels))
	copy(known, channels)
	seenChannels := make(map[string]struct{}, len(known))
	for _, channel := range known {
		if validateRef(channel) != nil {
			return nil, ErrInvalidTargetDescriptor
		}
		key := refKey(channel)
		if _, exists := seenChannels[key]; exists {
			return nil, ErrInvalidTargetDescriptor
		}
		seenChannels[key] = struct{}{}
	}
	out := make(map[string][]TargetDescriptor, len(targets))
	total := 0
	for tenant, descriptors := range targets {
		if err := ValidateTenantID(tenant); err != nil {
			return nil, err
		}
		if len(descriptors) > MaxTargets || total > MaxTargets-len(descriptors) {
			return nil, ErrTargetDirectoryCapacity
		}
		seenTargets := make(map[string]struct{}, len(descriptors))
		list := make([]TargetDescriptor, 0, len(descriptors))
		for _, descriptor := range descriptors {
			if err := descriptor.Validate(known); err != nil {
				return nil, err
			}
			key := descriptor.Target.String()
			if _, exists := seenTargets[key]; exists {
				return nil, ErrInvalidTargetDescriptor
			}
			seenTargets[key] = struct{}{}
			list = append(list, descriptor.Clone())
		}
		sort.Slice(list, func(i, j int) bool {
			if list[i].Target.String() != list[j].Target.String() {
				return list[i].Target.String() < list[j].Target.String()
			}
			return refKey(list[i].Channel) < refKey(list[j].Channel)
		})
		out[tenant] = list
		total += len(list)
	}
	return &SnapshotDirectory{targets: out}, nil
}

// List returns only the requested tenant's cloned descriptors.
func (d *SnapshotDirectory) List(ctx context.Context, tenantID string) ([]TargetDescriptor, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, err
	}
	if d == nil {
		return nil, ErrTargetDirectoryFailure
	}
	entries := d.targets[tenantID]
	out := make([]TargetDescriptor, len(entries))
	for i, entry := range entries {
		out[i] = entry.Clone()
	}
	return out, nil
}
