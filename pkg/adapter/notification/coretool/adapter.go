// Package coretool exposes notification channels through the core guarded
// capability funnel. It contains no transport implementation.
package coretool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/cc-auto-agent/harness-core/pkg/app/notification"
	"github.com/cc-auto-agent/harness-core/pkg/core"
)

const (
	ChannelsCapabilityID = "notify.channels"
	TargetsCapabilityID  = "notify.targets"
	SendCapabilityID     = "notify.send"
)

// New returns the two standard notification capabilities. The returned
// providers retain only the supplied immutable registry; registration into a
// core CapabilityRegistry remains the caller's composition decision.
func New(registry *notification.Registry) ([]core.Capability, error) {
	if registry == nil {
		return nil, errors.New("notification registry is nil")
	}
	return []core.Capability{
		capability{kind: capabilityChannels, registry: registry},
		capability{kind: capabilitySend, registry: registry},
	}, nil
}

// NewWithDirectory returns the channel, tenant-scoped target discovery, and
// send capabilities. The directory is consulted dynamically per invocation;
// it is not an authorization boundary for notify.send.
func NewWithDirectory(registry *notification.Registry, directory notification.TargetDirectory) ([]core.Capability, error) {
	if registry == nil {
		return nil, errors.New("notification registry is nil")
	}
	if directory == nil {
		return nil, errors.New("notification target directory is nil")
	}
	return []core.Capability{
		capability{kind: capabilityChannels, registry: registry},
		capability{kind: capabilityTargets, registry: registry, directory: directory},
		capability{kind: capabilitySend, registry: registry},
	}, nil
}

type capabilityKind uint8

const (
	capabilityChannels capabilityKind = iota + 1
	capabilityTargets
	capabilitySend
)

type capability struct {
	kind      capabilityKind
	registry  *notification.Registry
	directory notification.TargetDirectory
}

func (c capability) Manifest() core.CapabilityManifest {
	if c.kind == capabilityChannels {
		return channelsManifest()
	}
	if c.kind == capabilityTargets {
		return targetsManifest()
	}
	return sendManifest()
}

func (c capability) Execute(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{Content: err.Error(), OK: false, Metadata: map[string]any{"code": "accepted_invocation_required"}}, nil
	}
	if c.kind == capabilityChannels {
		return c.listChannels()
	}
	if c.kind == capabilityTargets {
		return c.listTargets(ctx, request)
	}
	return c.send(ctx, request)
}

func (c capability) listChannels() (core.CapabilityResult, error) {
	data, err := json.Marshal(struct {
		Channels []notification.Descriptor `json:"channels"`
	}{Channels: c.registry.Descriptors()})
	if err != nil {
		return core.CapabilityResult{}, err
	}
	return core.CapabilityResult{Content: string(data), OK: true}, nil
}

func (c capability) listTargets(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if len(request.Args) != 0 {
		return failedResult(errors.New("notify.targets arguments must be empty")), nil
	}
	if c.directory == nil {
		return failedDirectoryResult(notification.ErrTargetDirectoryFailure), nil
	}
	targets, err := safeDirectoryList(c.directory, ctx, request.Context.Principal.TenantID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return core.CapabilityResult{}, context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return core.CapabilityResult{}, context.DeadlineExceeded
		}
		return failedDirectoryResult(err), nil
	}
	known := make([]notification.ChannelRef, 0, len(c.registry.Descriptors()))
	for _, descriptor := range c.registry.Descriptors() {
		known = append(known, descriptor.Ref)
	}
	wire := make([]targetWire, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if err := target.Validate(known); err != nil {
			return failedDirectoryResult(notification.ErrTargetDirectoryFailure), nil
		}
		key := target.Target.String()
		if _, exists := seen[key]; exists {
			return failedDirectoryResult(notification.ErrTargetDirectoryFailure), nil
		}
		seen[key] = struct{}{}
		target = target.Clone()
		wire = append(wire, targetWire{
			TargetRef: target.Target.String(), ChannelID: target.Channel.ID, ChannelVersion: target.Channel.Version,
			Label: target.Label, Formats: append([]string(nil), target.Formats...),
		})
	}
	sort.Slice(wire, func(i, j int) bool {
		if wire[i].TargetRef != wire[j].TargetRef {
			return wire[i].TargetRef < wire[j].TargetRef
		}
		if wire[i].ChannelID != wire[j].ChannelID {
			return wire[i].ChannelID < wire[j].ChannelID
		}
		return wire[i].ChannelVersion < wire[j].ChannelVersion
	})
	data, err := json.Marshal(struct {
		Targets []targetWire `json:"targets"`
	}{Targets: wire})
	if err != nil || len(data) > maxTargetOutputBytes {
		return failedDirectoryResult(notification.ErrTargetDirectoryCapacity), nil
	}
	return core.CapabilityResult{Content: string(data), OK: true}, nil
}

type targetWire struct {
	TargetRef      string   `json:"target_ref"`
	ChannelID      string   `json:"channel_id"`
	ChannelVersion string   `json:"channel_version"`
	Label          string   `json:"label,omitempty"`
	Formats        []string `json:"formats,omitempty"`
}

const maxTargetOutputBytes = 512 << 10

func safeDirectoryList(directory notification.TargetDirectory, ctx context.Context, tenantID string) (targets []notification.TargetDescriptor, err error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := notification.ValidateTenantID(tenantID); err != nil {
		return nil, err
	}
	defer func() {
		if recover() != nil {
			targets = nil
			err = notification.ErrTargetDirectoryPanic
		}
	}()
	targets, err = directory.List(ctx, tenantID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, context.DeadlineExceeded
		}
		return nil, notification.ErrTargetDirectoryFailure
	}
	if len(targets) > notification.MaxTargets {
		return nil, notification.ErrTargetDirectoryCapacity
	}
	return targets, nil
}

func (c capability) send(ctx context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	ref, delivery, err := deliveryFromRequest(request)
	if err != nil {
		return failedResult(err), nil
	}
	receipt, err := c.registry.Deliver(ctx, ref, delivery)
	if err != nil {
		if errors.Is(err, notification.ErrInvalidDelivery) || errors.Is(err, notification.ErrChannelNotFound) || errors.Is(err, notification.ErrInvalidDescriptor) {
			return failedResult(err), nil
		}
		// A channel error may follow an external side effect. Returning an error
		// lets core's ToolInvocationJournal mark the call outcome uncertain.
		return core.CapabilityResult{}, err
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return core.CapabilityResult{}, err
	}
	return core.CapabilityResult{Content: string(data), OK: true}, nil
}

func deliveryFromRequest(request core.CapabilityRequest) (notification.ChannelRef, notification.Delivery, error) {
	args := request.Args
	if len(args) != 5 && len(args) != 6 {
		return notification.ChannelRef{}, notification.Delivery{}, fmt.Errorf("notify.send requires channel, target, text, format, and idempotency metadata")
	}
	if err := rejectUnknownArgs(args, "channel_id", "channel_version", "target_ref", "text", "format", "metadata"); err != nil {
		return notification.ChannelRef{}, notification.Delivery{}, err
	}
	id, okID := args["channel_id"].(string)
	version, okVersion := args["channel_version"].(string)
	target, okTarget := args["target_ref"].(string)
	text, okText := args["text"].(string)
	format, okFormat := args["format"].(string)
	if !okID || !okVersion || !okTarget || !okText || !okFormat {
		return notification.ChannelRef{}, notification.Delivery{}, errors.New("notify.send arguments must be strings")
	}
	targetRef, err := notification.NewTargetRef(target)
	if err != nil {
		return notification.ChannelRef{}, notification.Delivery{}, err
	}
	metadata, err := stringMetadata(args["metadata"])
	if err != nil {
		return notification.ChannelRef{}, notification.Delivery{}, err
	}
	invocation := request.Context.Invocation
	idempotencyKey := request.IdempotencyKey
	if idempotencyKey == "" {
		// notify.send is deliberately non-idempotent at the core manifest
		// boundary. Keep a bounded per-call identity in the delivery contract
		// without claiming that a generic channel deduplicates it.
		idempotencyKey = request.CallID
	}
	delivery := notification.Delivery{
		TenantID: request.Context.Principal.TenantID, SessionID: invocation.SessionID,
		RunID: invocation.RunID, CallID: request.CallID, IdempotencyKey: idempotencyKey,
		Target: targetRef, Text: text, Format: format, Metadata: metadata,
	}
	return notification.ChannelRef{ID: id, Version: version}, delivery, notification.ValidateDelivery(delivery)
}

func stringMetadata(value any) (map[string]string, error) {
	if value == nil {
		return nil, nil
	}
	items, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("notify.send metadata must be an object")
	}
	metadata := make(map[string]string, len(items))
	for key, raw := range items {
		item, ok := raw.(string)
		if !ok {
			return nil, errors.New("notify.send metadata values must be strings")
		}
		metadata[key] = item
	}
	return metadata, nil
}

func rejectUnknownArgs(args map[string]any, allowed ...string) error {
	set := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		set[key] = struct{}{}
	}
	for key := range args {
		if _, ok := set[key]; !ok {
			return fmt.Errorf("notify.send argument %q is not allowed", key)
		}
	}
	return nil
}

func failedResult(err error) core.CapabilityResult {
	return core.CapabilityResult{Content: err.Error(), OK: false, Metadata: map[string]any{"code": "notification_request_invalid"}}
}

func failedDirectoryResult(err error) core.CapabilityResult {
	return core.CapabilityResult{Content: err.Error(), OK: false, Metadata: map[string]any{"code": "notification_target_directory_failed"}}
}

func channelsManifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: ChannelsCapabilityID, Version: "1", Name: "Notification channels",
		Description: "List available notification channel references and non-secret capabilities.",
		Kind:        core.KindTool, Contract: "notification/coretool/v1", RequiredPermissions: []core.Permission{core.PermRead},
		MaxOutputBytes: 64 << 10, Idempotent: true,
		Tool: &core.ToolExposure{Description: "List exact notification channel references and capabilities.", Parameters: map[string]any{"type": "object", "additionalProperties": false}},
	}
}

func targetsManifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: TargetsCapabilityID, Version: "1", Name: "Notification targets",
		Description: "List tenant-visible notification targets without exposing endpoints or credentials.",
		Kind:        core.KindTool, Contract: "notification/coretool/v1", RequiredPermissions: []core.Permission{core.PermRead},
		MaxOutputBytes: maxTargetOutputBytes, Idempotent: true,
		Tool: &core.ToolExposure{Description: "List opaque notification target references for the accepted tenant.", Parameters: emptyInputSchema()},
	}
}

func emptyInputSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false}
}

func sendManifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: SendCapabilityID, Version: "1", Name: "Send notification",
		Description: "Deliver text through an approved notification channel and opaque target reference.",
		Kind:        core.KindTool, Contract: "notification/coretool/v1", RequiredPermissions: []core.Permission{core.PermSend},
		// A generic Channel cannot prove durable deduplication. The guarded core
		// therefore treats send as non-idempotent by default; the request field
		// remains part of Delivery and is forwarded when present.
		MaxOutputBytes: 32 << 10, Idempotent: false, RequiresApproval: true,
		Tool: &core.ToolExposure{Description: "Send text using a channel reference and opaque target reference. URLs and credentials are not accepted.", Parameters: sendInputSchema()},
	}
}

func sendInputSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []any{"channel_id", "channel_version", "target_ref", "text", "format"},
		"properties": map[string]any{
			"channel_id":      map[string]any{"type": "string", "maxLength": notification.MaxChannelIDBytes},
			"channel_version": map[string]any{"type": "string", "maxLength": notification.MaxChannelVersionBytes},
			"target_ref":      map[string]any{"type": "string", "maxLength": notification.MaxTargetRefBytes},
			"text":            map[string]any{"type": "string", "maxLength": notification.MaxTextBytes},
			"format":          map[string]any{"type": "string", "maxLength": notification.MaxFormatBytes},
			"metadata":        map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}},
		},
	}
}
