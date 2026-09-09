// Package effectreceipt adapts an accepted core tool invocation to the
// provider-neutral external-effect receipt contract. It intentionally does
// not name, configure, or enumerate any concrete effect provider.
package effectreceipt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	appreceipt "github.com/whhhh1500/auto-agent/pkg/app/effectreceipt"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	// CodeAcceptedInvocationRequired means the request did not come from
	// core's guarded tool funnel. No Binder or Driver method was called.
	CodeAcceptedInvocationRequired = "accepted_invocation_required"
	CodeRequestInvalid             = "external_effect_request_invalid"
	CodeBindingFailed              = "external_effect_binding_failed"
	CodeStoreFailed                = "external_effect_store_failed"
	CodeDriverFailed               = "external_effect_driver_failed"
	CodeReadBackFailed             = "external_effect_read_back_failed"
	CodeBridgePanic                = "external_effect_bridge_panic"
)

var ErrInvalidBridge = errors.New("invalid external effect capability bridge")

// Binding is bounded ephemeral material obtained from an accepted core tool
// request. Target and Payload are immediately hashed by the app contract and
// are never emitted in a CapabilityResult or passed to the durable Store.
type Binding struct {
	Target  []byte
	Payload []byte
}

// Binder translates one accepted capability request into provider-specific
// bytes. It is an application integration point: the bridge treats every
// returned error or panic as a fixed failure and does not expose its text.
type Binder interface {
	Bind(context.Context, core.CapabilityRequest) (Binding, error)
}

// BinderFunc adapts a function to Binder.
type BinderFunc func(context.Context, core.CapabilityRequest) (Binding, error)

func (f BinderFunc) Bind(ctx context.Context, request core.CapabilityRequest) (Binding, error) {
	return f(ctx, request)
}

// Bridge is a core.Capability whose only visible result is a fixed external
// effect state and code. The supplied manifest remains caller-owned input; a
// defensive copy is retained so later map or slice mutations cannot change
// the capability identity used for dispatch.
type Bridge struct {
	manifest  core.CapabilityManifest
	driverRef appreceipt.DriverRef
	revision  string
	store     appreceipt.Store
	driver    appreceipt.Driver
	binder    Binder
}

// canonicalTargetDriver is deliberately private to this adapter package. A
// reference driver can bind its actual external target before NewIntent writes
// its target digest, while arbitrary provider-neutral Drivers retain the
// normal caller-provided Binder target contract.
type canonicalTargetDriver interface {
	canonicalEffectTarget() []byte
}

var _ core.Capability = (*Bridge)(nil)
var _ core.ArtifactRevisioner = (*Bridge)(nil)

// NewBridge creates one dynamic core capability. The manifest must be a
// model-exposed, idempotent tool because a durable effect identity is only
// safe under the core guarded, journaled tool funnel.
func NewBridge(manifest core.CapabilityManifest, store appreceipt.Store, driver appreceipt.Driver, binder Binder) (*Bridge, error) {
	if !validBridgeManifest(manifest) || nilInterface(store) || nilInterface(driver) || nilInterface(binder) {
		return nil, ErrInvalidBridge
	}
	copyManifest, ok := cloneManifest(manifest)
	if !ok {
		return nil, ErrInvalidBridge
	}
	ref, refErr := safeDriverRef(driver)
	if refErr != nil {
		return nil, ErrInvalidBridge
	}
	return &Bridge{
		manifest: copyManifest, driverRef: ref, revision: bridgeRevision(copyManifest, ref),
		store: store, driver: driver, binder: binder,
	}, nil
}

// Manifest returns a defensive copy of the caller-supplied capability
// declaration. Registration remains the caller's core registry decision.
func (b *Bridge) Manifest() core.CapabilityManifest {
	if b == nil {
		return core.CapabilityManifest{}
	}
	manifest, ok := cloneManifest(b.manifest)
	if !ok {
		return core.CapabilityManifest{}
	}
	return manifest
}

// ArtifactRevision binds this capability's public manifest and fixed driver
// reference without exposing either value in run-composition metadata. The
// stable opaque label changes when either integration input changes.
func (b *Bridge) ArtifactRevision() string {
	if b == nil {
		return ""
	}
	return b.revision
}

// Execute verifies the core proof, binds ephemeral bytes, and dispatches the
// durable effect. A successful Dispatch acknowledgement is immediately
// read-back once; accepted remains accepted when read-back is pending. All
// returned content and metadata contain only the fixed state and code.
func (b *Bridge) Execute(ctx context.Context, request core.CapabilityRequest) (result core.CapabilityResult, err error) {
	defer func() {
		if recover() != nil {
			result = bridgeResult(appreceipt.StateUnknown, CodeBridgePanic)
			err = nil
		}
	}()
	if b == nil || ctx == nil || core.RequireAcceptedInvocation(request) != nil {
		return bridgeResult(appreceipt.StateUnknown, CodeAcceptedInvocationRequired), nil
	}
	if !requestMatchesManifest(request, b.manifest) {
		return bridgeResult(appreceipt.StateUnknown, CodeRequestInvalid), nil
	}

	binding, bindErr := safeBind(b.binder, ctx, request)
	if bindErr != nil {
		return bridgeResult(appreceipt.StateUnknown, CodeBindingFailed), nil
	}
	if targeter, ok := b.driver.(canonicalTargetDriver); ok && !bytes.Equal(binding.Target, targeter.canonicalEffectTarget()) {
		return bridgeResult(appreceipt.StateUnknown, CodeBindingFailed), nil
	}
	invocation, invocationErr := core.NewToolInvocation(core.RunInfo{
		RunID: request.Context.Invocation.RunID, SessionID: request.Context.Invocation.SessionID,
		Principal: request.Context.Principal,
	}, core.ToolCall{ID: request.CallID, Name: b.manifest.ID, Args: request.Args}, true)
	if invocationErr != nil {
		return bridgeResult(appreceipt.StateUnknown, CodeRequestInvalid), nil
	}
	intent, intentErr := appreceipt.NewIntent(invocation, b.driverRef, binding.Target, binding.Payload)
	if intentErr != nil {
		return bridgeResult(appreceipt.StateUnknown, CodeBindingFailed), nil
	}
	service, serviceErr := appreceipt.NewService(b.store, b.driver)
	if serviceErr != nil {
		return bridgeResult(appreceipt.StateUnknown, CodeStoreFailed), nil
	}
	record, effectErr := service.Execute(ctx, appreceipt.DispatchRequest{Intent: intent, Target: binding.Target, Payload: binding.Payload})
	if effectErr != nil {
		return bridgeFailure(record, effectErr), nil
	}
	if record.State == appreceipt.StateAccepted {
		record, effectErr = service.Reconcile(ctx, intent)
		if effectErr != nil {
			return bridgeFailure(record, effectErr), nil
		}
	}
	return recordResult(record), nil
}

func requestMatchesManifest(request core.CapabilityRequest, manifest core.CapabilityManifest) bool {
	return request.Context.CapabilityID == manifest.ID &&
		request.Context.Invocation.CallID == request.CallID &&
		request.Context.Invocation.SessionID != "" && request.Context.Invocation.RunID != ""
}

func validBridgeManifest(manifest core.CapabilityManifest) bool {
	return manifest.Kind == core.KindTool && manifest.Tool != nil && manifest.Idempotent &&
		core.ValidateNamespacedID(manifest.ID) == nil && strings.TrimSpace(manifest.Version) != ""
}

func cloneManifest(manifest core.CapabilityManifest) (core.CapabilityManifest, bool) {
	encoded, err := json.Marshal(manifest)
	if err != nil || len(encoded) > core.MaxCapabilityManifestBytes {
		return core.CapabilityManifest{}, false
	}
	var copyManifest core.CapabilityManifest
	if json.Unmarshal(encoded, &copyManifest) != nil {
		return core.CapabilityManifest{}, false
	}
	return copyManifest, true
}

func bridgeRevision(manifest core.CapabilityManifest, ref appreceipt.DriverRef) string {
	encoded, err := json.Marshal(struct {
		Manifest core.CapabilityManifest `json:"manifest"`
		Driver   appreceipt.DriverRef    `json:"driver"`
	}{Manifest: manifest, Driver: ref})
	if err != nil {
		return ""
	}
	return "effectreceipt/" + appreceipt.SHA256Digest(encoded)
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func safeBind(binder Binder, ctx context.Context, request core.CapabilityRequest) (binding Binding, err error) {
	defer func() {
		if recover() != nil {
			binding = Binding{}
			err = ErrInvalidBridge
		}
	}()
	binding, err = binder.Bind(ctx, request)
	if err != nil {
		return Binding{}, ErrInvalidBridge
	}
	binding.Target = append([]byte(nil), binding.Target...)
	binding.Payload = append([]byte(nil), binding.Payload...)
	return binding, nil
}

func safeDriverRef(driver appreceipt.Driver) (ref appreceipt.DriverRef, err error) {
	defer func() {
		if recover() != nil {
			ref = appreceipt.DriverRef{}
			err = ErrInvalidBridge
		}
	}()
	ref = driver.Ref()
	if !validDriverRef(ref) {
		return appreceipt.DriverRef{}, ErrInvalidBridge
	}
	return ref, nil
}

func bridgeFailure(record appreceipt.Record, cause error) core.CapabilityResult {
	if isEffectRecord(record) {
		if record.State == appreceipt.StateUnknown && record.ErrorCode != "" {
			return bridgeResult(record.State, record.ErrorCode)
		}
		return recordResult(record)
	}
	switch {
	case errors.Is(cause, appreceipt.ErrStore), errors.Is(cause, appreceipt.ErrStorePanic), errors.Is(cause, appreceipt.ErrConflict), errors.Is(cause, appreceipt.ErrNotFound):
		return bridgeResult(appreceipt.StateUnknown, CodeStoreFailed)
	case errors.Is(cause, appreceipt.ErrReadBack), errors.Is(cause, appreceipt.ErrReadBackPanic), errors.Is(cause, appreceipt.ErrReadBackMismatch):
		return bridgeResult(appreceipt.StateUnknown, CodeReadBackFailed)
	default:
		return bridgeResult(appreceipt.StateUnknown, CodeDriverFailed)
	}
}

func recordResult(record appreceipt.Record) core.CapabilityResult {
	if !isEffectRecord(record) {
		return bridgeResult(appreceipt.StateUnknown, CodeStoreFailed)
	}
	code := string(record.State)
	if record.State == appreceipt.StateUnknown && record.ErrorCode != "" {
		code = record.ErrorCode
	}
	return bridgeResult(record.State, code)
}

func isEffectRecord(record appreceipt.Record) bool {
	switch record.State {
	case appreceipt.StatePrepared, appreceipt.StateDispatching, appreceipt.StateAccepted, appreceipt.StateConfirmed, appreceipt.StateRejected, appreceipt.StateUnknown:
		return true
	default:
		return false
	}
}

func bridgeResult(state appreceipt.State, code string) core.CapabilityResult {
	if !isEffectRecord(appreceipt.Record{State: state}) {
		state, code = appreceipt.StateUnknown, CodeStoreFailed
	}
	content, marshalErr := json.Marshal(struct {
		State appreceipt.State `json:"state"`
		Code  string           `json:"code"`
	}{State: state, Code: code})
	if marshalErr != nil { // Both fields are fixed primitive values.
		content = []byte(`{"state":"unknown","code":"external_effect_bridge_panic"}`)
		state, code = appreceipt.StateUnknown, CodeBridgePanic
	}
	return core.CapabilityResult{
		Content: string(content), OK: state == appreceipt.StateConfirmed,
		Metadata: map[string]any{"state": string(state), "code": code},
	}
}
