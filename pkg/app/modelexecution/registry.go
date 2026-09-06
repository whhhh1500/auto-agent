package modelexecution

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cc-auto-agent/harness-core/pkg/app/modelcontrol"
)

// Registry immutably binds one catalog snapshot to exact provider and protocol
// implementation evidence. It never uses an ID-only fallback.
type Registry struct {
	snapshotRevision string
	providers        map[string]ProviderRegistration
	protocols        map[string]ProtocolRegistration
	plans            map[string]struct{}
}

func NewRegistry(snapshotRevision string, plans []modelcontrol.ProviderPlan, providers []ProviderRegistration, protocols []ProtocolRegistration) (*Registry, error) {
	if snapshotRevision == "" || len(plans) == 0 || len(plans) > modelcontrol.DefaultMaxEntries || len(providers) > modelcontrol.DefaultMaxEntries || len(protocols) > modelcontrol.DefaultMaxEntries {
		return nil, fmt.Errorf("%w: registry bounds", ErrInvalidRequest)
	}
	r := &Registry{snapshotRevision: snapshotRevision, providers: make(map[string]ProviderRegistration, len(providers)), protocols: make(map[string]ProtocolRegistration, len(protocols)), plans: make(map[string]struct{}, len(plans))}
	for _, plan := range plans {
		if plan.SnapshotRevision != snapshotRevision {
			return nil, fmt.Errorf("%w: registered plan snapshot", ErrBindingDrift)
		}
		if err := validateRequest(Request{Plan: plan, Messages: []Message{{Role: "user", Content: "binding"}}}); err != nil {
			return nil, err
		}
		key, err := planKey(plan)
		if err != nil {
			return nil, err
		}
		if _, exists := r.plans[key]; exists {
			return nil, fmt.Errorf("%w: duplicate plan", ErrInvalidRequest)
		}
		r.plans[key] = struct{}{}
	}
	for _, entry := range providers {
		if err := validateProviderRegistration(entry); err != nil {
			return nil, err
		}
		key := bindingKey(entry.Binding)
		if _, exists := r.providers[key]; exists {
			return nil, fmt.Errorf("%w: duplicate provider", ErrInvalidRequest)
		}
		r.providers[key] = entry
	}
	for _, entry := range protocols {
		if err := validateProtocolRegistration(entry); err != nil {
			return nil, err
		}
		key := bindingKey(entry.Binding)
		if _, exists := r.protocols[key]; exists {
			return nil, fmt.Errorf("%w: duplicate protocol", ErrInvalidRequest)
		}
		r.protocols[key] = entry
	}
	for _, plan := range plans {
		if plan.Catalog.Provider != plan.Provider.Ref || plan.Catalog.Protocol != plan.Protocol.Ref || plan.Catalog.Credential != plan.Credential {
			return nil, fmt.Errorf("%w: registered plan closure", ErrBindingDrift)
		}
		if _, ok := r.providers[bindingKey(plan.Provider)]; !ok {
			return nil, fmt.Errorf("%w: registered plan provider", ErrBindingDrift)
		}
		if _, ok := r.protocols[bindingKey(plan.Protocol)]; !ok {
			return nil, fmt.Errorf("%w: registered plan protocol", ErrBindingDrift)
		}
	}
	return r, nil
}

func (r *Registry) Execute(ctx context.Context, request Request, emit Emit) error {
	if r == nil || ctx == nil || emit == nil {
		return fmt.Errorf("%w: nil execution dependency", ErrInvalidRequest)
	}
	if err := validateRequest(request); err != nil {
		return err
	}
	if request.Plan.SnapshotRevision != r.snapshotRevision {
		return fmt.Errorf("%w: snapshot revision", ErrBindingDrift)
	}
	key, err := planKey(request.Plan)
	if err != nil {
		return err
	}
	if _, ok := r.plans[key]; !ok {
		return fmt.Errorf("%w: unregistered provider plan", ErrBindingDrift)
	}
	providerRegistration, ok := r.providers[bindingKey(request.Plan.Provider)]
	if !ok {
		return fmt.Errorf("%w: provider", ErrBindingDrift)
	}
	protocolRegistration, ok := r.protocols[bindingKey(request.Plan.Protocol)]
	if !ok {
		return fmt.Errorf("%w: protocol", ErrBindingDrift)
	}
	provider, err := invokeProvider(providerRegistration.Factory)
	if err != nil {
		return err
	}
	protocol, err := invokeProtocol(protocolRegistration.Factory)
	if err != nil {
		return err
	}
	validator := NewStreamValidator()
	err = safeProtocolExecute(protocol, ctx, request.Clone(), provider, func(event Event) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		validated, err := validator.Accept(event)
		if err != nil {
			return err
		}
		return safeEmit(emit, validated)
	})
	if err != nil {
		return err
	}
	if !validator.Finished() {
		return fmt.Errorf("%w: protocol ended without finish", ErrStreamState)
	}
	return nil
}
func planKey(plan modelcontrol.ProviderPlan) (string, error) {
	encoded, err := json.Marshal(clonePlan(plan))
	if err != nil {
		return "", fmt.Errorf("%w: plan encoding", ErrInvalidRequest)
	}
	return string(encoded), nil
}

func validateProviderRegistration(r ProviderRegistration) error {
	if r.Factory == nil || r.Binding.Ref.ID == "" || r.Binding.Ref.Version == "" || r.Binding.ImplementationRevision == "" {
		return fmt.Errorf("%w: provider registration", ErrInvalidRequest)
	}
	return nil
}
func validateProtocolRegistration(r ProtocolRegistration) error {
	if r.Factory == nil || r.Binding.Ref.ID == "" || r.Binding.Ref.Version == "" || r.Binding.ImplementationRevision == "" {
		return fmt.Errorf("%w: protocol registration", ErrInvalidRequest)
	}
	return nil
}
func bindingKey(b modelcontrol.ImplementationBinding) string {
	return b.Ref.ID + "\x00" + b.Ref.Version + "\x00" + b.ImplementationRevision
}
func invokeProvider(factory ProviderFactory) (provider Provider, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("%w: %T", ErrFactoryPanic, value)
			provider = nil
		}
	}()
	provider, err = factory()
	if err == nil && provider == nil {
		err = fmt.Errorf("%w: nil provider", ErrInvalidRequest)
	}
	return
}
func invokeProtocol(factory ProtocolFactory) (protocol Protocol, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("%w: %T", ErrFactoryPanic, value)
			protocol = nil
		}
	}()
	protocol, err = factory()
	if err == nil && protocol == nil {
		err = fmt.Errorf("%w: nil protocol", ErrInvalidRequest)
	}
	return
}
func safeEmit(emit Emit, event Event) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("%w: emit panicked: %T", ErrStreamState, value)
		}
	}()
	return emit(event)
}
func safeProtocolExecute(protocol Protocol, ctx context.Context, request Request, provider Provider, emit Emit) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("%w: protocol panic: %T", ErrStreamState, value)
		}
	}()
	return protocol.Execute(ctx, request, provider, emit)
}
