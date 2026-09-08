package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/app/notification"
)

type testChannel struct{ ref notification.ChannelRef }

func (channel testChannel) Descriptor() notification.Descriptor {
	return notification.Descriptor{Ref: channel.ref, Capabilities: []notification.Capability{notification.CapabilityDeliver}}
}
func (testChannel) Deliver(context.Context, notification.Delivery) (notification.Receipt, error) {
	return notification.Receipt{}, errors.New("not called")
}

type testValidator struct{ ref notification.ChannelRef }

func (validator testValidator) Channel() notification.ChannelRef { return validator.ref }
func (testValidator) ValidateConfiguration([]byte) error         { return nil }

func testRegistration(ref notification.ChannelRef) Registration {
	return Registration{
		Ref: ref, Validator: testValidator{ref: ref},
		Build: func(*notification.Service) (notification.Channel, error) { return testChannel{ref: ref}, nil },
	}
}

func TestAssemblyDerivesSortedRefsValidatorsAndImmutableRegistry(t *testing.T) {
	first := notification.ChannelRef{ID: "alpha", Version: "1"}
	second := notification.ChannelRef{ID: "beta", Version: "2"}
	assembly, err := New([]Registration{testRegistration(second), testRegistration(first)})
	if err != nil {
		t.Fatal(err)
	}
	refs := assembly.Refs()
	if len(refs) != 2 || refs[0] != first || refs[1] != second {
		t.Fatalf("refs=%#v", refs)
	}
	refs[0] = second
	if got := assembly.Refs(); got[0] != first {
		t.Fatalf("refs leaked assembly state: %#v", got)
	}
	validators := assembly.Validators()
	if len(validators) != 2 || validators[0].Channel() != first || validators[1].Channel() != second {
		t.Fatalf("validators=%#v", validators)
	}
	service, err := notification.NewService(&repositoryStub{}, assembly.Refs(), assembly.Validators()...)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := assembly.BuildRegistry(service)
	if err != nil {
		t.Fatal(err)
	}
	if got := registry.Descriptors(); len(got) != 2 || got[0].Ref != first || got[1].Ref != second {
		t.Fatalf("registry descriptors=%#v", got)
	}
}

func TestAssemblyRejectsInvalidOrInconsistentRegistration(t *testing.T) {
	ref := notification.ChannelRef{ID: "provider", Version: "1"}
	for _, registrations := range [][]Registration{
		nil,
		{{Ref: ref}},
		{{Ref: ref, Build: testRegistration(ref).Build}, {Ref: ref, Build: testRegistration(ref).Build}},
		{{Ref: ref, Validator: testValidator{ref: notification.ChannelRef{ID: "other", Version: "1"}}, Build: testRegistration(ref).Build}},
	} {
		if _, err := New(registrations); !errors.Is(err, ErrInvalidRegistration) {
			t.Fatalf("New(%#v) err=%v", registrations, err)
		}
	}
}

func TestAssemblyBuildRejectsFactoryMismatchedDescriptor(t *testing.T) {
	ref := notification.ChannelRef{ID: "provider", Version: "1"}
	assembly, err := New([]Registration{{
		Ref: ref,
		Build: func(*notification.Service) (notification.Channel, error) {
			return testChannel{ref: notification.ChannelRef{ID: "unexpected", Version: "1"}}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	service, err := notification.NewService(&repositoryStub{}, assembly.Refs())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := assembly.BuildRegistry(service); !errors.Is(err, ErrInvalidRegistration) {
		t.Fatalf("BuildRegistry err=%v", err)
	}
}

func TestAssemblyRejectsPanickingAndTypedNilValidators(t *testing.T) {
	ref := notification.ChannelRef{ID: "provider", Version: "1"}
	var typedNil *nilValidator
	for _, validator := range []notification.ConfigurationValidator{panickingValidator{}, typedNil} {
		if _, err := New([]Registration{{Ref: ref, Validator: validator, Build: testRegistration(ref).Build}}); !errors.Is(err, ErrInvalidRegistration) {
			t.Fatalf("validator=%T err=%v", validator, err)
		}
	}
}

func TestAssemblyBuildRejectsServiceWithDifferentRefs(t *testing.T) {
	ref := notification.ChannelRef{ID: "provider", Version: "1"}
	assembly, err := New([]Registration{testRegistration(ref)})
	if err != nil {
		t.Fatal(err)
	}
	service, err := notification.NewService(&repositoryStub{}, []notification.ChannelRef{{ID: "other", Version: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := assembly.BuildRegistry(service); !errors.Is(err, ErrInvalidRegistration) {
		t.Fatalf("BuildRegistry err=%v", err)
	}
}

type panickingValidator struct{}

func (panickingValidator) Channel() notification.ChannelRef   { panic("provider validator panic") }
func (panickingValidator) ValidateConfiguration([]byte) error { return nil }

type nilValidator struct{}

func (*nilValidator) Channel() notification.ChannelRef   { return notification.ChannelRef{} }
func (*nilValidator) ValidateConfiguration([]byte) error { return nil }

type repositoryStub struct{}

func (repositoryStub) List(context.Context, string) ([]notification.TargetDescriptor, error) {
	return nil, nil
}
func (repositoryStub) ListRecords(context.Context, string) ([]notification.TargetRecord, error) {
	return nil, nil
}
func (repositoryStub) Create(context.Context, string, notification.TargetDescriptor, notification.TargetConfiguration, bool) (string, error) {
	return "1", nil
}
func (repositoryStub) Update(context.Context, string, notification.TargetDescriptor, notification.TargetConfiguration, bool, string) (string, error) {
	return "1", nil
}
func (repositoryStub) Delete(context.Context, string, notification.TargetRef, string) error {
	return nil
}
func (repositoryStub) ResolveConfig(context.Context, string, notification.TargetRef, notification.ChannelRef) (notification.TargetConfiguration, error) {
	return notification.TargetConfiguration{}, nil
}
