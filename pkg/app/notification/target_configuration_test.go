package notification

import (
	"context"
	"errors"
	"testing"
)

type targetRepositoryStub struct {
	listResult []TargetDescriptor
	records    []TargetRecord
	config     TargetConfiguration
	created    TargetConfiguration
	updated    TargetConfiguration
	err        error
	panicCall  bool
}

type targetConfigurationValidator struct {
	channel ChannelRef
}

func (validator targetConfigurationValidator) Channel() ChannelRef { return validator.channel }
func (targetConfigurationValidator) ValidateConfiguration(payload []byte) error {
	if string(payload) != `{"ok":true}` {
		return ErrInvalidTargetConfiguration
	}
	return nil
}

func (stub *targetRepositoryStub) List(context.Context, string) ([]TargetDescriptor, error) {
	return stub.listResult, stub.err
}
func (stub *targetRepositoryStub) ListRecords(context.Context, string) ([]TargetRecord, error) {
	if stub.panicCall {
		panic("repository secret")
	}
	if stub.records != nil {
		return stub.records, stub.err
	}
	records := make([]TargetRecord, len(stub.listResult))
	for index, descriptor := range stub.listResult {
		records[index] = TargetRecord{Descriptor: descriptor, Enabled: true, Revision: "1"}
	}
	return records, stub.err
}
func (stub *targetRepositoryStub) Create(_ context.Context, _ string, _ TargetDescriptor, configuration TargetConfiguration, _ bool) (string, error) {
	if stub.panicCall {
		panic("repository secret")
	}
	stub.created = configuration.Clone()
	return "1", stub.err
}
func (stub *targetRepositoryStub) Update(_ context.Context, _ string, _ TargetDescriptor, configuration TargetConfiguration, _ bool, _ string) (string, error) {
	stub.updated = configuration.Clone()
	return "2", stub.err
}
func (stub *targetRepositoryStub) Delete(context.Context, string, TargetRef, string) error {
	return stub.err
}
func (stub *targetRepositoryStub) ResolveConfig(context.Context, string, TargetRef, ChannelRef) (TargetConfiguration, error) {
	return stub.config.Clone(), stub.err
}

func targetServiceFixture(t *testing.T) (*Service, TargetDescriptor, *targetRepositoryStub) {
	t.Helper()
	channel := ChannelRef{ID: "webhook", Version: "1"}
	target, err := NewTargetRef("ops")
	if err != nil {
		t.Fatal(err)
	}
	descriptor := TargetDescriptor{Target: target, Channel: channel, Label: "Operations", Formats: []string{"text", "markdown"}}
	repository := &targetRepositoryStub{listResult: []TargetDescriptor{descriptor}, config: TargetConfiguration{Payload: []byte(`{"url":"https://example.test/hook","secret":"s"}`)}}
	service, err := NewService(repository, []ChannelRef{channel})
	if err != nil {
		t.Fatal(err)
	}
	return service, descriptor, repository
}

func TestTargetServiceListOnlyDescriptorsAndDefensiveCopies(t *testing.T) {
	service, descriptor, repository := targetServiceFixture(t)
	got, err := service.List(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Target.String() != descriptor.Target.String() {
		t.Fatalf("unexpected list: %#v", got)
	}
	got[0].Formats[0] = "changed"
	if repository.listResult[0].Formats[0] == "changed" {
		t.Fatal("list leaked repository formats")
	}
}

func TestTargetServiceChannelsReturnsRegisteredReferencesAndCopy(t *testing.T) {
	service, _, _ := targetServiceFixture(t)
	first := service.Channels()
	if len(first) != 1 || first[0] != (ChannelRef{ID: "webhook", Version: "1"}) {
		t.Fatalf("channels=%#v", first)
	}
	first[0] = ChannelRef{ID: "changed", Version: "2"}
	second := service.Channels()
	if len(second) != 1 || second[0] != (ChannelRef{ID: "webhook", Version: "1"}) {
		t.Fatalf("channels leaked mutable slice: %#v", second)
	}
}

func TestTargetServiceListRecordsIncludesDisabledRevisionWithoutConfig(t *testing.T) {
	service, descriptor, repository := targetServiceFixture(t)
	repository.records = []TargetRecord{{Descriptor: descriptor, Enabled: false, Revision: "7"}}
	records, err := service.ListRecords(context.Background(), "tenant-a")
	if err != nil || len(records) != 1 || records[0].Enabled || records[0].Revision != "7" {
		t.Fatalf("records=%#v err=%v", records, err)
	}
	if listed, err := service.List(context.Background(), "tenant-a"); err != nil || len(listed) != 0 {
		t.Fatalf("disabled list=%#v err=%v", listed, err)
	}
}

func TestTargetServiceConfigurationOwnershipAndErrors(t *testing.T) {
	service, descriptor, repository := targetServiceFixture(t)
	input := TargetConfiguration{Payload: []byte("opaque")}
	if _, err := service.Create(context.Background(), "tenant-a", descriptor, input, true); err != nil {
		t.Fatal(err)
	}
	input.Payload[0] = 'X'
	if string(repository.created.Payload) != "opaque" {
		t.Fatalf("create did not isolate caller buffer: %q", repository.created.Payload)
	}
	resolved, err := service.ResolveConfig(context.Background(), "tenant-a", descriptor.Target, descriptor.Channel)
	if err != nil {
		t.Fatal(err)
	}
	resolved[0] = 'X'
	if repository.config.Payload[0] == 'X' {
		t.Fatal("resolve leaked repository buffer")
	}
	repository.err = errors.New("provider secret must not escape")
	if _, err := service.ResolveConfig(context.Background(), "tenant-a", descriptor.Target, descriptor.Channel); !errors.Is(err, ErrTargetRepository) {
		t.Fatalf("error = %v, want fixed repository error", err)
	}
}

func TestTargetServiceRejectsInvalidAndCanceledCommands(t *testing.T) {
	service, descriptor, _ := targetServiceFixture(t)
	if _, err := service.Create(context.Background(), "", descriptor, TargetConfiguration{}, true); !errors.Is(err, ErrInvalidTenantID) {
		t.Fatalf("invalid tenant error = %v", err)
	}
	if _, err := service.Update(context.Background(), "tenant-a", descriptor, TargetConfiguration{}, true, "0"); !errors.Is(err, ErrInvalidTargetRevision) {
		t.Fatalf("invalid revision error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.List(ctx, "tenant-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled list error = %v", err)
	}
	large := TargetConfiguration{Payload: make([]byte, MaxTargetConfigurationBytes+1)}
	if _, err := service.Create(context.Background(), "tenant-a", descriptor, large, true); !errors.Is(err, ErrInvalidTargetConfiguration) {
		t.Fatalf("large config error = %v", err)
	}
}

func TestTargetServicePreservesStableErrorsAndContainsRepositoryPanics(t *testing.T) {
	service, descriptor, repository := targetServiceFixture(t)
	repository.err = ErrTargetDirectoryCapacity
	if _, err := service.ListRecords(context.Background(), "tenant-a"); !errors.Is(err, ErrTargetDirectoryCapacity) {
		t.Fatalf("capacity error=%v", err)
	}
	repository.err = ErrInvalidTargetRevision
	if _, err := service.Update(context.Background(), "tenant-a", descriptor, TargetConfiguration{}, true, "1"); !errors.Is(err, ErrInvalidTargetRevision) {
		t.Fatalf("revision error=%v", err)
	}
	repository.err = nil
	repository.panicCall = true
	if _, err := service.ListRecords(context.Background(), "tenant-a"); !errors.Is(err, ErrTargetRepository) {
		t.Fatalf("panic error=%v", err)
	}
}

func TestTargetServiceProviderValidatorFailsClosedBeforePersistence(t *testing.T) {
	channel := ChannelRef{ID: "webhook", Version: "1"}
	target, err := NewTargetRef("ops")
	if err != nil {
		t.Fatal(err)
	}
	repository := &targetRepositoryStub{}
	service, err := NewService(repository, []ChannelRef{channel}, targetConfigurationValidator{channel: channel})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := TargetDescriptor{Target: target, Channel: channel}
	if _, err := service.Create(context.Background(), "tenant-a", descriptor, TargetConfiguration{Payload: []byte(`{"provider":"unknown"}`)}, true); !errors.Is(err, ErrInvalidTargetConfiguration) {
		t.Fatalf("invalid provider configuration error=%v", err)
	}
	if repository.created.Payload != nil {
		t.Fatal("repository called for rejected provider configuration")
	}
	if _, err := service.Create(context.Background(), "tenant-a", descriptor, TargetConfiguration{Payload: []byte(`{"ok":true}`)}, true); err != nil {
		t.Fatal(err)
	}
}
