package modelsettings

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type cacheRepo struct {
	mu      sync.Mutex
	value   StoredConfiguration
	found   bool
	err     error
	loads   atomic.Int32
	creates atomic.Int32
	block   chan struct{}
	started chan struct{}
}

type orderedSaveRepo struct {
	*cacheRepo
	firstStarted chan struct{}
	firstRelease chan struct{}
	saves        atomic.Int32
}

func (r *orderedSaveRepo) Save(ctx context.Context, value StoredConfiguration) error {
	call := r.saves.Add(1)
	if call == 1 {
		close(r.firstStarted)
		select {
		case <-r.firstRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return r.cacheRepo.Save(ctx, value)
}

func (r *cacheRepo) Load(ctx context.Context) (StoredConfiguration, bool, error) {
	r.loads.Add(1)
	if r.started != nil {
		select {
		case <-r.started:
		default:
			close(r.started)
		}
	}
	if r.block != nil {
		select {
		case <-r.block:
		case <-ctx.Done():
			return StoredConfiguration{}, false, ctx.Err()
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneCached(r.value), r.found, r.err
}
func (r *cacheRepo) Save(_ context.Context, value StoredConfiguration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.value, r.found = cloneCached(value), true
	return nil
}
func (r *cacheRepo) CreateIfAbsent(_ context.Context, value StoredConfiguration) (bool, error) {
	r.creates.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return false, r.err
	}
	if r.found {
		return false, nil
	}
	r.value, r.found = cloneCached(value), true
	return true, nil
}

func cacheConfig() StoredConfiguration {
	return StoredConfiguration{BaseURL: "https://example.test", APIKey: "secret", Model: "model-a", Provider: ProviderOpenAI, Protocol: ProtocolOpenAIChatCompletions, AllowedModels: []string{"model-a"}, Source: ConfigSourceDB}
}

func TestCachedRepositoryTTLAndFailClosed(t *testing.T) {
	now := time.Unix(100, 0)
	r := &cacheRepo{value: cacheConfig(), found: true}
	c, err := NewCachedRepository(r, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := c.Load(context.Background())
	if err != nil || got.APIKey != "secret" {
		t.Fatalf("first load=%#v err=%v", got, err)
	}
	r.mu.Lock()
	r.value.Model = "model-b"
	r.mu.Unlock()
	got, _, err = c.Load(context.Background())
	if err != nil || got.Model != "model-a" {
		t.Fatalf("cache miss before ttl=%#v err=%v", got, err)
	}
	now = now.Add(6 * time.Second)
	r.mu.Lock()
	r.err = errors.New("database down")
	r.mu.Unlock()
	if _, _, err = c.Load(context.Background()); err == nil {
		t.Fatal("expired cache returned stale value")
	}
}

func TestCachedRepositorySuppressesStampedeAndAllowsCancellation(t *testing.T) {
	r := &cacheRepo{value: cacheConfig(), found: true, block: make(chan struct{}), started: make(chan struct{})}
	c, err := NewCachedRepository(r, nil)
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { _, _, e := c.Load(context.Background()); firstDone <- e }()
	<-r.started
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := c.Load(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting load err=%v", err)
	}
	close(r.block)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if got := r.loads.Load(); got != 1 {
		t.Fatalf("loads=%d, want one", got)
	}
}

func TestCachedRepositoryCopiesAllowedModelsAndDelegatesAtomicCreate(t *testing.T) {
	r := &cacheRepo{}
	c, err := NewCachedRepository(r, nil)
	if err != nil {
		t.Fatal(err)
	}
	value := cacheConfig()
	if created, err := c.CreateIfAbsent(context.Background(), value); err != nil || !created {
		t.Fatalf("create=%v err=%v", created, err)
	}
	value.AllowedModels[0] = "mutated"
	got, _, err := c.Load(context.Background())
	if err != nil || got.AllowedModels[0] != "model-a" {
		t.Fatalf("alias leaked: %#v err=%v", got, err)
	}
	if got := r.creates.Load(); got != 1 {
		t.Fatalf("creates=%d", got)
	}
	value2 := cacheConfig()
	if created, err := c.CreateIfAbsent(context.Background(), value2); err != nil || created {
		t.Fatalf("second create=%v err=%v", created, err)
	}
	if got := r.creates.Load(); got != 2 {
		t.Fatalf("creates=%d", got)
	}
}

func TestCachedRepositorySavePublishesDefensiveSnapshot(t *testing.T) {
	r := &cacheRepo{value: cacheConfig(), found: true}
	c, err := NewCachedRepository(r, nil)
	if err != nil {
		t.Fatal(err)
	}
	value := cacheConfig()
	value.Model = "model-b"
	value.AllowedModels = []string{"model-b"}
	if err := c.Save(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	value.AllowedModels[0] = "changed-after-save"
	got, _, err := c.Load(context.Background())
	if err != nil || got.Model != "model-b" || got.AllowedModels[0] != "model-b" {
		t.Fatalf("saved snapshot=%#v err=%v", got, err)
	}
}

func TestCachedRepositoryMutationCannotBeOverwrittenByInflightLoad(t *testing.T) {
	r := &cacheRepo{value: cacheConfig(), found: true, block: make(chan struct{}), started: make(chan struct{})}
	c, err := NewCachedRepository(r, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan StoredConfiguration, 1)
	go func() {
		value, _, loadErr := c.Load(context.Background())
		if loadErr != nil {
			t.Errorf("load: %v", loadErr)
		}
		result <- value
	}()
	<-r.started
	updated := cacheConfig()
	updated.Model, updated.AllowedModels = "model-b", []string{"model-b"}
	if err := c.Save(context.Background(), updated); err != nil {
		t.Fatal(err)
	}
	close(r.block)
	if got := <-result; got.Model != "model-b" || got.AllowedModels[0] != "model-b" {
		t.Fatalf("inflight load regressed after save: %#v", got)
	}
}

func TestCachedRepositorySerializesConcurrentMutations(t *testing.T) {
	base := &orderedSaveRepo{cacheRepo: &cacheRepo{value: cacheConfig(), found: true}, firstStarted: make(chan struct{}), firstRelease: make(chan struct{})}
	c, err := NewCachedRepository(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	first := cacheConfig()
	first.Model, first.AllowedModels = "model-b", []string{"model-b"}
	second := cacheConfig()
	second.Model, second.AllowedModels = "model-c", []string{"model-c"}
	firstDone := make(chan error, 1)
	go func() { firstDone <- c.Save(context.Background(), first) }()
	<-base.firstStarted
	secondDone := make(chan error, 1)
	go func() { secondDone <- c.Save(context.Background(), second) }()
	deadline := time.After(30 * time.Millisecond)
	select {
	case <-deadline:
	case <-secondDone:
		t.Fatal("second mutation passed the first mutation barrier")
	}
	close(base.firstRelease)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	got, _, err := c.Load(context.Background())
	base.mu.Lock()
	baseModel := base.value.Model
	base.mu.Unlock()
	if err != nil || got.Model != "model-c" || baseModel != "model-c" {
		t.Fatalf("final cache/base diverged: cache=%#v base_model=%q err=%v", got, baseModel, err)
	}
}

func BenchmarkCachedRepositoryLoad(b *testing.B) {
	r := &cacheRepo{value: cacheConfig(), found: true}
	c, err := NewCachedRepository(r, nil)
	if err != nil {
		b.Fatal(err)
	}
	if _, _, err := c.Load(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := c.Load(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
