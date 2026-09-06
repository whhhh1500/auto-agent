package storage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func newCommittedColdSession(t *testing.T) (*FileObjectStore, *S3SessionStore, *core.Session) {
	t.Helper()
	objects, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewS3SessionStore(objects)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, user := testScopes()
	session := mustSession(t, user, testPrincipal(user))
	ctx := context.Background()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-a", core.EvUserMessage, core.UserMessageData{Text: "before cold storage"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, session, 0); err != nil {
		t.Fatal(err)
	}
	return objects, store, session
}

type coldStreamingSpy struct {
	inner          *FileObjectStore
	getCalls       int
	putStreamCalls int
	openErr        error
	putErr         error
	blockPut       bool
}

func (s *coldStreamingSpy) Get(ctx context.Context, key string) ([]byte, string, error) {
	s.getCalls++
	return s.inner.Get(ctx, key)
}
func (s *coldStreamingSpy) Put(ctx context.Context, key string, data []byte, opts PutOptions) (string, error) {
	return s.inner.Put(ctx, key, data, opts)
}
func (s *coldStreamingSpy) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}
func (s *coldStreamingSpy) List(ctx context.Context, prefix, startAfter string, limit int) ([]ObjectItem, error) {
	return s.inner.List(ctx, prefix, startAfter, limit)
}
func (s *coldStreamingSpy) Open(ctx context.Context, key string) (io.ReadCloser, string, int64, error) {
	if s.openErr != nil {
		return nil, "", 0, s.openErr
	}
	return s.inner.Open(ctx, key)
}
func (s *coldStreamingSpy) PutStream(ctx context.Context, key string, body io.Reader, maxBytes int64, opts PutOptions) (string, error) {
	s.putStreamCalls++
	if s.putErr != nil {
		return "", s.putErr
	}
	if s.blockPut {
		<-ctx.Done()
		return "", ctx.Err()
	}
	return s.inner.PutStream(ctx, key, body, maxBytes, opts)
}
func (s *coldStreamingSpy) deleteColdPlain(ctx context.Context, key string) error {
	return s.inner.deleteColdPlain(ctx, key)
}

func TestColdSweeperUsesStreamingSeamsWithoutGetForEvent(t *testing.T) {
	objects, _, session := newCommittedColdSession(t)
	spy := &coldStreamingSpy{inner: objects}
	key := chunkKey(session.ID(), 0)
	if swept, err := newColdSweeper(spy, 10).Sweep(context.Background()); err != nil || swept != 1 {
		t.Fatalf("streaming sweep=%d err=%v", swept, err)
	}
	if spy.getCalls != 1 {
		t.Fatalf("streaming cold sweep Get calls=%d, want metadata-only lookup", spy.getCalls)
	}
	if spy.putStreamCalls != 1 {
		t.Fatalf("streaming cold sweep PutStream calls=%d, want 1", spy.putStreamCalls)
	}
	compressed, _, err := objects.Get(context.Background(), key+".zst")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecompressZstd(compressed)
	if err != nil || len(decoded) == 0 {
		t.Fatalf("compressed event decode err=%v bytes=%d", err, len(decoded))
	}
}

func TestColdSweeperStreamingPutFailureDoesNotDeleteSourceOrLeak(t *testing.T) {
	objects, _, session := newCommittedColdSession(t)
	key := chunkKey(session.ID(), 0)
	spy := &coldStreamingSpy{inner: objects, putErr: errors.New("injected stream put failure")}
	done := make(chan struct{})
	go func() {
		_, _ = newColdSweeper(spy, 10).Sweep(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("streaming put failure deadlocked")
	}
	if _, _, err := objects.Get(context.Background(), key); err != nil {
		t.Fatalf("source was deleted after streaming put failure: %v", err)
	}
}

func TestColdSweeperCancellationPreservesSource(t *testing.T) {
	objects, _, session := newCommittedColdSession(t)
	key := chunkKey(session.ID(), 0)
	spy := &coldStreamingSpy{inner: objects, blockPut: true}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := newColdSweeper(spy, 10).Sweep(ctx)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error=%v, want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation deadlocked")
	}
	if _, _, err := objects.Get(context.Background(), key); err != nil {
		t.Fatalf("source was deleted after cancellation: %v", err)
	}
}

func TestColdSweeperOpenCancellationDoesNotFallbackToGet(t *testing.T) {
	objects, _, _ := newCommittedColdSession(t)
	spy := &coldStreamingSpy{inner: objects, openErr: context.Canceled}
	_, err := newColdSweeper(spy, 10).Sweep(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Open cancellation error=%v, want context canceled", err)
	}
	if spy.getCalls != 1 {
		t.Fatalf("Open cancellation unexpectedly used Get calls=%d, want metadata-only lookup", spy.getCalls)
	}
}

func objectPath(t *testing.T, store *FileObjectStore, key string) string {
	t.Helper()
	path, err := store.path(key)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func newColdSweeper(store ObjectStore, limit int) *ColdSweeper {
	return &ColdSweeper{
		Store: store, OlderThan: -time.Minute, Prefixes: []string{s3SessionsPrefix}, Limit: limit,
	}
}

func TestColdSweeperKeepsCommittedSessionEventsLoadableAndAppendable(t *testing.T) {
	objects, store, session := newCommittedColdSession(t)
	ctx := context.Background()
	key := chunkKey(session.ID(), 0)

	swept, err := newColdSweeper(objects, 10).Sweep(ctx)
	if err != nil || swept != 1 {
		t.Fatalf("sweep = %d, %v; want one committed chunk", swept, err)
	}
	if _, err := os.Stat(objectPath(t, objects, key)); !os.IsNotExist(err) {
		t.Fatalf("plain event must be removed after cold sweep: %v", err)
	}

	loaded, err := store.Load(ctx, session.ID())
	if err != nil || loaded.Version() != 1 {
		t.Fatalf("load cold session version=%d err=%v; want 1", loaded.Version(), err)
	}
	if _, err := loaded.Append("run-a", core.EvUserMessage, core.UserMessageData{Text: "after cold storage"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, loaded, 1); err != nil {
		t.Fatalf("save after cold load: %v", err)
	}
	reloaded, err := store.Load(ctx, session.ID())
	if err != nil || reloaded.Version() != 2 {
		t.Fatalf("reloaded version=%d err=%v; want 2", reloaded.Version(), err)
	}
}

func TestColdSweeperOnlyCompressesCommittedSessionEventChunks(t *testing.T) {
	objects, _, session := newCommittedColdSession(t)
	ctx := context.Background()
	resourceKey := "sessions/" + session.ID() + "/resources/note.txt"
	if _, err := objects.Put(ctx, resourceKey, []byte("mutable resource"), PutOptions{}); err != nil {
		t.Fatal(err)
	}

	swept, err := newColdSweeper(objects, 10).Sweep(ctx)
	if err != nil || swept != 1 {
		t.Fatalf("sweep = %d, %v; want exactly one event chunk", swept, err)
	}
	for _, key := range []string{s3MetaKey(session.ID()), s3EvidenceKey(session.ID()), resourceKey} {
		if _, err := os.Stat(objectPath(t, objects, key)); err != nil {
			t.Fatalf("%s must remain plain: %v", key, err)
		}
		if _, err := os.Stat(objectPath(t, objects, key) + ".zst"); !os.IsNotExist(err) {
			t.Fatalf("%s must not gain a cold sibling: %v", key, err)
		}
	}
	eventKey := chunkKey(session.ID(), 0)
	if _, err := os.Stat(objectPath(t, objects, eventKey) + ".zst"); err != nil {
		t.Fatalf("committed event must gain a cold sibling: %v", err)
	}
}

func TestColdSweeperLeavesOrphanAndInitializingEventChunksPlain(t *testing.T) {
	t.Run("orphan at committed version", func(t *testing.T) {
		objects, _, session := newCommittedColdSession(t)
		ctx := context.Background()
		orphanKey := chunkKey(session.ID(), 1) // meta.Version is one.
		if _, err := objects.Put(ctx, orphanKey, []byte("orphan event chunk"), PutOptions{}); err != nil {
			t.Fatal(err)
		}

		swept, err := newColdSweeper(objects, 10).Sweep(ctx)
		if err != nil || swept != 1 {
			t.Fatalf("sweep = %d, %v; want only the committed chunk", swept, err)
		}
		if _, err := os.Stat(objectPath(t, objects, orphanKey)); err != nil {
			t.Fatalf("orphan chunk must remain plain: %v", err)
		}
		if _, err := os.Stat(objectPath(t, objects, orphanKey) + ".zst"); !os.IsNotExist(err) {
			t.Fatalf("orphan chunk must not gain a cold sibling: %v", err)
		}
	})

	t.Run("initializing metadata", func(t *testing.T) {
		objects, _, session := newCommittedColdSession(t)
		ctx := context.Background()
		metaPayload, _, err := objects.Get(ctx, s3MetaKey(session.ID()))
		if err != nil {
			t.Fatal(err)
		}
		var meta s3SessionMeta
		if err := json.Unmarshal(metaPayload, &meta); err != nil {
			t.Fatal(err)
		}
		meta.Initializing = true
		metaPayload, err = json.Marshal(meta)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := objects.Put(ctx, s3MetaKey(session.ID()), metaPayload, PutOptions{}); err != nil {
			t.Fatal(err)
		}

		swept, err := newColdSweeper(objects, 10).Sweep(ctx)
		if err != nil || swept != 0 {
			t.Fatalf("initializing sweep = %d, %v; want no compression", swept, err)
		}
		eventKey := chunkKey(session.ID(), 0)
		if _, err := os.Stat(objectPath(t, objects, eventKey)); err != nil {
			t.Fatalf("initializing event must remain plain: %v", err)
		}
		if _, err := os.Stat(objectPath(t, objects, eventKey) + ".zst"); !os.IsNotExist(err) {
			t.Fatalf("initializing event must not gain a cold sibling: %v", err)
		}
	})
}

func TestColdSweeperPagesPastIneligibleObjects(t *testing.T) {
	objects, _, session := newCommittedColdSession(t)
	ctx := context.Background()
	// This object is first in lexical order but has no matching committed meta.
	ineligible := "sessions/000-invalid/events/000000000000.jsonl"
	if _, err := objects.Put(ctx, ineligible, []byte("not a committed session event"), PutOptions{}); err != nil {
		t.Fatal(err)
	}

	swept, err := newColdSweeper(objects, 1).Sweep(ctx)
	if err != nil || swept != 1 {
		t.Fatalf("paged sweep = %d, %v; want later eligible event", swept, err)
	}
	if _, err := os.Stat(objectPath(t, objects, chunkKey(session.ID(), 0))); !os.IsNotExist(err) {
		t.Fatalf("eligible event must be reached after ineligible first page: %v", err)
	}
	if _, err := os.Stat(objectPath(t, objects, ineligible)); err != nil {
		t.Fatalf("ineligible object must stay plain: %v", err)
	}
}

type failFirstDeleteStore struct {
	ObjectStore
	key    string
	failed bool
}

func (s *failFirstDeleteStore) deleteColdPlain(ctx context.Context, key string) error {
	if key == s.key && !s.failed {
		s.failed = true
		return errors.New("injected delete interruption")
	}
	return deleteColdPlain(ctx, s.ObjectStore, key)
}

func TestColdSweeperRetriesInterruptedCompressionIdempotently(t *testing.T) {
	objects, sessions, session := newCommittedColdSession(t)
	ctx := context.Background()
	key := chunkKey(session.ID(), 0)
	interrupted := &failFirstDeleteStore{ObjectStore: objects, key: key}
	sweeper := newColdSweeper(interrupted, 10)

	swept, err := sweeper.Sweep(ctx)
	if err == nil || swept != 0 {
		t.Fatalf("interrupted sweep = %d, %v; want zero plus delete error", swept, err)
	}
	if _, err := os.Stat(objectPath(t, objects, key)); err != nil {
		t.Fatalf("plain object must survive failed delete: %v", err)
	}
	if _, err := os.Stat(objectPath(t, objects, key) + ".zst"); err != nil {
		t.Fatalf("compressed sibling must survive failed delete: %v", err)
	}
	loaded, err := sessions.Load(ctx, session.ID())
	if err != nil || loaded.Version() != 1 {
		t.Fatalf("load with plain and cold sibling version=%d err=%v; want one logical chunk", loaded.Version(), err)
	}

	swept, err = sweeper.Sweep(ctx)
	if err != nil || swept != 1 {
		t.Fatalf("retry sweep = %d, %v; want one completed delete", swept, err)
	}
	if _, err := os.Stat(objectPath(t, objects, key)); !os.IsNotExist(err) {
		t.Fatalf("plain event must be removed by retry: %v", err)
	}
	if got, _, err := objects.Get(ctx, key); err != nil || len(got) == 0 {
		t.Fatalf("retried cold event must remain readable: %q %v", got, err)
	}
}

func TestObjectStoresDoNotReviveMutableObjectsFromZstdSiblings(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		store, err := NewFileObjectStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		assertMutableObjectDoesNotRevive(t, store)
	})
	t.Run("s3", func(t *testing.T) {
		server := newFakeS3()
		httpServer := httptest.NewServer(server)
		t.Cleanup(httpServer.Close)
		store, err := NewS3ObjectStore(S3Config{
			Endpoint: httpServer.URL, Region: "us-east-1", Bucket: "test-bucket",
			AccessKey: "test-key", SecretKey: "test-secret", PathStyle: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		assertMutableObjectDoesNotRevive(t, store)
	})
}

func TestObjectStoresRejectConditionalWritesToColdSessionEvents(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		store, err := NewFileObjectStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		assertColdEventConditionalPutFailsClosed(t, store)
	})
	t.Run("s3", func(t *testing.T) {
		server := newFakeS3()
		httpServer := httptest.NewServer(server)
		t.Cleanup(httpServer.Close)
		store, err := NewS3ObjectStore(S3Config{
			Endpoint: httpServer.URL, Region: "us-east-1", Bucket: "test-bucket",
			AccessKey: "test-key", SecretKey: "test-secret", PathStyle: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		assertColdEventConditionalPutFailsClosed(t, store)
	})
}

func TestObjectStoresDeleteCompleteColdSessionEvent(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		store, err := NewFileObjectStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		assertDeleteCompleteColdEvent(t, store)
	})
	t.Run("s3", func(t *testing.T) {
		server := newFakeS3()
		httpServer := httptest.NewServer(server)
		t.Cleanup(httpServer.Close)
		store, err := NewS3ObjectStore(S3Config{
			Endpoint: httpServer.URL, Region: "us-east-1", Bucket: "test-bucket",
			AccessKey: "test-key", SecretKey: "test-secret", PathStyle: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		assertDeleteCompleteColdEvent(t, store)
	})
}

func assertDeleteCompleteColdEvent(t *testing.T, store ObjectStore) {
	t.Helper()
	ctx := context.Background()
	key := "sessions/session-a/events/000000000000.jsonl"
	if _, err := store.Put(ctx, key+".zst", zstdEncoder.EncodeAll([]byte("event"), nil), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	assertMissingObject(t, ctx, store, key)
	assertMissingObject(t, ctx, store, key+".zst")
}

func TestObjectStoresPreserveConditionalWritesToPlainSessionEvents(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		store, err := NewFileObjectStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		assertPlainEventConditionalPutWorks(t, store)
	})
	t.Run("s3", func(t *testing.T) {
		server := newFakeS3()
		httpServer := httptest.NewServer(server)
		t.Cleanup(httpServer.Close)
		store, err := NewS3ObjectStore(S3Config{
			Endpoint: httpServer.URL, Region: "us-east-1", Bucket: "test-bucket",
			AccessKey: "test-key", SecretKey: "test-secret", PathStyle: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		assertPlainEventConditionalPutWorks(t, store)
	})
}

func assertPlainEventConditionalPutWorks(t *testing.T, store ObjectStore) {
	t.Helper()
	ctx := context.Background()
	key := "sessions/session-a/events/000000000000.jsonl"
	etag, err := store.Put(ctx, key, []byte("plain event"), PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate the safe interrupted-sweep state where plain and cold forms
	// coexist. The plain object remains the conditional-write authority.
	if _, err := store.Put(ctx, key+".zst", zstdEncoder.EncodeAll([]byte("plain event"), nil), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, key, []byte("replacement"), PutOptions{IfMatch: etag}); err != nil {
		t.Fatalf("matching conditional event PUT: %v", err)
	}
	if _, err := store.Put(ctx, key, []byte("duplicate"), PutOptions{IfNoneMatchStar: true}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("existing plain event create-only PUT = %v, want ErrPreconditionFailed", err)
	}
	if got, _, err := store.Get(ctx, key); err != nil || string(got) != "replacement" {
		t.Fatalf("conditional event replacement = %q, %v", got, err)
	}
	if _, _, err := store.Get(ctx, key+".zst"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("successful plain replacement must clean interrupted cold sibling: %v", err)
	}
}

func assertColdEventConditionalPutFailsClosed(t *testing.T, store ObjectStore) {
	t.Helper()
	ctx := context.Background()
	key := "sessions/session-a/events/000000000000.jsonl"
	original := []byte("immutable cold event")
	if _, err := store.Put(ctx, key+".zst", zstdEncoder.EncodeAll(original, nil), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	got, etag, err := store.Get(ctx, key)
	if err != nil || string(got) != string(original) {
		t.Fatalf("cold event initial read = %q, %v", got, err)
	}
	for _, options := range []PutOptions{
		{IfNoneMatchStar: true},
		{IfMatch: etag},
	} {
		if _, err := store.Put(ctx, key, []byte("overwrite"), options); !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("conditional cold-event PUT = %v, want ErrPreconditionFailed", err)
		}
	}
	got, _, err = store.Get(ctx, key)
	if err != nil || string(got) != string(original) {
		t.Fatalf("conditional PUT must preserve cold event = %q, %v", got, err)
	}
	items, err := store.List(ctx, key, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if item.Key == key {
			t.Fatalf("conditional cold-event PUT must not create plain primary %q", key)
		}
	}
	compressed, _, err := store.Get(ctx, key+".zst")
	if err != nil {
		t.Fatalf("cold sibling must remain present: %v", err)
	}
	decoded, err := DecompressZstd(compressed)
	if err != nil || string(decoded) != string(original) {
		t.Fatalf("cold sibling changed after conditional PUT: %q, %v", decoded, err)
	}
}

func assertMutableObjectDoesNotRevive(t *testing.T, store ObjectStore) {
	t.Helper()
	ctx := context.Background()
	key := "tenants/acme/resources/mutable.json"
	firstETag, err := store.Put(ctx, key, []byte("one"), PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, key+".zst", zstdEncoder.EncodeAll([]byte("stale"), nil), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	secondETag, err := store.Put(ctx, key, []byte("two"), PutOptions{})
	if err != nil {
		t.Fatalf("unconditional overwrite: %v", err)
	}
	if _, err := store.Put(ctx, key, []byte("three"), PutOptions{IfMatch: secondETag}); err != nil {
		t.Fatalf("matching conditional overwrite: %v", err)
	}
	if _, err := store.Put(ctx, key, []byte("lost"), PutOptions{IfMatch: firstETag}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("stale conditional overwrite must keep CAS semantics, got %v", err)
	}
	data, _, err := store.Get(ctx, key)
	if err != nil || string(data) != "three" {
		t.Fatalf("mutable overwrite read = %q, %v; want three", data, err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Get(ctx, key); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("deleted mutable object must not revive from stale .zst: %v", err)
	}
	createKey := "tenants/acme/resources/create-only.json"
	if _, err := store.Put(ctx, createKey+".zst", zstdEncoder.EncodeAll([]byte("stale"), nil), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, createKey, []byte("created"), PutOptions{IfNoneMatchStar: true}); err != nil {
		t.Fatalf("create-only PUT must succeed with only a stale sidecar: %v", err)
	}
	if _, err := store.Put(ctx, createKey, []byte("duplicate"), PutOptions{IfNoneMatchStar: true}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("duplicate create-only PUT must conflict, got %v", err)
	}
	if _, _, err := store.Get(ctx, createKey+".zst"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("successful primary PUT must clean its stale sidecar, got %v", err)
	}
}

func TestFileObjectStoreCleanupFailureDoesNotBreakPutOrCAS(t *testing.T) {
	store, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "tenants/acme/resources/cleanup.json"
	firstETag, err := store.Put(ctx, key, []byte("one"), PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	siblingPath := objectPath(t, store, key) + ".zst"
	if err := os.MkdirAll(siblingPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(siblingPath, "keep"), []byte("block cleanup"), 0o644); err != nil {
		t.Fatal(err)
	}
	secondETag, err := store.Put(ctx, key, []byte("two"), PutOptions{})
	if err != nil {
		t.Fatalf("unconditional PUT must succeed despite cleanup failure: %v", err)
	}
	if _, err := store.Put(ctx, key, []byte("three"), PutOptions{IfMatch: secondETag}); err != nil {
		t.Fatalf("conditional PUT must succeed despite cleanup failure: %v", err)
	}
	if _, err := store.Put(ctx, key, []byte("lost"), PutOptions{IfMatch: firstETag}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("stale conditional PUT must conflict, got %v", err)
	}
	if data, _, err := store.Get(ctx, key); err != nil || string(data) != "three" {
		t.Fatalf("successful writes must persist primary data: %q %v", data, err)
	}
	if err := store.Delete(ctx, key); err == nil {
		t.Fatal("Delete must retain plain key when stale-sidecar cleanup fails")
	}
	if data, _, err := store.Get(ctx, key); err != nil || string(data) != "three" {
		t.Fatalf("failed Delete must preserve primary data: %q %v", data, err)
	}
	if err := os.RemoveAll(siblingPath); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, key+".zst", zstdEncoder.EncodeAll([]byte("stale"), nil), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("normal Delete must remove sidecar then primary: %v", err)
	}
	assertMissingObject(t, ctx, store, key)
	assertMissingObject(t, ctx, store, key+".zst")
}

func TestS3ObjectStoreCleanupFailureDoesNotBreakPutOrCAS(t *testing.T) {
	server := newFakeS3()
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	store, err := NewS3ObjectStore(S3Config{
		Endpoint: httpServer.URL, Region: "us-east-1", Bucket: "test-bucket",
		AccessKey: "test-key", SecretKey: "test-secret", PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "tenants/acme/resources/cleanup.json"
	firstETag, err := store.Put(ctx, key, []byte("one"), PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, key+".zst", zstdEncoder.EncodeAll([]byte("stale"), nil), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	server.deleteFailures[key+".zst"] = 3 // two PUT cleanups, then one Delete cleanup
	server.mu.Unlock()
	secondETag, err := store.Put(ctx, key, []byte("two"), PutOptions{})
	if err != nil {
		t.Fatalf("unconditional PUT must succeed despite cleanup failure: %v", err)
	}
	if _, err := store.Put(ctx, key, []byte("three"), PutOptions{IfMatch: secondETag}); err != nil {
		t.Fatalf("conditional PUT must succeed despite cleanup failure: %v", err)
	}
	if _, err := store.Put(ctx, key, []byte("lost"), PutOptions{IfMatch: firstETag}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("stale conditional PUT must conflict, got %v", err)
	}
	if data, _, err := store.Get(ctx, key); err != nil || string(data) != "three" {
		t.Fatalf("successful writes must persist primary data: %q %v", data, err)
	}
	if err := store.Delete(ctx, key); err == nil {
		t.Fatal("Delete must retain plain key when stale-sidecar cleanup fails")
	}
	if data, _, err := store.Get(ctx, key); err != nil || string(data) != "three" {
		t.Fatalf("failed Delete must preserve primary data: %q %v", data, err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("normal Delete must remove sidecar then primary: %v", err)
	}
	assertMissingObject(t, ctx, store, key)
	assertMissingObject(t, ctx, store, key+".zst")
}

func assertMissingObject(t *testing.T, ctx context.Context, store ObjectStore, key string) {
	t.Helper()
	if _, _, err := store.Get(ctx, key); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("%s must be missing, got %v", key, err)
	}
}

func TestS3ObjectStoreReadsCompressedSessionEvent(t *testing.T) {
	server := newFakeS3()
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	store, err := NewS3ObjectStore(S3Config{
		Endpoint: httpServer.URL, Region: "us-east-1", Bucket: "test-bucket",
		AccessKey: "test-key", SecretKey: "test-secret", PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "sessions/session-a/events/000000000000.jsonl"
	if _, err := store.Put(ctx, key+".zst", zstdEncoder.EncodeAll([]byte("event"), nil), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	data, _, err := store.Get(ctx, key)
	if err != nil || string(data) != "event" {
		t.Fatalf("compressed event read = %q, %v", data, err)
	}
}
