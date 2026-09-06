package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type migrationRecorder struct {
	mu                   sync.Mutex
	mutations            []ArtifactMutation
	progresses           []ArtifactCopyProgress
	reconcileProgress    []ArtifactReconcileProgress
	reconcileProgressErr error
}

type countingStreamingStore struct {
	base  ObjectStore
	get   StreamingObjectGetter
	put   StreamingObjectPutter
	lists atomic.Int32
}

type countingFileWalker struct {
	*FileObjectStore
	walks atomic.Int32
}

func (s *countingFileWalker) walkObjects(ctx context.Context, prefix, startAfter string, fn func(ObjectItem) error) error {
	s.walks.Add(1)
	return s.FileObjectStore.walkObjects(ctx, prefix, startAfter, fn)
}

func (s *countingStreamingStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	return s.base.Get(ctx, key)
}
func (s *countingStreamingStore) Put(ctx context.Context, key string, data []byte, opts PutOptions) (string, error) {
	return s.base.Put(ctx, key, data, opts)
}
func (s *countingStreamingStore) Delete(ctx context.Context, key string) error {
	return s.base.Delete(ctx, key)
}
func (s *countingStreamingStore) List(ctx context.Context, prefix, startAfter string, limit int) ([]ObjectItem, error) {
	s.lists.Add(1)
	return s.base.List(ctx, prefix, startAfter, limit)
}
func (s *countingStreamingStore) Open(ctx context.Context, key string) (io.ReadCloser, string, int64, error) {
	return s.get.Open(ctx, key)
}
func (s *countingStreamingStore) PutStream(ctx context.Context, key string, body io.Reader, maxBytes int64, opts PutOptions) (string, error) {
	return s.put.PutStream(ctx, key, body, maxBytes, opts)
}

func (r *migrationRecorder) RecordArtifactMutation(_ context.Context, mutation ArtifactMutation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mutations = append(r.mutations, mutation)
	return nil
}

func (r *migrationRecorder) RecordArtifactCopyProgress(_ context.Context, progress ArtifactCopyProgress) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progresses = append(r.progresses, progress)
	return nil
}

func (r *migrationRecorder) RecordArtifactReconcileProgress(_ context.Context, progress ArtifactReconcileProgress) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reconcileProgress = append(r.reconcileProgress, progress)
	return r.reconcileProgressErr
}

func TestArtifactCopierStreamsInBoundedPagesAndReconciles(t *testing.T) {
	source, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for key, data := range map[string][]byte{"a/one": []byte("one"), "a/two": []byte("two"), "a/three": []byte("three")} {
		if _, err := source.Put(ctx, key, data, PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	recorder := &migrationRecorder{}
	copier, err := NewArtifactCopier(source, target, ArtifactCopyOptions{PageSize: 1, BatchSize: 2, MaxStreamingObjectBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := copier.Copy(ctx, "a", "", recorder)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Objects != 3 || stats.Bytes != 11 || stats.LastKey != "a/two" && stats.LastKey != "a/three" {
		t.Fatalf("copy stats=%+v", stats)
	}
	if got := len(recorder.progresses); got != 2 {
		t.Fatalf("progress batches=%d, want 2", got)
	}
	reconciled, err := copier.Reconcile(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.Objects != 3 || reconciled.Bytes != 11 {
		t.Fatalf("reconcile stats=%+v", reconciled)
	}
}

func TestArtifactCopierSingleWalkMatchesPagedResumePath(t *testing.T) {
	sourceBase, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a/one", "a/two", "b/one"} {
		if _, err := sourceBase.Put(context.Background(), key, []byte(key), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	walkerSource := &countingFileWalker{FileObjectStore: sourceBase}
	walkerTarget, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	walkerCopier, err := NewArtifactCopier(walkerSource, walkerTarget, ArtifactCopyOptions{PageSize: 1, BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	walkerStats, err := walkerCopier.Copy(ctx, "a/", "a/one", nil)
	if err != nil {
		t.Fatal(err)
	}
	if walkerSource.walks.Load() != 1 || walkerStats.Objects != 1 || walkerStats.LastKey != "a/two" {
		t.Fatalf("single walk=%d stats=%+v", walkerSource.walks.Load(), walkerStats)
	}

	pagedSourceBase, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a/one", "a/two", "b/one"} {
		if _, err := pagedSourceBase.Put(ctx, key, []byte(key), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	pagedTargetBase, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pagedSource := &countingStreamingStore{base: pagedSourceBase, get: pagedSourceBase, put: pagedSourceBase}
	pagedTarget := &countingStreamingStore{base: pagedTargetBase, get: pagedTargetBase, put: pagedTargetBase}
	pagedCopier, err := NewArtifactCopier(pagedSource, pagedTarget, ArtifactCopyOptions{PageSize: 1, BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	pagedStats, err := pagedCopier.Copy(ctx, "a/", "a/one", nil)
	if err != nil {
		t.Fatal(err)
	}
	if pagedStats.Objects != walkerStats.Objects || pagedStats.Bytes != walkerStats.Bytes || pagedStats.LastKey != walkerStats.LastKey {
		t.Fatalf("walker stats=%+v paged stats=%+v", walkerStats, pagedStats)
	}
	items, err := walkerTarget.List(ctx, "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	pagedItems, err := pagedTargetBase.List(ctx, "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != len(pagedItems) || len(items) != 1 || items[0].Key != pagedItems[0].Key {
		t.Fatalf("walker items=%+v paged items=%+v", items, pagedItems)
	}
}

func TestArtifactReconcileWithProgressReportsBoundedBatches(t *testing.T) {
	source, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for key, data := range map[string][]byte{"a/one": []byte("one"), "a/two": []byte("two"), "a/three": []byte("three")} {
		if _, err := source.Put(ctx, key, data, PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := target.Put(ctx, key, data, PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	recorder := &migrationRecorder{}
	copier, err := NewArtifactCopier(source, target, ArtifactCopyOptions{PageSize: 2, BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := copier.ReconcileWithProgress(ctx, "a", recorder)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Objects != 3 || stats.Bytes != 11 {
		t.Fatalf("stats=%+v", stats)
	}
	recorder.mu.Lock()
	progress := append([]ArtifactReconcileProgress(nil), recorder.reconcileProgress...)
	recorder.mu.Unlock()
	if len(progress) != 2 || progress[0].Objects != 2 || progress[0].LastKey != "a/three" || progress[1].Objects != 1 || progress[1].LastKey != "a/two" {
		t.Fatalf("reconcile progress=%+v", progress)
	}
}

func TestArtifactReconcileWithProgressStopsOnCallbackError(t *testing.T) {
	source, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for key, data := range map[string][]byte{"a/one": []byte("one"), "a/two": []byte("two"), "a/three": []byte("three")} {
		if _, err := source.Put(ctx, key, data, PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := target.Put(ctx, key, data, PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	want := errors.New("lease progress failed")
	recorder := &migrationRecorder{reconcileProgressErr: want}
	copier, err := NewArtifactCopier(source, target, ArtifactCopyOptions{PageSize: 2, BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := copier.ReconcileWithProgress(ctx, "a", recorder)
	if !errors.Is(err, want) {
		t.Fatalf("err=%v, want %v", err, want)
	}
	if stats.Objects != 1 {
		t.Fatalf("callback error processed %d objects, want 1", stats.Objects)
	}
	recorder.mu.Lock()
	progressCount := len(recorder.reconcileProgress)
	recorder.mu.Unlock()
	if progressCount != 1 {
		t.Fatalf("callback invoked %d times, want 1", progressCount)
	}
}

func TestArtifactStreamingAndMigrationAcceptObjectsAboveByteSliceLimit(t *testing.T) {
	source, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	wantSize := int64(MaxObjectBytes + 1)
	if _, err := source.PutStream(ctx, "large", &fixedByteReader{remaining: wantSize, value: 'x'}, 0, PutOptions{}); err != nil {
		t.Fatalf("streaming path rejected %d-byte object: %v", wantSize, err)
	}
	copier, err := NewArtifactCopier(source, target, ArtifactCopyOptions{PageSize: 1, BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := copier.Copy(ctx, "", "", nil)
	if err != nil || stats.Objects != 1 || stats.Bytes != wantSize {
		t.Fatalf("large copy stats=%+v err=%v", stats, err)
	}
	verified, err := copier.Reconcile(ctx, "")
	if err != nil || verified.Objects != 1 || verified.Bytes != wantSize {
		t.Fatalf("large reconcile stats=%+v err=%v", verified, err)
	}
	if _, _, err := source.Get(ctx, "large"); !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("byte-slice Get of large streamed object returned err=%v", err)
	}
	if _, err := source.Put(ctx, "large", []byte("small overwrite"), PutOptions{}); err != nil {
		t.Fatalf("small byte-slice overwrite of large streamed object failed: %v", err)
	}
}

func TestArtifactCopierRejectsMismatchAndExtraKeys(t *testing.T) {
	source, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := source.Put(ctx, "same", []byte("source"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Put(ctx, "same", []byte("target"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	copier, err := NewArtifactCopier(source, target, ArtifactCopyOptions{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := copier.Reconcile(ctx, ""); err == nil {
		t.Fatal("digest mismatch accepted")
	}
	if _, err := target.Put(ctx, "extra", []byte("extra"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := copier.Reconcile(ctx, ""); err == nil {
		t.Fatal("extra target key accepted")
	}
}

func TestDynamicArtifactMigrationKeepsLocalUntilVerifiedActivation(t *testing.T) {
	local, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := local.Put(ctx, "keep", []byte("local"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	dynamic := NewDynamicObjectStore(local, "embedded")
	recorder := &migrationRecorder{}
	source, candidate, err := dynamic.BeginArtifactMigration("migration-1", target, "s3", recorder)
	if err != nil {
		t.Fatal(err)
	}
	copier, err := NewArtifactCopier(source, candidate, ArtifactCopyOptions{PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := copier.Copy(ctx, "", "", recorder); err != nil {
		t.Fatal(err)
	}
	if _, err := dynamic.Put(ctx, "new", []byte("new"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := dynamic.TrySwap(target, "bypassed"); !errors.Is(err, ErrArtifactMigrationActive) {
		t.Fatalf("legacy swap bypassed migration: %v", err)
	}
	if err := dynamic.Delete(ctx, "keep"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := target.Get(ctx, "new"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("target changed before activation: %v", err)
	}
	if err := dynamic.WithArtifactActivationBarrier(ctx, "migration-1", func(ctx context.Context, source, target ObjectStore) error {
		// The upper migration service replays the captured final delta before
		// asking the router to prove and atomically activate the candidate.
		finalCopier, err := NewArtifactCopier(source, target, ArtifactCopyOptions{PageSize: 1})
		if err != nil {
			return err
		}
		return finalCopier.ApplyAndVerifyKeys(ctx, []ArtifactKeyChange{{Key: "keep", Deleted: true}, {Key: "new"}})
	}); err != nil {
		t.Fatal(err)
	}
	if dynamic.Label() != "s3" {
		t.Fatalf("label=%q", dynamic.Label())
	}
	if _, _, err := dynamic.Get(ctx, "new"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := dynamic.Get(ctx, "keep"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("route did not activate delete: %v", err)
	}
	if len(recorder.mutations) != 2 || recorder.mutations[0].MigrationID != "migration-1" {
		t.Fatalf("mutations=%+v", recorder.mutations)
	}
}

func TestDynamicArtifactMigrationFailureLeavesLocalActive(t *testing.T) {
	local, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dynamic := NewDynamicObjectStore(local, "embedded")
	if _, _, err := dynamic.BeginArtifactMigration("migration-2", target, "s3", nil); err != nil {
		t.Fatal(err)
	}
	want := errors.New("verification failed")
	if err := dynamic.WithArtifactActivationBarrier(context.Background(), "migration-2", func(context.Context, ObjectStore, ObjectStore) error { return want }); !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
	if dynamic.Label() != "embedded" {
		t.Fatalf("failed activation changed label=%q", dynamic.Label())
	}
	dynamic.CancelArtifactMigration("migration-2")
}

func TestDynamicArtifactMigrationCapturesConcurrentWritesOverwritesAndDeletes(t *testing.T) {
	local, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 16; i++ {
		key := fmt.Sprintf("seed/%02d", i)
		if _, err := local.Put(ctx, key, []byte("before"), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	dynamic := NewDynamicObjectStore(local, "embedded")
	recorder := &migrationRecorder{}
	if _, _, err := dynamic.BeginArtifactMigration("migration-concurrent", target, "s3", recorder); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		i := i
		group.Add(1)
		go func() {
			defer group.Done()
			key := fmt.Sprintf("seed/%02d", i)
			if _, err := dynamic.Put(ctx, key, []byte("after"), PutOptions{}); err != nil {
				t.Errorf("overwrite %s: %v", key, err)
			}
		}()
	}
	for i := 0; i < 16; i++ {
		i := i
		group.Add(1)
		go func() {
			defer group.Done()
			if err := dynamic.Delete(ctx, fmt.Sprintf("seed/%02d", i)); err != nil {
				t.Errorf("delete %02d: %v", i, err)
			}
		}()
	}
	group.Wait()
	if _, err := dynamic.PutStream(ctx, "streamed", strings.NewReader("streamed body"), 0, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	recorder.mu.Lock()
	mutations := append([]ArtifactMutation(nil), recorder.mutations...)
	recorder.mu.Unlock()
	if len(mutations) != 33 {
		t.Fatalf("captured %d mutations, want 33", len(mutations))
	}
	for _, mutation := range mutations {
		if mutation.MigrationID != "migration-concurrent" {
			t.Fatalf("mutation has wrong migration id: %+v", mutation)
		}
		if mutation.Operation == ArtifactMutationPut && mutation.Digest == "" {
			t.Fatalf("put mutation missing digest: %+v", mutation)
		}
	}
	dynamic.CancelArtifactMigration("migration-concurrent")
}

func TestDynamicArtifactActivationCommitsAfterSuccessfulCallbackCancellation(t *testing.T) {
	local, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dynamic := NewDynamicObjectStore(local, "embedded")
	if _, _, err := dynamic.BeginArtifactMigration("migration-cancel", target, "s3", nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := dynamic.WithArtifactActivationBarrier(ctx, "migration-cancel", func(context.Context, ObjectStore, ObjectStore) error {
		cancel()
		return nil
	}); err != nil {
		t.Fatalf("successful callback should commit despite cancellation: %v", err)
	}
	if dynamic.Label() != "s3" {
		t.Fatalf("route label=%q after successful callback cancellation", dynamic.Label())
	}
}

func TestArtifactActivationKeysDoNotListWholeStores(t *testing.T) {
	localBase, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	targetBase, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := localBase.Put(ctx, "key", []byte("value"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	local := &countingStreamingStore{base: localBase, get: localBase, put: localBase}
	target := &countingStreamingStore{base: targetBase, get: targetBase, put: targetBase}
	dynamic := NewDynamicObjectStore(local, "embedded")
	source, candidate, err := dynamic.BeginArtifactMigration("migration-keys", target, "s3", nil)
	if err != nil {
		t.Fatal(err)
	}
	copier, err := NewArtifactCopier(source, candidate, ArtifactCopyOptions{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := copier.Copy(ctx, "", "", nil); err != nil {
		t.Fatal(err)
	}
	local.lists.Store(0)
	target.lists.Store(0)
	if err := copier.ApplyAndVerifyKeys(ctx, []ArtifactKeyChange{{Key: "key"}}); err != nil {
		t.Fatal(err)
	}
	if got := local.lists.Load() + target.lists.Load(); got != 0 {
		t.Fatalf("finite activation verification listed stores %d times", got)
	}
}

func TestArtifactCopierRejectsUnboundedOptionsAndPages(t *testing.T) {
	local, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewArtifactCopier(local, local, ArtifactCopyOptions{PageSize: MaxArtifactCopyPageSize + 1}); err == nil {
		t.Fatal("oversized page accepted")
	}
	if _, err := NewArtifactCopier(local, local, ArtifactCopyOptions{BatchSize: MaxArtifactCopyBatchSize + 1}); err == nil {
		t.Fatal("oversized batch accepted")
	}
	if _, err := NewArtifactCopier(local, local, ArtifactCopyOptions{MaxStreamingObjectBytes: MaxObjectBytes + 1}); err != nil {
		t.Fatal(err)
	}
}
