package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"sync"
)

// DynamicObjectStore wraps an ObjectStore whose backing implementation can be
// swapped at runtime (e.g. the console re-pointing resources from the
// embedded filesystem to S3). All calls forward to the current inner store;
// migration-aware activation is the only route change allowed while a
// migration is active.
//
// In-flight operations on the old store complete against the snapshot they
// acquired. A migration keeps local authoritative until final verification.
type DynamicObjectStore struct {
	mu        sync.RWMutex
	opMu      sync.RWMutex
	inner     ObjectStore
	label     string
	migration *dynamicMigration
}

type dynamicMigration struct {
	id       string
	target   ObjectStore
	label    string
	recorder ArtifactMutationRecorder
}

var ErrArtifactMigrationActive = errors.New("artifact migration owns the active route")

func NewDynamicObjectStore(inner ObjectStore, label string) *DynamicObjectStore {
	return &DynamicObjectStore{inner: inner, label: label}
}

// Swap preserves the legacy void API. It atomically replaces the inner store
// when no migration owns the route; during migration it leaves the route
// unchanged. New callers should use TrySwap to observe that error.
func (d *DynamicObjectStore) Swap(inner ObjectStore, label string) {
	_ = d.TrySwap(inner, label)
}

// TrySwap is the error-returning compatibility form of Swap. It refuses to
// bypass an active migration; callers applying storage configuration should
// use WithArtifactActivationBarrier instead.
func (d *DynamicObjectStore) TrySwap(inner ObjectStore, label string) error {
	d.opMu.Lock()
	defer d.opMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.migration != nil {
		return ErrArtifactMigrationActive
	}
	d.inner = inner
	d.label = label
	return nil
}

// BeginArtifactMigration keeps the current store authoritative while a
// candidate is copied and caught up. It returns the current source and the
// candidate so a copier can operate without exposing route internals.
func (d *DynamicObjectStore) BeginArtifactMigration(id string, target ObjectStore, label string, recorder ArtifactMutationRecorder) (ObjectStore, ObjectStore, error) {
	if id == "" || target == nil {
		return nil, nil, errors.New("migration id and target are required")
	}
	d.opMu.Lock()
	defer d.opMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inner == nil {
		return nil, nil, errors.New("dynamic object store has no active store")
	}
	if d.migration != nil {
		return nil, nil, errors.New("an artifact migration is already active")
	}
	d.migration = &dynamicMigration{id: id, target: target, label: label, recorder: recorder}
	return d.inner, target, nil
}

// ArtifactMigrationStores returns stable source and target references for an
// active migration. The source remains the active route until activation.
func (d *DynamicObjectStore) ArtifactMigrationStores(id string) (ObjectStore, ObjectStore, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.migration == nil || d.migration.id != id {
		return nil, nil, errors.New("artifact migration is not active")
	}
	return d.inner, d.migration.target, nil
}

// WithArtifactActivationBarrier blocks new operations, lets the caller apply
// and verify a finite final journal delta, then atomically switches the route
// only when verify returns nil. Full reconciliation belongs outside this short
// barrier. A failed verification leaves local active.
func (d *DynamicObjectStore) WithArtifactActivationBarrier(ctx context.Context, id string, verify func(context.Context, ObjectStore, ObjectStore) error) error {
	if verify == nil {
		return errors.New("activation verification is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.opMu.Lock()
	defer d.opMu.Unlock()
	d.mu.RLock()
	migration := d.migration
	source := d.inner
	if migration == nil || migration.id != id {
		d.mu.RUnlock()
		return errors.New("artifact migration is not active")
	}
	target, label := migration.target, migration.label
	d.mu.RUnlock()
	if err := verify(ctx, source, target); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.migration == nil || d.migration.id != id {
		return errors.New("artifact migration changed during activation")
	}
	d.inner = target
	d.label = label
	d.migration = nil
	return nil
}

// CancelArtifactMigration abandons a candidate without changing the active
// route. It is idempotent for an already-finished migration.
func (d *DynamicObjectStore) CancelArtifactMigration(id string) {
	d.opMu.Lock()
	defer d.opMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.migration != nil && d.migration.id == id {
		d.migration = nil
	}
}

// Label names the current backend ("embedded" | "s3" | custom).
func (d *DynamicObjectStore) Label() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.label
}

func (d *DynamicObjectStore) inner_() ObjectStore {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.inner
}

func (d *DynamicObjectStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	d.opMu.RLock()
	defer d.opMu.RUnlock()
	return d.inner_().Get(ctx, key)
}

func (d *DynamicObjectStore) Put(ctx context.Context, key string, data []byte, opts PutOptions) (string, error) {
	d.opMu.RLock()
	defer d.opMu.RUnlock()
	etag, err := d.inner_().Put(ctx, key, data, opts)
	if err == nil {
		err = d.recordMutation(ctx, ArtifactMutation{Operation: ArtifactMutationPut, Key: key, ETag: etag, Digest: digestBytes(data), Size: int64(len(data))})
	}
	return etag, err
}

func (d *DynamicObjectStore) Open(ctx context.Context, key string) (io.ReadCloser, string, int64, error) {
	// Snapshot the route under the operation gate, then release it before
	// opening the body. The barrier therefore blocks new Opens without being
	// held hostage by a long-lived reader returned by an earlier Open.
	d.opMu.RLock()
	store := d.inner_()
	d.opMu.RUnlock()
	streaming, ok := store.(StreamingObjectGetter)
	if !ok {
		return nil, "", 0, ErrStreamingUnsupported
	}
	return streaming.Open(ctx, key)
}

func (d *DynamicObjectStore) PutStream(ctx context.Context, key string, body io.Reader, maxBytes int64, opts PutOptions) (string, error) {
	d.opMu.RLock()
	defer d.opMu.RUnlock()
	store := d.inner_()
	streaming, ok := store.(StreamingObjectPutter)
	if !ok {
		return "", ErrStreamingUnsupported
	}
	digesting := &artifactDigestReader{reader: body, hash: sha256.New()}
	etag, err := streaming.PutStream(ctx, key, digesting, maxBytes, opts)
	if err == nil {
		err = d.recordMutation(ctx, ArtifactMutation{Operation: ArtifactMutationPut, Key: key, ETag: etag, Digest: digestHex(digesting.hash), Size: digesting.size})
	}
	return etag, err
}

func (d *DynamicObjectStore) Delete(ctx context.Context, key string) error {
	d.opMu.RLock()
	defer d.opMu.RUnlock()
	if err := d.inner_().Delete(ctx, key); err != nil {
		return err
	}
	return d.recordMutation(ctx, ArtifactMutation{Operation: ArtifactMutationDelete, Key: key, Size: -1})
}

func (d *DynamicObjectStore) List(ctx context.Context, prefix, startAfter string, limit int) ([]ObjectItem, error) {
	d.opMu.RLock()
	defer d.opMu.RUnlock()
	return d.inner_().List(ctx, prefix, startAfter, limit)
}

func (d *DynamicObjectStore) EnsureBucket(ctx context.Context) error {
	d.opMu.RLock()
	defer d.opMu.RUnlock()
	if bucket, ok := d.inner_().(*S3ObjectStore); ok {
		return bucket.EnsureBucket(ctx)
	}
	return nil
}

func (d *DynamicObjectStore) recordMutation(ctx context.Context, mutation ArtifactMutation) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.migration == nil || d.migration.recorder == nil {
		return nil
	}
	mutation.MigrationID = d.migration.id
	return d.migration.recorder.RecordArtifactMutation(ctx, mutation)
}

func digestBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

type artifactDigestReader struct {
	reader io.Reader
	hash   hash.Hash
	size   int64
}

func (r *artifactDigestReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.size += int64(n)
		_, _ = r.hash.Write(p[:n])
	}
	return n, err
}

func digestHex(h hash.Hash) string {
	return hex.EncodeToString(h.Sum(nil))
}
