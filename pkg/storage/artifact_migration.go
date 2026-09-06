package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"sort"
)

// ArtifactMutationOperation is the small, storage-neutral vocabulary used by
// a migration-aware route to describe successful authoritative mutations.
type ArtifactMutationOperation string

const (
	ArtifactMutationPut    ArtifactMutationOperation = "put"
	ArtifactMutationDelete ArtifactMutationOperation = "delete"
)

// ArtifactMutation is deliberately free of database types and object bodies.
// A durable adapter may assign its own sequence while recording it.
type ArtifactMutation struct {
	MigrationID string
	Operation   ArtifactMutationOperation
	Key         string
	ETag        string
	Digest      string
	Size        int64
}

// ArtifactMutationRecorder is the neutral injection port for a durable
// mutation journal. It is called only after the local operation has committed.
type ArtifactMutationRecorder interface {
	RecordArtifactMutation(context.Context, ArtifactMutation) error
}

// ArtifactCopyProgress is emitted after each bounded copy batch. LastKey is a
// resumable lexical cursor and never contains object content.
type ArtifactCopyProgress struct {
	LastKey string
	Objects int
	Bytes   int64
}

// ArtifactCopyProgressRecorder is optional and intentionally independent of
// the application or SQL migration packages.
type ArtifactCopyProgressRecorder interface {
	RecordArtifactCopyProgress(context.Context, ArtifactCopyProgress) error
}

type ArtifactCopyOptions struct {
	PageSize                int
	BatchSize               int
	MaxStreamingObjectBytes int64
}

const (
	DefaultArtifactCopyPageSize  = 32
	DefaultArtifactCopyBatchSize = 8
	MaxArtifactCopyPageSize      = 128
	MaxArtifactCopyBatchSize     = 32
)

type ArtifactCopyStats struct {
	LastKey string
	Objects int
	Bytes   int64
}

type ArtifactReconcileStats struct {
	Objects int
	Bytes   int64
}

// ArtifactReconcileProgress is emitted after each bounded verification batch.
// LastKey is a resumable lexical cursor; it never contains object content.
type ArtifactReconcileProgress struct {
	LastKey string
	Objects int
	Bytes   int64
}

// ArtifactReconcileProgressRecorder is optional and lets a long-running
// complete reconciliation renew a lease or persist bounded progress without
// coupling storage to an application or database package.
type ArtifactReconcileProgressRecorder interface {
	RecordArtifactReconcileProgress(context.Context, ArtifactReconcileProgress) error
}

// ArtifactKeyChange is one final-journal key. Deleted keys are proved absent
// from the source and target; other keys are copied from source and verified
// against target. The slice is intentionally bounded for use inside the short
// activation barrier.
type ArtifactKeyChange struct {
	Key     string
	Deleted bool
}

const MaxArtifactActivationKeys = 256

// ArtifactCopier copies one authoritative store to one candidate store with
// bounded pages and a single sequential worker. It requires both streaming
// seams so a large object is never materialized in process memory.
type ArtifactCopier struct {
	source ObjectStore
	target ObjectStore
	page   int
	batch  int
	max    int64
}

// artifactObjectWalker is an internal optimization seam. It deliberately
// remains unexported so ObjectStore implementations keep their small public
// contract; FileObjectStore can walk one lexical stream while other stores
// continue using the paged List compatibility path.
type artifactObjectWalker interface {
	walkObjects(context.Context, string, string, func(ObjectItem) error) error
}

var errArtifactWalkStop = errors.New("artifact walk stopped")

func NewArtifactCopier(source, target ObjectStore, options ArtifactCopyOptions) (*ArtifactCopier, error) {
	if source == nil || target == nil {
		return nil, errors.New("artifact copier source and target are required")
	}
	page := options.PageSize
	if page == 0 {
		page = DefaultArtifactCopyPageSize
	}
	if page < 1 || page > MaxArtifactCopyPageSize {
		return nil, fmt.Errorf("artifact copy page size must be between 1 and %d", MaxArtifactCopyPageSize)
	}
	batch := options.BatchSize
	if batch == 0 {
		batch = DefaultArtifactCopyBatchSize
	}
	if batch < 1 || batch > MaxArtifactCopyBatchSize {
		return nil, fmt.Errorf("artifact copy batch size must be between 1 and %d", MaxArtifactCopyBatchSize)
	}
	max := options.MaxStreamingObjectBytes
	if max <= 0 || max > MaxStreamingObjectBytes {
		max = MaxStreamingObjectBytes
	}
	return &ArtifactCopier{source: source, target: target, page: page, batch: batch, max: max}, nil
}

// Copy resumes strictly after startAfter. It reports progress only after a
// complete bounded batch; a failed report stops the copy without changing the
// active route.
func (c *ArtifactCopier) Copy(ctx context.Context, prefix, startAfter string, reporter ArtifactCopyProgressRecorder) (ArtifactCopyStats, error) {
	if c == nil || c.source == nil || c.target == nil {
		return ArtifactCopyStats{}, errors.New("artifact copier is not initialized")
	}
	getter, ok := c.source.(StreamingObjectGetter)
	if !ok {
		return ArtifactCopyStats{}, ErrStreamingUnsupported
	}
	putter, ok := c.target.(StreamingObjectPutter)
	if !ok {
		return ArtifactCopyStats{}, ErrStreamingUnsupported
	}
	if err := ctx.Err(); err != nil {
		return ArtifactCopyStats{}, err
	}
	if walker, ok := c.source.(artifactObjectWalker); ok {
		return c.copyWalk(ctx, prefix, startAfter, walker, getter, putter, reporter)
	}
	stats := ArtifactCopyStats{LastKey: startAfter}
	batch := ArtifactCopyProgress{}
	cursor := startAfter
	for {
		items, err := c.source.List(ctx, prefix, cursor, c.page)
		if err != nil {
			return stats, err
		}
		if len(items) > c.page {
			return stats, fmt.Errorf("artifact source returned %d items above page bound %d", len(items), c.page)
		}
		if len(items) == 0 {
			break
		}
		if err := validateOrderedPage(items, cursor); err != nil {
			return stats, err
		}
		for _, item := range items {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			if item.Size > c.max {
				return stats, fmt.Errorf("artifact %q exceeds maximum size %d", item.Key, c.max)
			}
			body, _, declared, err := getter.Open(ctx, item.Key)
			if err != nil {
				return stats, err
			}
			reader := &boundedHashReader{reader: contextReader{ctx: ctx, reader: body}, hash: sha256.New(), limit: c.max}
			_, putErr := putter.PutStream(ctx, item.Key, reader, c.max, PutOptions{})
			closeErr := body.Close()
			if putErr != nil {
				return stats, putErr
			}
			if closeErr != nil {
				return stats, closeErr
			}
			if reader.count > c.max || (declared >= 0 && reader.count != declared) || (item.Size >= 0 && reader.count != item.Size) {
				return stats, fmt.Errorf("artifact %q changed while copying", item.Key)
			}
			stats.Objects++
			stats.Bytes += reader.count
			stats.LastKey = item.Key
			batch.Objects++
			batch.Bytes += reader.count
			batch.LastKey = item.Key
			cursor = item.Key
			if batch.Objects >= c.batch {
				if err := recordCopyProgress(ctx, reporter, batch); err != nil {
					return stats, err
				}
				batch = ArtifactCopyProgress{}
			}
		}
		if len(items) < c.page {
			break
		}
	}
	if batch.Objects != 0 {
		if err := recordCopyProgress(ctx, reporter, batch); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func (c *ArtifactCopier) copyWalk(ctx context.Context, prefix, startAfter string, walker artifactObjectWalker, getter StreamingObjectGetter, putter StreamingObjectPutter, reporter ArtifactCopyProgressRecorder) (ArtifactCopyStats, error) {
	stats := ArtifactCopyStats{LastKey: startAfter}
	batch := ArtifactCopyProgress{}
	err := walker.walkObjects(ctx, prefix, startAfter, func(item ObjectItem) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		copied, err := c.copyItem(ctx, item, getter, putter)
		if err != nil {
			return err
		}
		stats.Objects++
		stats.Bytes += copied
		stats.LastKey = item.Key
		batch.Objects++
		batch.Bytes += copied
		batch.LastKey = item.Key
		if batch.Objects >= c.batch {
			if err := recordCopyProgress(ctx, reporter, batch); err != nil {
				return err
			}
			batch = ArtifactCopyProgress{}
		}
		return nil
	})
	if err != nil {
		return stats, err
	}
	if batch.Objects != 0 {
		if err := recordCopyProgress(ctx, reporter, batch); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func (c *ArtifactCopier) copyItem(ctx context.Context, item ObjectItem, getter StreamingObjectGetter, putter StreamingObjectPutter) (int64, error) {
	if item.Size > c.max {
		return 0, fmt.Errorf("artifact %q exceeds maximum size %d", item.Key, c.max)
	}
	body, _, declared, err := getter.Open(ctx, item.Key)
	if err != nil {
		return 0, err
	}
	reader := &boundedHashReader{reader: contextReader{ctx: ctx, reader: body}, hash: sha256.New(), limit: c.max}
	_, putErr := putter.PutStream(ctx, item.Key, reader, c.max, PutOptions{})
	closeErr := body.Close()
	if putErr != nil {
		return 0, putErr
	}
	if closeErr != nil {
		return 0, closeErr
	}
	if reader.count > c.max || (declared >= 0 && reader.count != declared) || (item.Size >= 0 && reader.count != item.Size) {
		return 0, fmt.Errorf("artifact %q changed while copying", item.Key)
	}
	return reader.count, nil
}

func recordCopyProgress(ctx context.Context, reporter ArtifactCopyProgressRecorder, progress ArtifactCopyProgress) error {
	if reporter == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return reporter.RecordArtifactCopyProgress(ctx, progress)
}

// Reconcile compares complete lexical key sets and hashes every corresponding
// object. It is a barrier-external proof: callers should run it during bulk
// migration/catch-up, then use ApplyAndVerifyKeys for the finite journal delta
// inside DynamicObjectStore.WithArtifactActivationBarrier.
func (c *ArtifactCopier) Reconcile(ctx context.Context, prefix string) (ArtifactReconcileStats, error) {
	return c.ReconcileWithProgress(ctx, prefix, nil)
}

// ReconcileWithProgress is Reconcile with a bounded progress callback. The
// callback runs synchronously after each bounded batch and an error stops the
// scan immediately. No object body or unbounded work is retained between
// callbacks.
func (c *ArtifactCopier) ReconcileWithProgress(ctx context.Context, prefix string, reporter ArtifactReconcileProgressRecorder) (ArtifactReconcileStats, error) {
	if c == nil || c.source == nil || c.target == nil {
		return ArtifactReconcileStats{}, errors.New("artifact copier is not initialized")
	}
	sourceGetter, sourceOK := c.source.(StreamingObjectGetter)
	targetGetter, targetOK := c.target.(StreamingObjectGetter)
	if !sourceOK || !targetOK {
		return ArtifactReconcileStats{}, ErrStreamingUnsupported
	}
	if walker, ok := c.source.(artifactObjectWalker); ok {
		return c.reconcileWalkSource(ctx, prefix, walker, sourceGetter, targetGetter, reporter)
	}
	var sourcePage, targetPage []ObjectItem
	var sourceCursor, targetCursor string
	sourceIndex, targetIndex := 0, 0
	sourceDone, targetDone := false, false
	var previous string
	stats := ArtifactReconcileStats{}
	progress := ArtifactReconcileProgress{}
	for {
		if sourceIndex == len(sourcePage) && !sourceDone {
			items, err := c.source.List(ctx, prefix, sourceCursor, c.page)
			if err != nil {
				return stats, err
			}
			if len(items) > c.page {
				return stats, fmt.Errorf("artifact source returned %d items above page bound %d", len(items), c.page)
			}
			if err := validateOrderedPage(items, sourceCursor); err != nil {
				return stats, err
			}
			sourcePage, sourceIndex = items, 0
			if len(items) == 0 || len(items) < c.page {
				sourceDone = true
			}
			if len(items) > 0 {
				sourceCursor = items[len(items)-1].Key
			}
		}
		if targetIndex == len(targetPage) && !targetDone {
			items, err := c.target.List(ctx, prefix, targetCursor, c.page)
			if err != nil {
				return stats, err
			}
			if len(items) > c.page {
				return stats, fmt.Errorf("artifact target returned %d items above page bound %d", len(items), c.page)
			}
			if err := validateOrderedPage(items, targetCursor); err != nil {
				return stats, err
			}
			targetPage, targetIndex = items, 0
			if len(items) == 0 || len(items) < c.page {
				targetDone = true
			}
			if len(items) > 0 {
				targetCursor = items[len(items)-1].Key
			}
		}
		if sourceIndex == len(sourcePage) && targetIndex == len(targetPage) && sourceDone && targetDone {
			if progress.Objects != 0 {
				if err := recordReconcileProgress(ctx, reporter, progress); err != nil {
					return stats, err
				}
			}
			return stats, nil
		}
		if sourceIndex == len(sourcePage) {
			return stats, fmt.Errorf("artifact target has extra key %q", targetPage[targetIndex].Key)
		}
		if targetIndex == len(targetPage) {
			return stats, fmt.Errorf("artifact target is missing key %q", sourcePage[sourceIndex].Key)
		}
		sourceItem, targetItem := sourcePage[sourceIndex], targetPage[targetIndex]
		if sourceItem.Key < targetItem.Key {
			return stats, fmt.Errorf("artifact target is missing key %q", sourceItem.Key)
		}
		if targetItem.Key < sourceItem.Key {
			return stats, fmt.Errorf("artifact target has extra key %q", targetItem.Key)
		}
		if sourceItem.Key <= previous {
			return stats, fmt.Errorf("artifact source key order regressed at %q", sourceItem.Key)
		}
		previous = sourceItem.Key
		sourceSize, sourceDigest, err := digestArtifact(ctx, sourceGetter, sourceItem.Key, c.max)
		if err != nil {
			return stats, err
		}
		targetSize, targetDigest, err := digestArtifact(ctx, targetGetter, targetItem.Key, c.max)
		if err != nil {
			return stats, err
		}
		if sourceSize != targetSize || sourceDigest != targetDigest {
			return stats, fmt.Errorf("artifact %q digest or size mismatch", sourceItem.Key)
		}
		stats.Objects++
		stats.Bytes += sourceSize
		progress.Objects++
		progress.Bytes += sourceSize
		progress.LastKey = sourceItem.Key
		sourceIndex++
		targetIndex++
		if progress.Objects >= c.batch {
			if err := recordReconcileProgress(ctx, reporter, progress); err != nil {
				return stats, err
			}
			progress = ArtifactReconcileProgress{}
		}
	}
}

func (c *ArtifactCopier) reconcileWalkSource(ctx context.Context, prefix string, walker artifactObjectWalker, sourceGetter, targetGetter StreamingObjectGetter, reporter ArtifactReconcileProgressRecorder) (ArtifactReconcileStats, error) {
	var targetPage []ObjectItem
	targetIndex := 0
	var targetCursor string
	targetDone := false
	loadTarget := func() error {
		items, err := c.target.List(ctx, prefix, targetCursor, c.page)
		if err != nil {
			return err
		}
		if len(items) > c.page {
			return fmt.Errorf("artifact target returned %d items above page bound %d", len(items), c.page)
		}
		if err := validateOrderedPage(items, targetCursor); err != nil {
			return err
		}
		targetPage, targetIndex = items, 0
		if len(items) == 0 || len(items) < c.page {
			targetDone = true
		}
		if len(items) > 0 {
			targetCursor = items[len(items)-1].Key
		}
		return nil
	}
	nextTarget := func() (*ObjectItem, error) {
		for targetIndex == len(targetPage) && !targetDone {
			if err := loadTarget(); err != nil {
				return nil, err
			}
		}
		if targetIndex == len(targetPage) {
			return nil, nil
		}
		return &targetPage[targetIndex], nil
	}
	stats := ArtifactReconcileStats{}
	progress := ArtifactReconcileProgress{}
	var previous string
	err := walker.walkObjects(ctx, prefix, "", func(sourceItem ObjectItem) error {
		if sourceItem.Key <= previous {
			return fmt.Errorf("artifact source key order regressed at %q", sourceItem.Key)
		}
		previous = sourceItem.Key
		targetItem, err := nextTarget()
		if err != nil {
			return err
		}
		if targetItem == nil {
			return fmt.Errorf("artifact target is missing key %q", sourceItem.Key)
		}
		if sourceItem.Key < targetItem.Key {
			return fmt.Errorf("artifact target is missing key %q", sourceItem.Key)
		}
		if targetItem.Key < sourceItem.Key {
			return fmt.Errorf("artifact target has extra key %q", targetItem.Key)
		}
		sourceSize, sourceDigest, err := digestArtifact(ctx, sourceGetter, sourceItem.Key, c.max)
		if err != nil {
			return err
		}
		targetSize, targetDigest, err := digestArtifact(ctx, targetGetter, targetItem.Key, c.max)
		if err != nil {
			return err
		}
		if sourceSize != targetSize || sourceDigest != targetDigest {
			return fmt.Errorf("artifact %q digest or size mismatch", sourceItem.Key)
		}
		stats.Objects++
		stats.Bytes += sourceSize
		progress.Objects++
		progress.Bytes += sourceSize
		progress.LastKey = sourceItem.Key
		targetIndex++
		if progress.Objects >= c.batch {
			if err := recordReconcileProgress(ctx, reporter, progress); err != nil {
				return err
			}
			progress = ArtifactReconcileProgress{}
		}
		return nil
	})
	if err != nil {
		return stats, err
	}
	if targetItem, err := nextTarget(); err != nil {
		return stats, err
	} else if targetItem != nil {
		return stats, fmt.Errorf("artifact target has extra key %q", targetItem.Key)
	}
	if progress.Objects != 0 {
		if err := recordReconcileProgress(ctx, reporter, progress); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func recordReconcileProgress(ctx context.Context, reporter ArtifactReconcileProgressRecorder, progress ArtifactReconcileProgress) error {
	if reporter == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return reporter.RecordArtifactReconcileProgress(ctx, progress)
}

// ApplyAndVerifyKeys applies and verifies only a finite final mutation set. It
// performs no List call and is the primitive intended for an activation
// barrier after a complete reconciliation has already run outside the barrier.
func (c *ArtifactCopier) ApplyAndVerifyKeys(ctx context.Context, changes []ArtifactKeyChange) error {
	if c == nil || c.source == nil || c.target == nil {
		return errors.New("artifact copier is not initialized")
	}
	if len(changes) > MaxArtifactActivationKeys {
		return fmt.Errorf("artifact activation key count exceeds %d", MaxArtifactActivationKeys)
	}
	sourceGetter, sourceOK := c.source.(StreamingObjectGetter)
	targetPutter, targetPutOK := c.target.(StreamingObjectPutter)
	targetGetter, targetGetOK := c.target.(StreamingObjectGetter)
	if !sourceOK || !targetPutOK || !targetGetOK {
		return ErrStreamingUnsupported
	}
	ordered := append([]ArtifactKeyChange(nil), changes...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Key < ordered[j].Key })
	for i, change := range ordered {
		if err := ctx.Err(); err != nil {
			return err
		}
		if change.Key == "" || (i > 0 && ordered[i-1].Key == change.Key) {
			return fmt.Errorf("artifact activation keys must be unique and non-empty")
		}
		if change.Deleted {
			if err := ensureArtifactMissing(ctx, sourceGetter, change.Key); err != nil {
				return err
			}
			if err := c.target.Delete(ctx, change.Key); err != nil {
				return err
			}
			if err := ensureArtifactMissing(ctx, targetGetter, change.Key); err != nil {
				return err
			}
			continue
		}
		if err := copyArtifact(ctx, sourceGetter, targetPutter, change.Key, c.max); err != nil {
			return err
		}
		sourceSize, sourceDigest, err := digestArtifact(ctx, sourceGetter, change.Key, c.max)
		if err != nil {
			return err
		}
		targetSize, targetDigest, err := digestArtifact(ctx, targetGetter, change.Key, c.max)
		if err != nil {
			return err
		}
		if sourceSize != targetSize || sourceDigest != targetDigest {
			return fmt.Errorf("artifact %q digest or size mismatch", change.Key)
		}
	}
	return nil
}

func copyArtifact(ctx context.Context, sourceGetter StreamingObjectGetter, targetPutter StreamingObjectPutter, key string, max int64) error {
	body, _, declared, err := sourceGetter.Open(ctx, key)
	if err != nil {
		return err
	}
	reader := &boundedHashReader{reader: contextReader{ctx: ctx, reader: body}, hash: sha256.New(), limit: max}
	_, putErr := targetPutter.PutStream(ctx, key, reader, max, PutOptions{})
	closeErr := body.Close()
	if putErr != nil {
		return putErr
	}
	if closeErr != nil {
		return closeErr
	}
	if reader.count > max || (declared >= 0 && declared != reader.count) {
		return fmt.Errorf("artifact %q changed while copying", key)
	}
	return nil
}

func ensureArtifactMissing(ctx context.Context, getter StreamingObjectGetter, key string) error {
	body, _, _, err := getter.Open(ctx, key)
	if err == nil {
		_ = body.Close()
		return fmt.Errorf("artifact %q exists but is marked deleted", key)
	}
	if errors.Is(err, ErrObjectNotFound) {
		return nil
	}
	return err
}

func validateOrderedPage(items []ObjectItem, cursor string) error {
	previous := cursor
	for _, item := range items {
		if item.Key == "" || item.Key <= previous {
			return fmt.Errorf("artifact list is not strictly ordered after %q", previous)
		}
		previous = item.Key
	}
	return nil
}

func digestArtifact(ctx context.Context, getter StreamingObjectGetter, key string, max int64) (int64, string, error) {
	body, _, declared, err := getter.Open(ctx, key)
	if err != nil {
		return 0, "", err
	}
	reader := &boundedHashReader{reader: contextReader{ctx: ctx, reader: body}, hash: sha256.New(), limit: max}
	_, readErr := io.Copy(io.Discard, reader)
	closeErr := body.Close()
	if readErr != nil {
		return 0, "", readErr
	}
	if closeErr != nil {
		return 0, "", closeErr
	}
	if reader.count > max || (declared >= 0 && declared != reader.count) {
		return 0, "", fmt.Errorf("artifact %q changed while hashing", key)
	}
	return reader.count, hex.EncodeToString(reader.hash.Sum(nil)), nil
}

type boundedHashReader struct {
	reader io.Reader
	hash   hash.Hash
	limit  int64
	count  int64
}

func (r *boundedHashReader) Read(p []byte) (int, error) {
	if r.count > r.limit {
		return 0, objectTooLarge(r.limit)
	}
	remaining := r.limit + 1 - r.count
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	if len(p) == 0 {
		return 0, objectTooLarge(r.limit)
	}
	n, err := r.reader.Read(p)
	if n > 0 {
		r.count += int64(n)
		_, _ = r.hash.Write(p[:n])
	}
	return n, err
}
