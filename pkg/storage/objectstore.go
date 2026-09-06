package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

// Object-store level errors. Session stores translate them onto
// ErrSessionNotFound / ErrSessionConflict at the session boundary.
var (
	ErrObjectNotFound       = errors.New("object not found")
	ErrPreconditionFailed   = errors.New("object precondition failed")
	ErrConditionUnsupported = errors.New("object store does not support conditional writes")
	ErrObjectTooLarge       = errors.New("object exceeds maximum size")
	ErrStreamingUnsupported = errors.New("object store streaming is not supported")
)

// PutOptions carries the conditional-write preconditions for one PUT.
// At most one field may be set. Vendors without conditional-write support
// reject both with ErrConditionUnsupported.
type PutOptions struct {
	// IfMatch fails the PUT with ErrPreconditionFailed unless the stored
	// object's etag matches. Empty means no condition.
	IfMatch string
	// IfNoneMatchStar fails the PUT when the object already exists
	// (S3 If-None-Match:*). Used for atomic create.
	IfNoneMatchStar bool
}

// ObjectItem is one listed object.
type ObjectItem struct {
	Key     string
	ETag    string
	Size    int64
	ModTime time.Time
}

// ObjectStore is the minimal key-value surface the S3 session store needs:
// get, conditional put, delete, and prefix listing. Implementations exist for
// in-memory tests, local disks, and S3-compatible services (AWS S3, MinIO,
// R2, OSS/COS S3-compatible endpoints).
type ObjectStore interface {
	// Get returns the object body and its opaque etag.
	Get(ctx context.Context, key string) (data []byte, etag string, err error)
	// Put stores the body and returns the new etag.
	Put(ctx context.Context, key string, data []byte, opts PutOptions) (etag string, err error)
	// Delete removes the object; deleting a missing object is a no-op.
	Delete(ctx context.Context, key string) error
	// List returns keys under prefix, strictly after startAfter, in
	// lexicographic order, up to limit items.
	List(ctx context.Context, prefix, startAfter string, limit int) ([]ObjectItem, error)
}

// StreamingObjectGetter is an optional bounded-memory read seam. ObjectStore
// remains the compatibility surface for implementations that only support
// byte-slice reads. The returned body must be closed by the caller.
type StreamingObjectGetter interface {
	Open(ctx context.Context, key string) (body io.ReadCloser, etag string, size int64, err error)
}

// StreamingObjectPutter is an optional bounded-memory write seam. maxBytes is
// the caller's limit; implementations also enforce MaxStreamingObjectBytes as
// a hard upper bound. A negative or zero maxBytes selects that hard upper
// bound. Byte-slice Put remains bounded by MaxObjectBytes.
// ErrStreamingUnsupported must be returned before reading body so callers can
// safely fall back to ObjectStore.Put with the same request body.
type StreamingObjectPutter interface {
	PutStream(ctx context.Context, key string, body io.Reader, maxBytes int64, opts PutOptions) (etag string, err error)
}

// StreamingObjectStore documents an implementation supporting both optional
// stream directions. Servers type-assert the narrower getter/putter seams so
// a partial implementation (for example S3 GET only) remains useful.
type StreamingObjectStore interface {
	ObjectStore
	StreamingObjectGetter
	StreamingObjectPutter
}

const (
	MaxMemoryObjects              = 4096
	MaxObjectBytes                = 16 << 20
	MaxStreamingObjectBytes int64 = 5 << 30
)

func streamingObjectLimit(requested int64) int64 {
	if requested <= 0 || requested > MaxStreamingObjectBytes {
		return MaxStreamingObjectBytes
	}
	return requested
}

func objectTooLarge(limit int64) error {
	return fmt.Errorf("%w: %d bytes", ErrObjectTooLarge, limit)
}

// MemoryObjectStore is a concurrency-safe reference implementation used by
// tests and as the in-process cache of truth for examples.
type MemoryObjectStore struct {
	mu         sync.RWMutex
	objects    map[string]memoryObject
	maxObjects int
}

type memoryObject struct {
	data []byte
	etag string
}

func NewMemoryObjectStore() *MemoryObjectStore {
	return &MemoryObjectStore{objects: map[string]memoryObject{}}
}

func contentETag(data []byte) string {
	sum := sha256.Sum256(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func (m *MemoryObjectStore) Get(_ context.Context, key string) ([]byte, string, error) {
	if err := ValidateObjectKey(key); err != nil {
		return nil, "", err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	object, ok := m.objects[key]
	if !ok {
		return nil, "", fmt.Errorf("%w: %s", ErrObjectNotFound, key)
	}
	return append([]byte(nil), object.data...), object.etag, nil
}

func (m *MemoryObjectStore) Put(_ context.Context, key string, data []byte, opts PutOptions) (string, error) {
	if err := ValidateObjectKey(key); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, exists := m.objects[key]
	if opts.IfNoneMatchStar {
		if exists {
			return "", fmt.Errorf("%w: %s already exists", ErrPreconditionFailed, key)
		}
	}
	if opts.IfMatch != "" {
		if !exists {
			return "", fmt.Errorf("%w: %s vanished", ErrPreconditionFailed, key)
		}
		if existing.etag != opts.IfMatch {
			return "", fmt.Errorf("%w: %s etag mismatch", ErrPreconditionFailed, key)
		}
	}
	if len(data) > MaxObjectBytes {
		return "", objectTooLarge(MaxObjectBytes)
	}
	if !exists && len(m.objects) >= m.objectCap() {
		return "", fmt.Errorf("memory objects exceed maximum of %d", m.objectCap())
	}
	etag := contentETag(data)
	m.objects[key] = memoryObject{data: append([]byte(nil), data...), etag: etag}
	return etag, nil
}

func (m *MemoryObjectStore) objectCap() int {
	if m != nil && m.maxObjects > 0 {
		return m.maxObjects
	}
	return MaxMemoryObjects
}

func (m *MemoryObjectStore) Delete(_ context.Context, key string) error {
	if err := ValidateObjectKey(key); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *MemoryObjectStore) List(_ context.Context, prefix, startAfter string, limit int) ([]ObjectItem, error) {
	if prefix != "" {
		if err := ValidateObjectKey(strings.TrimSuffix(prefix, "/")); err != nil {
			return nil, err
		}
	}
	m.mu.RLock()
	keys := make([]string, 0, len(m.objects))
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) && key > startAfter {
			keys = append(keys, key)
		}
	}
	m.mu.RUnlock()
	sort.Strings(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	items := make([]ObjectItem, 0, len(keys))
	m.mu.RLock()
	for _, key := range keys {
		object := m.objects[key]
		items = append(items, ObjectItem{Key: key, ETag: object.etag, Size: int64(len(object.data))})
	}
	m.mu.RUnlock()
	return items, nil
}

// FileObjectStore lays the object keys out on a local directory. It backs the
// S3-shaped session store on a plain disk (including network mounts) and
// gives every ObjectStore consumer an offline-testable implementation.
//
// ETags are content hashes, so conditional writes are exact. Cross-process
// access to one directory is not guarded; one writer per deployment, or a
// real object store, is required for correctness.
type FileObjectStore struct {
	root string
	// mu serializes conditional writes across keys; contention is irrelevant
	// at session-store write rates.
	mu         sync.Mutex
	maxObjects int
}

func NewFileObjectStore(root string) (*FileObjectStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, fmt.Errorf("object store root is empty")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve object store root: %w", err)
	}
	if err := privateMkdirAll(abs); err != nil {
		return nil, fmt.Errorf("create object store root: %w", err)
	}
	return &FileObjectStore{root: abs}, nil
}

func (f *FileObjectStore) objectCap() int {
	if f != nil && f.maxObjects > 0 {
		return f.maxObjects
	}
	// Filesystem storage is the large-artifact backend. Unlike the in-memory
	// implementation it has no process-resident object map, so the default is
	// uncapped; tests or embedding code may still set maxObjects explicitly.
	return 0
}

func isObjectStreamTemp(name string) bool {
	return strings.HasPrefix(name, ".object-stream-")
}

func (f *FileObjectStore) objectCountLocked() (int, error) {
	count := 0
	err := filepath.WalkDir(f.root, func(_ string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && !isObjectStreamTemp(entry.Name()) {
			count++
		}
		return nil
	})
	return count, err
}

// ValidateObjectKey rejects empty, absolute, parent-directory, and control
// character object keys before they are used as filesystem or S3 paths.
func ValidateObjectKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("invalid object key %q", key)
	}
	if strings.Contains(key, "..") || strings.HasPrefix(key, "/") || strings.HasPrefix(key, "\\") {
		return fmt.Errorf("invalid object key %q", key)
	}
	for _, char := range key {
		if unicode.IsControl(char) {
			return fmt.Errorf("invalid object key %q", key)
		}
	}
	return nil
}

func (f *FileObjectStore) path(key string) (string, error) {
	if err := ValidateObjectKey(key); err != nil {
		return "", err
	}
	clean := filepath.FromSlash(key)
	if filepath.IsAbs(clean) {
		return "", fmt.Errorf("invalid object key %q", key)
	}
	full := filepath.Join(f.root, clean)
	rel, err := filepath.Rel(f.root, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("invalid object key %q", key)
	}
	return full, nil
}

func (f *FileObjectStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	path, err := f.path(key)
	if err != nil {
		return nil, "", err
	}
	info, statErr := os.Stat(path)
	if statErr == nil {
		if !info.Mode().IsRegular() {
			return nil, "", fmt.Errorf("object is not a regular file")
		}
		if info.Size() > MaxObjectBytes {
			return nil, "", objectTooLarge(MaxObjectBytes)
		}
		file, openErr := os.Open(path)
		if openErr != nil {
			return nil, "", openErr
		}
		data, readErr := io.ReadAll(io.LimitReader(contextReader{ctx: ctx, reader: file}, MaxObjectBytes+1))
		closeErr := file.Close()
		if readErr != nil {
			return nil, "", readErr
		}
		if closeErr != nil {
			return nil, "", closeErr
		}
		if len(data) > MaxObjectBytes {
			return nil, "", objectTooLarge(MaxObjectBytes)
		}
		// The initial Stat prevents a large allocation; this second Stat closes
		// the common replace-after-open TOCTOU window before returning bytes.
		if latest, latestErr := os.Stat(path); latestErr == nil && latest.Size() > MaxObjectBytes {
			return nil, "", objectTooLarge(MaxObjectBytes)
		}
		return data, contentETag(data), nil
	}
	if !os.IsNotExist(statErr) {
		return nil, "", statErr
	}
	// Only immutable-layout session-event chunks may have a transparent cold
	// representation. Sweep additionally proves a chunk is committed before
	// creating that sibling. Other objects remain ordinary mutable keys, so a
	// stale sibling can never revive a deleted object.
	if _, _, cold := sessionEventObjectKey(key); cold {
		zstPath := path + ".zst"
		if zstInfo, zstErr := os.Stat(zstPath); zstErr == nil && zstInfo.Mode().IsRegular() {
			zstFile, openErr := os.Open(zstPath)
			if openErr != nil {
				return nil, "", openErr
			}
			plain, decodeErr := decompressZstdReaderBounded(contextReader{ctx: ctx, reader: zstFile}, MaxObjectBytes)
			closeErr := zstFile.Close()
			if decodeErr == nil && closeErr == nil {
				return plain, contentETag(plain), nil
			} else if errors.Is(decodeErr, ErrObjectTooLarge) {
				return nil, "", decodeErr
			} else if decodeErr != nil {
				return nil, "", decodeErr
			} else if closeErr != nil {
				return nil, "", closeErr
			}
		}
	}
	return nil, "", fmt.Errorf("%w: %s", ErrObjectNotFound, key)
}

// Open streams an ordinary file without materializing its body. Cold session
// event siblings retain the legacy decompression fallback because that layout
// is not a resource path and has no streaming decompressor seam yet.
func (f *FileObjectStore) Open(ctx context.Context, key string) (io.ReadCloser, string, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", 0, err
	}
	path, err := f.path(key)
	if err != nil {
		return nil, "", 0, err
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			if _, _, cold := sessionEventObjectKey(key); cold {
				data, etag, getErr := f.Get(ctx, key)
				if getErr == nil {
					return io.NopCloser(bytes.NewReader(data)), etag, int64(len(data)), nil
				}
			}
			return nil, "", 0, fmt.Errorf("%w: %s", ErrObjectNotFound, key)
		}
		return nil, "", 0, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, "", 0, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, "", 0, fmt.Errorf("object is not a regular file")
	}
	etag, err := fileETag(file)
	if err != nil {
		_ = file.Close()
		return nil, "", 0, err
	}
	if err := ctx.Err(); err != nil {
		_ = file.Close()
		return nil, "", 0, err
	}
	return file, etag, info.Size(), nil
}

func fileETag(file *os.File) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	return `"` + hex.EncodeToString(hash.Sum(nil)) + `"`, nil
}

func (f *FileObjectStore) Put(_ context.Context, key string, data []byte, opts PutOptions) (string, error) {
	path, err := f.path(key)
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	_, statErr := os.Stat(path)
	exists := statErr == nil
	switch {
	case exists:
		if opts.IfNoneMatchStar {
			return "", fmt.Errorf("%w: %s already exists", ErrPreconditionFailed, key)
		}
		if opts.IfMatch != "" {
			file, openErr := os.Open(path)
			if openErr != nil {
				return "", openErr
			}
			current, etagErr := fileETag(file)
			closeErr := file.Close()
			if etagErr != nil {
				return "", etagErr
			}
			if closeErr != nil {
				return "", closeErr
			}
			if current != opts.IfMatch {
				return "", fmt.Errorf("%w: %s etag mismatch", ErrPreconditionFailed, key)
			}
		}
	case os.IsNotExist(statErr):
		// A cold-only event is still a logically existing immutable object.
		// Conditional mutation cannot atomically cover both representations,
		// so fail closed without materializing or deleting the cold sibling.
		if (opts.IfMatch != "" || opts.IfNoneMatchStar) && isSessionEventObjectKey(key) {
			if _, siblingErr := os.Stat(path + ".zst"); siblingErr == nil {
				return "", fmt.Errorf("%w: immutable session event %s is cold", ErrPreconditionFailed, key)
			} else if !os.IsNotExist(siblingErr) {
				return "", siblingErr
			}
		}
		if opts.IfMatch != "" {
			return "", fmt.Errorf("%w: %s vanished", ErrPreconditionFailed, key)
		}
	default:
		return "", statErr
	}
	if len(data) > MaxObjectBytes {
		return "", objectTooLarge(MaxObjectBytes)
	}
	if !exists && f.objectCap() > 0 {
		count, countErr := f.objectCountLocked()
		if countErr != nil {
			return "", countErr
		}
		if count >= f.objectCap() {
			return "", fmt.Errorf("file objects exceed maximum of %d", f.objectCap())
		}
	}
	if err := privateMkdirAll(filepath.Dir(path)); err != nil {
		return "", err
	}
	if err := privateWriteFile(path, data); err != nil {
		return "", err
	}
	// A primary PUT is the commit point. Cleaning up a historical cold sibling
	// must never turn a successful unconditional or conditional write into an
	// error; a later Delete still removes the sibling before its mutable key.
	if sibling, ok := zstdSibling(key); ok {
		if siblingPath, err := f.path(sibling); err == nil {
			_ = os.Remove(siblingPath)
		}
	}
	return contentETag(data), nil
}

// PutStream writes through a private temporary file, hashing and size-checking
// as bytes arrive. The rename preserves the same replacement/CAS boundary as
// Put while keeping input and output memory bounded.
func (f *FileObjectStore) PutStream(ctx context.Context, key string, body io.Reader, maxBytes int64, opts PutOptions) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	path, err := f.path(key)
	if err != nil {
		return "", err
	}
	limit := streamingObjectLimit(maxBytes)
	if err := privateMkdirAll(filepath.Dir(path)); err != nil {
		return "", err
	}

	// Stream into a private temporary file without holding the store lock. The
	// lock is reserved for the short condition-check and commit section below.
	tmp, err := os.CreateTemp(filepath.Dir(path), ".object-stream-*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if err := privateChmod(tmpName, 0o600); err != nil {
		_ = tmp.Close()
		return "", err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(tmp, hash), io.LimitReader(contextReader{ctx: ctx, reader: body}, limit+1))
	if copyErr != nil {
		_ = tmp.Close()
		return "", copyErr
	}
	if err := ctx.Err(); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if written > limit {
		_ = tmp.Close()
		return "", objectTooLarge(limit)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := privateChmod(tmpName, 0o600); err != nil {
		return "", err
	}

	// Re-check all conditions at commit time so concurrent writers cannot use
	// stale pre-upload state for CAS, create-only, or object-cap decisions.
	f.mu.Lock()
	defer f.mu.Unlock()
	existing, statErr := os.Stat(path)
	exists := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return "", statErr
	}
	if opts.IfNoneMatchStar {
		if exists {
			return "", fmt.Errorf("%w: %s already exists", ErrPreconditionFailed, key)
		}
		if isSessionEventObjectKey(key) {
			if _, siblingErr := os.Stat(path + ".zst"); siblingErr == nil {
				return "", fmt.Errorf("%w: immutable session event %s is cold", ErrPreconditionFailed, key)
			} else if !os.IsNotExist(siblingErr) {
				return "", siblingErr
			}
		}
	}
	if opts.IfMatch != "" {
		if !exists {
			if isSessionEventObjectKey(key) {
				if _, siblingErr := os.Stat(path + ".zst"); siblingErr == nil {
					return "", fmt.Errorf("%w: immutable session event %s is cold", ErrPreconditionFailed, key)
				} else if !os.IsNotExist(siblingErr) {
					return "", siblingErr
				}
			}
			return "", fmt.Errorf("%w: %s vanished", ErrPreconditionFailed, key)
		}
		if !existing.Mode().IsRegular() {
			return "", fmt.Errorf("%w: %s is not a regular file", ErrPreconditionFailed, key)
		}
		current, etagErr := func() (string, error) {
			file, openErr := os.Open(path)
			if openErr != nil {
				return "", openErr
			}
			defer file.Close()
			return fileETag(file)
		}()
		if etagErr != nil {
			return "", etagErr
		}
		if current != opts.IfMatch {
			return "", fmt.Errorf("%w: %s etag mismatch", ErrPreconditionFailed, key)
		}
	}
	if !exists && f.objectCap() > 0 {
		count, countErr := f.objectCountLocked()
		if countErr != nil {
			return "", countErr
		}
		if count >= f.objectCap() {
			return "", fmt.Errorf("file objects exceed maximum of %d", f.objectCap())
		}
	}
	if err := replaceObjectFile(tmpName, path); err != nil {
		return "", err
	}
	cleanup = false
	if sibling, ok := zstdSibling(key); ok {
		if siblingPath, pathErr := f.path(sibling); pathErr == nil {
			_ = os.Remove(siblingPath)
		}
	}
	return `"` + hex.EncodeToString(hash.Sum(nil)) + `"`, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func (f *FileObjectStore) Delete(_ context.Context, key string) error {
	path, err := f.path(key)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if sibling, ok := zstdSibling(key); ok {
		siblingPath, err := f.path(sibling)
		if err != nil {
			return err
		}
		if err := os.Remove(siblingPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (f *FileObjectStore) deleteColdPlain(_ context.Context, key string) error {
	path, err := f.path(key)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// zstdSiblingAfterPut is deliberately disabled for a sidecar's own PUT, so a
// cold sweep can write key+".zst" before removing its plain event object.
func zstdSibling(key string) (string, bool) {
	if strings.HasSuffix(key, ".zst") {
		return "", false
	}
	return key + ".zst", true
}

// rejectConditionalSessionEventPut fails closed for the immutable event
// layout. A cold event can exist solely as key+".zst", while ObjectStore's
// single-key conditional primitives cannot make an atomic assertion across
// the plain and compressed representations. S3SessionStore writes event
// chunks unconditionally, so this does not affect its durable commit path.
func isSessionEventObjectKey(key string) bool {
	_, _, ok := sessionEventObjectKey(key)
	return ok
}

func (f *FileObjectStore) List(ctx context.Context, prefix, startAfter string, limit int) ([]ObjectItem, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if prefix != "" {
		if err := ValidateObjectKey(strings.TrimSuffix(prefix, "/")); err != nil {
			return nil, err
		}
	}
	if limit < 0 {
		limit = 0
	}
	capacity := 0
	if limit > 0 {
		capacity = limit
	}
	items := make([]ObjectItem, 0, capacity)
	err := f.walkObjects(ctx, prefix, startAfter, func(item ObjectItem) error {
		items = append(items, item)
		if limit > 0 && len(items) >= limit {
			return errArtifactWalkStop
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

// walkObjects is the internal single-pass filesystem iterator used by the
// migration copier. WalkDir guarantees lexical directory-entry traversal;
// pruning uses slash-terminated prefixes so cursor punctuation follows the
// same string ordering as ObjectStore.List.
func (f *FileObjectStore) walkObjects(ctx context.Context, prefix, startAfter string, fn func(ObjectItem) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if fn == nil {
		return errors.New("object walk callback is required")
	}
	if prefix != "" {
		if err := ValidateObjectKey(strings.TrimSuffix(prefix, "/")); err != nil {
			return err
		}
	}
	rootPath, err := f.path(".")
	if err != nil {
		return err
	}
	err = filepath.WalkDir(rootPath, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return filepath.SkipAll
			}
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, relErr := filepath.Rel(f.root, path)
		if relErr != nil {
			return nil
		}
		relKey := filepath.ToSlash(rel)
		if relKey == "." {
			relKey = ""
		}
		if entry.IsDir() {
			if relKey != "" {
				if prefix != "" && !strings.HasPrefix(relKey, prefix) && !strings.HasPrefix(prefix, relKey+"/") {
					return filepath.SkipDir
				}
				// A directory's first possible object key is its slash-terminated
				// prefix. Compare that prefix, not the bare directory name: for
				// example, "a/" sorts after the cursor "a!".
				dirPrefix := relKey + "/"
				if startAfter != "" && dirPrefix <= startAfter && !strings.HasPrefix(startAfter, dirPrefix) {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if isObjectStreamTemp(entry.Name()) {
			return nil
		}
		if strings.HasPrefix(relKey, prefix) && relKey > startAfter {
			info, err := entry.Info()
			if err != nil {
				return nil
			}
			if err := fn(ObjectItem{Key: relKey, Size: info.Size(), ModTime: info.ModTime()}); err != nil {
				if errors.Is(err, errArtifactWalkStop) {
					return filepath.SkipAll
				}
				return err
			}
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
