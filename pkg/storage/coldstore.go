package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"

	"github.com/klauspost/compress/zstd"
)

// Cold-object sweeping rewrites committed session-event chunks as
// zstd-compressed ".zst" siblings and removes the plain object. It deliberately
// does not compress arbitrary objects: meta, evidence, resources, and orphan
// event chunks can still be replaced or deleted. FileObjectStore and
// S3ObjectStore transparently read the compressed form for this exact layout.
type ColdSweeper struct {
	Store ObjectStore
	// OlderThan: only objects whose ModTime is older are compressed.
	OlderThan time.Duration
	// Prefixes are scan roots. Eligibility remains restricted to committed
	// session event chunks regardless of the configured prefixes.
	Prefixes []string
	// Limit caps objects per sweep; default 500.
	Limit int
}

var zstdEncoder, _ = zstd.NewWriter(nil)

// Sweep compresses cold, committed session-event chunks once, returning how
// many were swept. Objects with a zero ModTime (unknown age) are skipped. The
// list is paged even when an earlier page has no eligible objects. Sweeping is
// idempotent: ".zst" siblings are skipped, and a sweep interrupted between
// PUT and DELETE leaves both objects — reads prefer the plain object, and the
// next sweep retries the delete.
func (s *ColdSweeper) Sweep(ctx context.Context) (int, error) {
	limit := s.Limit
	if limit <= 0 {
		limit = 500
	}
	swept := 0
	for _, prefix := range s.Prefixes {
		startAfter := ""
		for {
			if swept >= limit {
				return swept, nil
			}
			items, err := s.Store.List(ctx, prefix, startAfter, limit)
			if err != nil {
				return swept, err
			}
			if len(items) == 0 {
				break
			}
			for _, item := range items {
				if swept >= limit {
					return swept, nil
				}
				if item.ModTime.IsZero() || time.Since(item.ModTime) <= s.OlderThan ||
					!s.committedSessionEvent(ctx, item.Key) {
					continue
				}
				if err := s.compressObject(ctx, item.Key); err != nil {
					if errors.Is(err, errColdObjectVanished) {
						continue
					}
					return swept, err
				}
				if err := deleteColdPlain(ctx, s.Store, item.Key); err != nil {
					return swept, err
				}
				swept++
			}
			next := items[len(items)-1].Key
			if next <= startAfter {
				return swept, nil
			}
			startAfter = next
		}
	}
	return swept, nil
}

var errColdObjectVanished = errors.New("cold object vanished during sweep")

// compressObject prefers the optional streaming seams so a cold object is not
// simultaneously retained as plain bytes and as a compressed byte slice. The
// byte fallback remains for third-party ObjectStore implementations that do
// not expose Open/PutStream.
func (s *ColdSweeper) compressObject(ctx context.Context, key string) error {
	if opener, canOpen := s.Store.(StreamingObjectGetter); canOpen {
		if putter, canPut := s.Store.(StreamingObjectPutter); canPut {
			body, _, size, err := opener.Open(ctx, key)
			if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				return err
			}
			if err == nil && size > MaxObjectBytes {
				_ = body.Close()
				return objectTooLarge(MaxObjectBytes)
			}
			if err == nil {
				return streamColdObject(ctx, body, putter, key+".zst")
			}
		}
	}
	data, _, err := s.Store.Get(ctx, key)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return errColdObjectVanished
	}
	compressed := zstdEncoder.EncodeAll(data, nil)
	_, err = s.Store.Put(ctx, key+".zst", compressed, PutOptions{})
	return err
}

func streamColdObject(ctx context.Context, source io.ReadCloser, target StreamingObjectPutter, key string) error {
	pipeReader, pipeWriter := io.Pipe()
	producerErr := make(chan error, 1)
	go func() {
		defer source.Close()
		encoder, err := zstd.NewWriter(pipeWriter)
		if err != nil {
			_ = pipeWriter.CloseWithError(err)
			producerErr <- err
			return
		}
		_, copyErr := io.Copy(encoder, coldContextReader{ctx: ctx, reader: source})
		closeErr := encoder.Close()
		if copyErr != nil {
			_ = pipeWriter.CloseWithError(copyErr)
			producerErr <- copyErr
			return
		}
		if closeErr != nil {
			_ = pipeWriter.CloseWithError(closeErr)
			producerErr <- closeErr
			return
		}
		producerErr <- pipeWriter.Close()
	}()
	_, putErr := target.PutStream(ctx, key, pipeReader, MaxStreamingObjectBytes, PutOptions{})
	if putErr != nil {
		_ = pipeReader.CloseWithError(putErr)
	}
	encodeErr := <-producerErr
	if putErr != nil {
		return putErr
	}
	return encodeErr
}

type coldContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader coldContextReader) Read(p []byte) (int, error) {
	select {
	case <-reader.ctx.Done():
		return 0, reader.ctx.Err()
	default:
		return reader.reader.Read(p)
	}
}

// coldPlainDeleter is an internal maintenance seam. Normal ObjectStore.Delete
// removes the complete logical object, including a transparent cold sibling;
// a sweep alone must remove only the primary after its sibling is durable.
type coldPlainDeleter interface {
	deleteColdPlain(context.Context, string) error
}

func deleteColdPlain(ctx context.Context, store ObjectStore, key string) error {
	if err := ValidateObjectKey(key); err != nil {
		return err
	}
	if deleter, ok := store.(coldPlainDeleter); ok {
		return deleter.deleteColdPlain(ctx, key)
	}
	// Third-party stores normally have no transparent sibling cleanup in
	// Delete, so the ObjectStore operation already removes only this key.
	return store.Delete(ctx, key)
}

// committedSessionEvent accepts only chunks whose keys have the immutable
// session layout and which are already below the session's committed version.
// A failed Save leaves a chunk at exactly meta.version, where a later Save may
// overwrite it; such an orphan must remain plain.
func (s *ColdSweeper) committedSessionEvent(ctx context.Context, key string) bool {
	id, seq, ok := sessionEventObjectKey(key)
	if !ok {
		return false
	}
	metaPayload, _, err := s.Store.Get(ctx, s3MetaKey(id))
	if err != nil {
		return false
	}
	var meta s3SessionMeta
	if err := json.Unmarshal(metaPayload, &meta); err != nil {
		return false
	}
	return !meta.Initializing && meta.Version > seq && meta.Header.ID == id && core.ValidateSessionID(meta.Header.ID) == nil
}

// sessionEventObjectKey recognizes the only object layout eligible for cold
// compression and transparent decompression.
func sessionEventObjectKey(key string) (sessionID string, seq int64, ok bool) {
	parts := strings.Split(key, "/")
	if len(parts) != 4 || parts[0] != "sessions" || parts[2] != "events" ||
		core.ValidateSessionID(parts[1]) != nil {
		return "", 0, false
	}
	const suffix = ".jsonl"
	name := parts[3]
	if !strings.HasSuffix(name, suffix) {
		return "", 0, false
	}
	digits := strings.TrimSuffix(name, suffix)
	if len(digits) != 12 {
		return "", 0, false
	}
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return "", 0, false
		}
	}
	seq, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return "", 0, false
	}
	return parts[1], seq, true
}

// DecompressZstd expands a zstd payload (used by stores implementing the
// ".zst" read fallback).
func DecompressZstd(payload []byte) ([]byte, error) {
	reader, err := zstd.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func decompressZstdReaderBounded(source io.Reader, maxBytes int64) ([]byte, error) {
	reader, err := zstd.NewReader(source)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	decoded, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(decoded)) > maxBytes {
		return nil, objectTooLarge(maxBytes)
	}
	return decoded, nil
}
