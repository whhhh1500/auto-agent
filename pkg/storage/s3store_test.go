package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/cc-auto-agent/harness-core/pkg/core"
)

// fakeS3 implements the exact S3 REST surface the client uses, including
// conditional PUT preconditions, so the SigV4 client is exercised end to end.
type fakeS3 struct {
	mu             sync.Mutex
	objects        map[string]fakeObject
	deleteFailures map[string]int
	ifMatch        []string // recorded x-amz-if-match header values
	ifNone         []string // recorded x-amz-if-none-match header values
	authSeen       []string // recorded Authorization headers
}

type fakeObject struct {
	body []byte
	etag string
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objects: map[string]fakeObject{}, deleteFailures: map[string]int{}}
}

func (f *fakeS3) etagFor(body []byte) string {
	return contentETag(body)
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.authSeen = append(f.authSeen, r.Header.Get("Authorization"))
	key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Get("list-type") == "2" {
			prefix := r.URL.Query().Get("prefix")
			startAfter := r.URL.Query().Get("start-after")
			body := &strings.Builder{}
			body.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>`)
			truncated := false
			for _, name := range sortedFakeKeys(f.objects) {
				if !strings.HasPrefix(name, prefix) || name <= startAfter {
					continue
				}
				object := f.objects[name]
				body.WriteString("<Contents><Key>" + name + "</Key><ETag>&quot;" +
					strings.Trim(object.etag, `"`) + "&quot;</ETag></Contents>")
			}
			body.WriteString("<IsTruncated>" + fmt.Sprintf("%t", truncated) + "</IsTruncated></ListBucketResult>")
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, body.String())
			return
		}
		object, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", object.etag)
		_, _ = w.Write(object.body)
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		if value := r.Header.Get("x-amz-if-match"); value != "" {
			f.ifMatch = append(f.ifMatch, value)
			existing, ok := f.objects[key]
			if !ok || existing.etag != value {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
		}
		if value := r.Header.Get("x-amz-if-none-match"); value == "*" {
			f.ifNone = append(f.ifNone, value)
			if _, ok := f.objects[key]; ok {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
		}
		etag := f.etagFor(body)
		f.objects[key] = fakeObject{body: append([]byte(nil), body...), etag: etag}
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if f.deleteFailures[key] > 0 {
			f.deleteFailures[key]--
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func sortedFakeKeys(objects map[string]fakeObject) []string {
	keys := make([]string, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

func newFakeS3Store(t *testing.T) (*S3SessionStore, *fakeS3) {
	t.Helper()
	server := newFakeS3()
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	client, err := NewS3ObjectStore(S3Config{
		Endpoint: httpServer.URL, Region: "us-east-1", Bucket: "test-bucket",
		AccessKey: "test-key", SecretKey: "test-secret", PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewS3SessionStore(client)
	if err != nil {
		t.Fatal(err)
	}
	return store, server
}

func TestS3SessionStoreRoundTripAndConflict(t *testing.T) {
	store, _ := newFakeS3Store(t)
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	ctx := context.Background()

	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, session); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("duplicate create must conflict, got %v", err)
	}

	session.Append("run-a", EvUserMessage, UserMessageData{Text: "hello"})
	if err := store.Save(ctx, session, 0); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version() != session.Version() {
		t.Fatalf("round trip lost events: %d vs %d", loaded.Version(), session.Version())
	}
	original := session.Events()
	restored := loaded.Events()
	if string(original[0].Data) != string(restored[0].Data) {
		t.Fatalf("event payload diverged: %s vs %s", original[0].Data, restored[0].Data)
	}

	// Optimistic concurrency against the meta CAS point.
	session.Append("run-a", EvRunEnd, RunEndData{Status: RunCompleted})
	if err := store.Save(ctx, session, 0); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("stale save must conflict, got %v", err)
	}
	if err := store.Save(ctx, session, 1); err != nil {
		t.Fatalf("fresh save failed: %v", err)
	}
}

func TestS3SessionStoreRejectsBadIDs(t *testing.T) {
	store, _ := newFakeS3Store(t)
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	ctx := context.Background()

	// Path traversal in a session id must never reach the object store.
	for _, bad := range []string{"../escape", "a/b", "", `..\back`, "bad id", "a..b"} {
		_, err := store.Load(ctx, bad)
		if err == nil || !strings.Contains(err.Error(), "invalid session id") {
			t.Fatalf("bad id %q must be rejected, got %v", bad, err)
		}
	}
	valid := mustSession(t, user, principal)
	if err := store.Create(ctx, valid); err != nil {
		t.Fatal(err)
	}
}

func TestS3SessionStoreRepairsInterruptedTailOnLoad(t *testing.T) {
	store, _ := newFakeS3Store(t)
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	ctx := context.Background()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-crash", EvRunStart, RunStartData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-crash", EvUserMessage, UserMessageData{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Append("run-crash", EvToolCall, ToolCallData{CallID: "c1", Name: "x.tool"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, session, 0); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	types := []SessionEventType{}
	for _, event := range loaded.Events() {
		types = append(types, event.Type)
	}
	if types[len(types)-1] != EvRunEnd || !containsEventType(types, EvRunError) {
		t.Fatalf("S3 load did not repair the interrupted tail: %v", types)
	}
	// The repair is persisted: a second load sees a balanced log with no new
	// synthetic events beyond the first repair.
	second, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if second.Version() != loaded.Version() {
		t.Fatalf("repair is not idempotent: %d vs %d", second.Version(), loaded.Version())
	}
}

func TestS3SessionStoreTrustsMetaVersionOverOrphanChunks(t *testing.T) {
	store, server := newFakeS3Store(t)
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	ctx := context.Background()
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	session.Append("run-a", EvUserMessage, UserMessageData{Text: "hello"})
	if err := store.Save(ctx, session, 0); err != nil {
		t.Fatal(err)
	}
	// Simulate a failed commit: an events chunk exists beyond meta.version.
	orphan, err := json.Marshal(SessionEvent{Seq: 1, RunID: "run-a", Type: EvRunEnd, Data: mustJSON(t, RunEndData{Status: RunCompleted})})
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("sessions/%s/events/%012d.jsonl", session.ID(), 1)
	if _, exists := server.objects[key]; exists {
		t.Fatal("orphan key should not exist yet")
	}
	server.mu.Lock()
	server.objects[key] = fakeObject{body: orphan, etag: contentETag(orphan)}
	server.mu.Unlock()

	loaded, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version() != 1 {
		t.Fatalf("orphan chunk leaked into the committed view: version=%d", loaded.Version())
	}
}

func TestS3ClientSignsAndConditionallyPuts(t *testing.T) {
	server := newFakeS3()
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	client, err := NewS3ObjectStore(S3Config{
		Endpoint: httpServer.URL, Region: "eu-central-1", Bucket: "test-bucket",
		AccessKey: "AKID", SecretKey: "secret", PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := client.Put(ctx, "a/b key.txt", []byte("one"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if len(server.authSeen) == 0 || !strings.HasPrefix(server.authSeen[0], "AWS4-HMAC-SHA256") {
		t.Fatalf("request missing SigV4 authorization: %v", server.authSeen)
	}
	if len(server.authSeen[0]) < 100 {
		t.Fatal("authorization header unexpectedly short")
	}

	data, etag, err := client.Get(ctx, "a/b key.txt")
	if err != nil || string(data) != "one" {
		t.Fatalf("get failed: %q %v", data, err)
	}
	// Matching If-Match succeeds.
	if _, err := client.Put(ctx, "a/b key.txt", []byte("two"), PutOptions{IfMatch: etag}); err != nil {
		t.Fatal(err)
	}
	// Stale If-Match precondition-fails.
	if _, err := client.Put(ctx, "a/b key.txt", []byte("three"), PutOptions{IfMatch: etag}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("stale etag must fail, got %v", err)
	}
	// Create-only put conflicts with an existing object.
	if _, err := client.Put(ctx, "a/b key.txt", []byte("x"), PutOptions{IfNoneMatchStar: true}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("create on existing object must fail, got %v", err)
	}
	// Missing object maps to ErrObjectNotFound.
	if _, _, err := client.Get(ctx, "missing"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("missing object must map to not-found, got %v", err)
	}
	items, err := client.List(ctx, "a/", "", 0)
	if err != nil || len(items) != 1 || items[0].Key != "a/b key.txt" {
		t.Fatalf("list failed: %#v %v", items, err)
	}
}

func TestS3ObjectStoreOpenStreamsAndPreservesETag(t *testing.T) {
	server := newFakeS3()
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	client, err := NewS3ObjectStore(S3Config{
		Endpoint: httpServer.URL, Region: "eu-central-1", Bucket: "test-bucket",
		AccessKey: "AKID", SecretKey: "secret", PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	etag, err := client.Put(ctx, "resources/blob", []byte("stream me"), PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	body, openedETag, size, err := client.Open(ctx, "resources/blob")
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil || string(data) != "stream me" {
		t.Fatalf("stream read failed: %q %v", data, err)
	}
	if openedETag != etag || size != int64(len(data)) {
		t.Fatalf("stream metadata etag=%q size=%d, want etag=%q size=%d", openedETag, size, etag, len(data))
	}
}

func TestS3ObjectStoreGetRejectsOversizedContentLengthBeforeRead(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", MaxObjectBytes+1))
		w.WriteHeader(http.StatusOK)
	}))
	defer httpServer.Close()
	client, err := NewS3ObjectStore(S3Config{Endpoint: httpServer.URL, Region: "us-east-1", Bucket: "test-bucket", AccessKey: "AKID", SecretKey: "secret", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Get(context.Background(), "large"); !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("oversized S3 Get returned err=%v", err)
	}
}

type countingHTTPBody struct {
	reader io.Reader
	read   int64
}

func (b *countingHTTPBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += int64(n)
	return n, err
}

func (b *countingHTTPBody) Close() error { return nil }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestS3ObjectStoreGetBoundsUnknownContentLength(t *testing.T) {
	body := &countingHTTPBody{reader: bytes.NewReader(make([]byte, MaxObjectBytes+1))}
	client, err := NewS3ObjectStore(S3Config{
		Endpoint: "https://s3.example.test", Region: "us-east-1", Bucket: "test-bucket",
		AccessKey: "AKID", SecretKey: "secret", PathStyle: true,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: body, ContentLength: -1, Header: make(http.Header)}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Get(context.Background(), "large"); !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("oversized S3 Get returned err=%v", err)
	}
	if body.read > MaxObjectBytes+1 {
		t.Fatalf("S3 Get read %d bytes from unknown-length response, want <= %d", body.read, MaxObjectBytes+1)
	}
}

func TestS3ClientBoundsSuccessfulListResponse(t *testing.T) {
	body := &countingHTTPBody{reader: bytes.NewReader(make([]byte, maxS3ListResponseBytes+1))}
	client, err := NewS3ObjectStore(S3Config{
		Endpoint: "https://s3.example.test", Region: "us-east-1", Bucket: "test-bucket",
		AccessKey: "AKID", SecretKey: "secret", PathStyle: true,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: body, ContentLength: -1, Header: make(http.Header)}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.List(context.Background(), "resources", "", 1)
	if !errors.Is(err, ErrS3ResponseTooLarge) {
		t.Fatalf("oversized successful list returned err=%v", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("oversized response error leaked response data: %v", err)
	}
	if body.read > maxS3ListResponseBytes+1 {
		t.Fatalf("successful list read %d bytes, want <= %d", body.read, maxS3ListResponseBytes+1)
	}
}

func TestS3ClientBoundsSuccessfulMutationByContentLength(t *testing.T) {
	body := &countingHTTPBody{reader: bytes.NewReader([]byte("response body must not be read"))}
	client, err := NewS3ObjectStore(S3Config{
		Endpoint: "https://s3.example.test", Region: "us-east-1", Bucket: "test-bucket",
		AccessKey: "AKID", SecretKey: "secret", PathStyle: true,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: body, ContentLength: maxS3SuccessResponseBytes + 1, Header: make(http.Header)}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Put(context.Background(), "resources/object", []byte("payload"), PutOptions{})
	if !errors.Is(err, ErrS3ResponseTooLarge) {
		t.Fatalf("oversized successful mutation returned err=%v", err)
	}
	if body.read != 0 {
		t.Fatalf("known oversized successful response read %d bytes before rejection", body.read)
	}
}

func TestS3ObjectStorePutStreamSignsPayloadAndSupportsCAS(t *testing.T) {
	server := newFakeS3()
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	client, err := NewS3ObjectStore(S3Config{
		Endpoint: httpServer.URL, Region: "eu-central-1", Bucket: "test-bucket",
		AccessKey: "AKID", SecretKey: "secret", PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	etag, err := client.PutStream(ctx, "resources/blob", strings.NewReader("stream me"), 0, PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(server.objects["resources/blob"].body); got != "stream me" {
		t.Fatalf("stream PUT body=%q", got)
	}
	if len(server.authSeen) == 0 || !strings.HasPrefix(server.authSeen[0], "AWS4-HMAC-SHA256") {
		t.Fatalf("stream PUT missing SigV4 authorization: %v", server.authSeen)
	}
	if _, err := client.PutStream(ctx, "resources/blob", strings.NewReader("updated"), 0, PutOptions{IfMatch: etag}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PutStream(ctx, "resources/blob", strings.NewReader("stale"), 0, PutOptions{IfMatch: etag}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("stale stream CAS must fail, got %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := client.PutStream(canceled, "resources/canceled", strings.NewReader("body"), 0, PutOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled S3 stream must stop before upload, got %v", err)
	}
}

func TestFileObjectStoreStreamingRoundTripCASAndLimits(t *testing.T) {
	store, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	etag, err := store.PutStream(ctx, "resources/blob", strings.NewReader("stream me"), 0, PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	body, openedETag, size, err := store.Open(ctx, "resources/blob")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil || string(data) != "stream me" || openedETag != etag || size != int64(len(data)) {
		t.Fatalf("file stream failed: data=%q etag=%q size=%d err=%v", data, openedETag, size, err)
	}
	if _, err := store.PutStream(ctx, "resources/blob", strings.NewReader("stale"), 0, PutOptions{IfMatch: `"stale"`}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("stale stream CAS must fail, got %v", err)
	}
	if _, err := store.PutStream(ctx, "resources/too-large", &fixedByteReader{remaining: MaxObjectBytes * 2, value: 'x'}, MaxObjectBytes, PutOptions{}); !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("oversized stream must fail with size error, got %v", err)
	}
	if _, _, _, err := store.Open(ctx, "resources/too-large"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("failed oversized stream left an object: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.PutStream(canceled, "resources/canceled", strings.NewReader("body"), 0, PutOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled stream must stop before writing, got %v", err)
	}
}

func TestFileObjectStoreStreamingUploadsDifferentKeysOverlap(t *testing.T) {
	store, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	first := &blockingReader{started: make(chan struct{}), release: release, data: []byte("first")}
	second := &blockingReader{started: make(chan struct{}), release: release, data: []byte("second")}
	results := make(chan error, 2)
	go func() {
		_, err := store.PutStream(context.Background(), "a", first, 0, PutOptions{})
		results <- err
	}()
	go func() {
		_, err := store.PutStream(context.Background(), "b", second, 0, PutOptions{})
		results <- err
	}()
	waitForReaderStart := func(reader *blockingReader) bool {
		select {
		case <-reader.started:
			return true
		case <-time.After(time.Second):
			return false
		}
	}
	firstStarted := waitForReaderStart(first)
	secondStarted := waitForReaderStart(second)
	close(release)
	if !firstStarted || !secondStarted {
		t.Fatal("different-key streaming uploads did not overlap before either body was released")
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestFileObjectStoreListHidesStreamingTempButKeepsDotFiles(t *testing.T) {
	store, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), "resources/.keep", []byte("visible"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	body := &blockingReader{started: make(chan struct{}), release: release, data: []byte("in flight")}
	result := make(chan error, 1)
	go func() {
		_, err := store.PutStream(context.Background(), "resources/upload", body, 0, PutOptions{})
		result <- err
	}()
	select {
	case <-body.started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("stream did not reach body before listing")
	}
	items, err := store.List(context.Background(), "resources/", "", 0)
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	for _, item := range items {
		if isObjectStreamTemp(filepath.Base(filepath.FromSlash(item.Key))) {
			close(release)
			t.Fatalf("internal streaming temp leaked from List: %#v", item)
		}
	}
	if len(items) != 1 || items[0].Key != "resources/.keep" {
		close(release)
		t.Fatalf("List should expose only the ordinary dot file during upload: %#v", items)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func BenchmarkFileObjectStorePutStream16MiB(b *testing.B) {
	root, err := os.MkdirTemp("", "harness-file-stream-bench-")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = os.RemoveAll(root) })
	store, err := NewFileObjectStore(root)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(MaxObjectBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := &fixedByteReader{remaining: MaxObjectBytes, value: 'f'}
		if _, err := store.PutStream(context.Background(), "bench/file", reader, 0, PutOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkS3ObjectStorePutStream16MiB(b *testing.B) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		switch r.Method {
		case http.MethodPut:
			w.Header().Set("ETag", `"benchmark"`)
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	b.Cleanup(httpServer.Close)
	client, err := NewS3ObjectStore(S3Config{
		Endpoint: httpServer.URL, Region: "us-east-1", Bucket: "bench",
		AccessKey: "benchmark-access", SecretKey: "benchmark-secret", PathStyle: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(MaxObjectBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := &fixedByteReader{remaining: MaxObjectBytes, value: 's'}
		if _, err := client.PutStream(context.Background(), "bench/object", reader, 0, PutOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func TestFileObjectStoreStreamingCASConflictKeepsCommittedObject(t *testing.T) {
	store, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	oldETag, err := store.Put(ctx, "resource", []byte("old"), PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	body := &blockingReader{started: make(chan struct{}), release: release, data: []byte("streamed")}
	result := make(chan error, 1)
	go func() {
		_, err := store.PutStream(ctx, "resource", body, 0, PutOptions{IfMatch: oldETag})
		result <- err
	}()
	select {
	case <-body.started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("stream did not reach body before CAS race")
	}
	if _, err := store.Put(ctx, "resource", []byte("committed"), PutOptions{}); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err := <-result; !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("stale stream CAS must fail at commit, got %v", err)
	}
	data, _, err := store.Get(ctx, "resource")
	if err != nil || string(data) != "committed" {
		t.Fatalf("failed CAS replaced committed object: data=%q err=%v", data, err)
	}
}

type blockingReader struct {
	started chan struct{}
	release <-chan struct{}
	data    []byte
	once    sync.Once
}

func (r *blockingReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	<-r.release
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

type fixedByteReader struct {
	remaining int64
	value     byte
}

func (r *fixedByteReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > r.remaining {
		n = r.remaining
	}
	for i := 0; i < int(n); i++ {
		p[i] = r.value
	}
	r.remaining -= n
	return int(n), nil
}

func TestFileObjectStoreCASAndList(t *testing.T) {
	store, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	etag, err := store.Put(ctx, "sessions/a/meta.json", []byte("v1"), PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "sessions/a/meta.json", []byte("v2"), PutOptions{IfMatch: etag}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "sessions/a/meta.json", []byte("v3"), PutOptions{IfMatch: etag}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("stale CAS must fail, got %v", err)
	}
	if _, err := store.Put(ctx, "sessions/b/meta.json", []byte("x"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	items, err := store.List(ctx, "sessions/", "", 0)
	if err != nil || len(items) != 2 {
		t.Fatalf("unexpected listing: %#v %v", items, err)
	}
	if items[0].Key >= items[1].Key {
		t.Fatalf("listing not sorted: %#v", items)
	}
}

func TestFileObjectStoreRejectsPathTraversal(t *testing.T) {
	store, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, key := range []string{"../escape", "foo/../../etc/passwd", "/abs", "a\x00b", ""} {
		if _, err := store.Put(ctx, key, []byte("x"), PutOptions{}); err == nil {
			t.Fatalf("file put accepted %q", key)
		}
		if _, _, err := store.Get(ctx, key); err == nil {
			t.Fatalf("file get accepted %q", key)
		}
	}
	if _, err := store.List(ctx, "../", "", 0); err == nil {
		t.Fatal("file list accepted parent prefix")
	}
}

func TestMemoryObjectStoreRejectsPathTraversal(t *testing.T) {
	store := NewMemoryObjectStore()
	ctx := context.Background()
	if _, err := store.Put(ctx, "../escape", []byte("x"), PutOptions{}); err == nil {
		t.Fatal("memory put accepted parent key")
	}
	if _, err := store.List(ctx, "../", "", 0); err == nil {
		t.Fatal("memory list accepted parent prefix")
	}
}

func TestS3ObjectStoreRejectsPathTraversal(t *testing.T) {
	store, err := NewS3ObjectStore(S3Config{
		Endpoint: "http://127.0.0.1:1", Bucket: "bucket", AccessKey: "ak", SecretKey: "sk",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, key := range []string{"../escape", "/abs", "a\x00b", ""} {
		if _, err := store.Put(ctx, key, []byte("x"), PutOptions{}); err == nil || !strings.Contains(err.Error(), "invalid object key") {
			t.Fatalf("s3 put accepted %q: %v", key, err)
		}
		if _, _, err := store.Get(ctx, key); err == nil || !strings.Contains(err.Error(), "invalid object key") {
			t.Fatalf("s3 get accepted %q: %v", key, err)
		}
		if err := store.Delete(ctx, key); err == nil || !strings.Contains(err.Error(), "invalid object key") {
			t.Fatalf("s3 delete accepted %q: %v", key, err)
		}
	}
	if _, err := store.List(ctx, "../", "", 0); err == nil || !strings.Contains(err.Error(), "invalid object key") {
		t.Fatal("s3 list accepted parent prefix")
	}
}

func TestMemoryObjectStoreRejectsOverflow(t *testing.T) {
	store := NewMemoryObjectStore()
	store.maxObjects = 2
	ctx := context.Background()
	if _, err := store.Put(ctx, "a", []byte("1"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "b", []byte("2"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "c", []byte("3"), PutOptions{}); err == nil {
		t.Fatal("memory object overflow was accepted")
	}
	if _, err := store.Put(ctx, "a", []byte("replaced"), PutOptions{}); err != nil {
		t.Fatalf("replacing an existing object was blocked by the cap: %v", err)
	}
	if _, err := store.Put(ctx, "a", make([]byte, MaxObjectBytes+1), PutOptions{}); err == nil {
		t.Fatal("oversized memory object was accepted")
	}
}

func TestFileObjectStoreRejectsOverflow(t *testing.T) {
	store, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store.maxObjects = 2
	ctx := context.Background()
	if _, err := store.Put(ctx, "a", []byte("1"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "b", []byte("2"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "c", []byte("3"), PutOptions{}); err == nil {
		t.Fatal("file object overflow was accepted")
	}
	if _, err := store.Put(ctx, "a", []byte("replaced"), PutOptions{}); err != nil {
		t.Fatalf("replacing an existing file object was blocked by the cap: %v", err)
	}
	if _, err := store.Put(ctx, "a", make([]byte, MaxObjectBytes+1), PutOptions{}); err == nil {
		t.Fatal("oversized file object was accepted")
	}
}

func TestFileObjectStoreDefaultAllowsLargeObjectCount(t *testing.T) {
	store, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	want := MaxMemoryObjects + 1
	for i := 0; i < want; i++ {
		if _, err := store.Put(ctx, fmt.Sprintf("bulk/%04d", i), []byte("x"), PutOptions{}); err != nil {
			t.Fatalf("put %d/%d: %v", i, want, err)
		}
	}
	items, err := store.List(ctx, "bulk/", "", want)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != want {
		t.Fatalf("listed %d objects, want %d", len(items), want)
	}
}

func TestFileObjectStoreListUsesStringPrefixAcrossDirectories(t *testing.T) {
	store, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, key := range []string{"abc", "a-dir/nested", "b"} {
		if _, err := store.Put(ctx, key, []byte(key), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	items, err := store.List(ctx, "a", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Key != "a-dir/nested" || items[1].Key != "abc" {
		t.Fatalf("prefix list=%+v", items)
	}
}

func TestFileObjectStoreListCursorUsesDirectoryPrefixOrdering(t *testing.T) {
	store, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, key := range []string{"a/x", "a/y", "b/x"} {
		if _, err := store.Put(ctx, key, []byte(key), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	items, err := store.List(ctx, "a/", "a!", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Key != "a/x" {
		t.Fatalf("punctuation cursor list=%+v", items)
	}
	items, err = store.List(ctx, "a/", "a0", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("cursor after directory subtree returned=%+v", items)
	}
}

func TestFileObjectStoreListLimitOnePaginatesAcrossDirectories(t *testing.T) {
	store, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, key := range []string{"a/x", "a/y", "b/x", "b/y", "c/x"} {
		if _, err := store.Put(ctx, key, []byte(key), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	var cursor string
	var got []string
	for {
		items, err := store.List(ctx, "", cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(items) == 0 {
			break
		}
		got = append(got, items[0].Key)
		cursor = items[0].Key
	}
	want := []string{"a/x", "a/y", "b/x", "b/y", "c/x"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("paged keys=%v want=%v", got, want)
	}
}

// BenchmarkFileObjectStorePagedList makes the remaining pagination boundary
// explicit: each public List call still starts a fresh WalkDir. The bounded
// page implementation avoids full-result allocation/sort, but this benchmark
// is the guardrail for the future single-walk copier optimization.
func BenchmarkFileObjectStorePagedList(b *testing.B) {
	store, err := NewFileObjectStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	const objectCount = 2048
	for i := 0; i < objectCount; i++ {
		if _, err := store.Put(ctx, fmt.Sprintf("bench/%04d", i), []byte("x"), PutOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor := ""
		for {
			items, err := store.List(ctx, "bench/", cursor, 32)
			if err != nil {
				b.Fatal(err)
			}
			if len(items) == 0 {
				break
			}
			cursor = items[len(items)-1].Key
		}
	}
}

func BenchmarkArtifactCopierFileSingleWalk(b *testing.B) {
	source, err := NewFileObjectStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	const objectCount = 2048
	for i := 0; i < objectCount; i++ {
		if _, err := source.Put(ctx, fmt.Sprintf("bench/%04d", i), []byte("x"), PutOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	target, err := NewFileObjectStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	copier, err := NewArtifactCopier(source, target, ArtifactCopyOptions{PageSize: 32, BatchSize: 32})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := copier.Copy(ctx, "bench/", "", nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkArtifactCopierFilePagedFallback(b *testing.B) {
	sourceBase, err := NewFileObjectStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	const objectCount = 2048
	for i := 0; i < objectCount; i++ {
		if _, err := sourceBase.Put(ctx, fmt.Sprintf("bench/%04d", i), []byte("x"), PutOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	targetBase, err := NewFileObjectStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	source := &countingStreamingStore{base: sourceBase, get: sourceBase, put: sourceBase}
	target := &countingStreamingStore{base: targetBase, get: targetBase, put: targetBase}
	copier, err := NewArtifactCopier(source, target, ArtifactCopyOptions{PageSize: 32, BatchSize: 32})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := copier.Copy(ctx, "bench/", "", nil); err != nil {
			b.Fatal(err)
		}
	}
}

func TestS3SessionStoreRejectsOverflow(t *testing.T) {
	store, err := NewS3SessionStore(NewMemoryObjectStore())
	if err != nil {
		t.Fatal(err)
	}
	store.maxSessions = 2
	ctx := context.Background()
	if err := store.Create(ctx, mustNamedSession(t, "session-s3-1")); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, mustNamedSession(t, "session-s3-2")); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(ctx, mustNamedSession(t, "session-s3-3")); err == nil {
		t.Fatal("s3 session overflow was accepted")
	} else if !strings.Contains(err.Error(), "stored sessions exceed maximum of 2") {
		t.Fatalf("unexpected overflow error: %v", err)
	}
	if err := store.Create(ctx, mustNamedSession(t, "session-s3-1")); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("existing s3 session must still conflict: %v", err)
	}
}
