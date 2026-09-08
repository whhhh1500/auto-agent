package storage

import (
	"context"
	. "github.com/whhhh1500/auto-agent/pkg/core"
	"os"
	"testing"
	"time"
)

// Smoke test against a live S3-compatible server (e.g. local RustFS):
//
//	HARNESS_SMOKE_S3_ENDPOINT=http://127.0.0.1:9000 \
//	HARNESS_SMOKE_S3_BUCKET=harness \
//	HARNESS_SMOKE_S3_ACCESS_KEY=... HARNESS_SMOKE_S3_SECRET_KEY=... \
//	go test ./pkg/storage -run TestSmokeS3 -v
//
// Skipped when the endpoint env is unset. Auto-creates the bucket.
func TestSmokeS3(t *testing.T) {
	endpoint := os.Getenv("HARNESS_SMOKE_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("HARNESS_SMOKE_S3_ENDPOINT not set")
	}
	store, err := NewS3ObjectStore(S3Config{
		Endpoint:  endpoint,
		Region:    "us-east-1",
		Bucket:    os.Getenv("HARNESS_SMOKE_S3_BUCKET"),
		AccessKey: os.Getenv("HARNESS_SMOKE_S3_ACCESS_KEY"),
		SecretKey: os.Getenv("HARNESS_SMOKE_S3_SECRET_KEY"),
		PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// EnsureBucket: create when missing (idempotent).
	if err := store.EnsureBucket(ctx); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}

	// Round trip.
	key := "smoke/" + time.Now().Format("150405") + ".txt"
	if _, err := store.Put(ctx, key, []byte("hello rustfs"), PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	data, etag, err := store.Get(ctx, key)
	if err != nil || string(data) != "hello rustfs" {
		t.Fatalf("get: %q %q %v", data, etag, err)
	}
	items, err := store.List(ctx, "smoke/", "", 10)
	if err != nil || len(items) == 0 {
		t.Fatalf("list: %#v %v", items, err)
	}
	// Conditional write (S3SessionStore meta CAS relies on this).
	if _, err := store.Put(ctx, key, []byte("hello again"), PutOptions{IfMatch: etag}); err != nil {
		t.Fatalf("conditional put (If-Match) unsupported: %v", err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}

	// Session store round trip on the live server.
	sessions, err := NewS3SessionStore(store)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, user := testScopes()
	principal := testPrincipal(user)
	session := mustSession(t, user, principal)
	if err := sessions.Create(ctx, session); err != nil {
		t.Fatal(err)
	}
	session.Append("run-smoke", EvUserMessage, UserMessageData{Text: "rustfs smoke"})
	if err := sessions.Save(ctx, session, 0); err != nil {
		t.Fatalf("save: %v", err)
	}
	loaded, err := sessions.Load(ctx, session.ID())
	if err != nil || loaded.Version() != session.Version() {
		t.Fatalf("session round trip: %d vs %d (%v)", loaded.Version(), session.Version(), err)
	}
}
