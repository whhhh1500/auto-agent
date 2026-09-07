package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestS3SessionStoreRejectsInvalidCommittedVersion(t *testing.T) {
	for _, version := range []int64{-1, int64(core.MaxSessionEvents) + 1} {
		t.Run(fmt.Sprintf("version_%d", version), func(t *testing.T) {
			store, server, session := newS3SessionPrefixFixture(t)
			meta := setS3SessionPrefixVersion(t, server, session.ID(), version)
			err, panicked := s3SessionErrorWithoutPanic(func() error {
				_, err := store.Load(context.Background(), session.ID())
				return err
			})
			if panicked {
				t.Fatal("S3 Load panicked on invalid committed version")
			}
			if err == nil {
				t.Fatal("S3 Load accepted invalid committed version")
			}
			err, panicked = s3SessionErrorWithoutPanic(func() error {
				_, err := store.loadCanonicalForEvidence(context.Background(), session.ID(), meta)
				return err
			})
			if panicked {
				t.Fatal("S3 canonical restore panicked on invalid committed version")
			}
			if err == nil {
				t.Fatal("S3 canonical restore accepted invalid committed version")
			}
			err, panicked = s3SessionErrorWithoutPanic(func() error {
				_, err := store.QueryEvidence(context.Background(), EvidenceQuery{Limit: 1})
				return err
			})
			if panicked {
				t.Fatal("S3 evidence fallback panicked on invalid committed version")
			}
			if err == nil {
				t.Fatal("S3 evidence fallback accepted invalid committed version")
			}
		})
	}
}

func TestS3SessionStoreAllowsZeroAndNormalCommittedPrefixes(t *testing.T) {
	t.Run("zero version", func(t *testing.T) {
		store, _, session := newS3SessionPrefixFixture(t)
		loaded, err := store.Load(context.Background(), session.ID())
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Version() != 0 {
			t.Fatalf("zero-version S3 load version=%d", loaded.Version())
		}
	})
	t.Run("normal prefix", func(t *testing.T) {
		store, server, session := newS3SessionPrefixFixture(t)
		if _, err := session.Append("run-s3-prefix", core.EvUserMessage, core.UserMessageData{Text: "persisted"}); err != nil {
			t.Fatal(err)
		}
		if err := store.Save(context.Background(), session, 0); err != nil {
			t.Fatal(err)
		}
		loaded, err := store.Load(context.Background(), session.ID())
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Version() != 1 {
			t.Fatalf("normal S3 prefix version=%d", loaded.Version())
		}
		meta := setS3SessionPrefixVersion(t, server, session.ID(), 1)
		canonical, err := store.loadCanonicalForEvidence(context.Background(), session.ID(), meta)
		if err != nil {
			t.Fatal(err)
		}
		if canonical.Version() != 1 {
			t.Fatalf("normal S3 canonical prefix version=%d", canonical.Version())
		}
	})
}

func newS3SessionPrefixFixture(t *testing.T) (*S3SessionStore, *fakeS3, *core.Session) {
	t.Helper()
	store, server := newFakeS3Store(t)
	session := mustNamedSession(t, "session-s3-prefix")
	if err := store.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	return store, server, session
}

func setS3SessionPrefixVersion(t *testing.T, server *fakeS3, sessionID string, version int64) s3SessionMeta {
	t.Helper()
	server.mu.Lock()
	defer server.mu.Unlock()
	key := s3MetaKey(sessionID)
	object, ok := server.objects[key]
	if !ok {
		t.Fatalf("missing S3 session meta %q", key)
	}
	var meta s3SessionMeta
	if err := json.Unmarshal(object.body, &meta); err != nil {
		t.Fatal(err)
	}
	meta.Version = version
	payload, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	server.objects[key] = fakeObject{body: payload, etag: server.etagFor(payload)}
	return meta
}

func s3SessionErrorWithoutPanic(call func() error) (err error, panicked bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("S3 session restore panicked: %v", recovered)
			panicked = true
		}
	}()
	return call(), false
}
