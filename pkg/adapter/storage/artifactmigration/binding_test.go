package artifactmigration

import (
	"context"
	"errors"
	"testing"

	app "github.com/whhhh1500/auto-agent/pkg/app/artifactmigration"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

type recordingMutationSink struct {
	items []app.MutationEvidence
}

func (s *recordingMutationSink) Record(_ context.Context, evidence app.MutationEvidence) error {
	s.items = append(s.items, evidence)
	return nil
}

func TestBindingCopiesReconcilesAndActivatesWithoutStorageTypesInAppPort(t *testing.T) {
	ctx := context.Background()
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := local.Put(ctx, "existing", []byte("before"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	router := storage.NewDynamicObjectStore(local, "local")
	sink := &recordingMutationSink{}
	binding, err := NewBinding(router, target, "s3", storage.ArtifactCopyOptions{PageSize: 1, BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	copyPort, err := binding.Bind(ctx, "migration-1", sink)
	if err != nil {
		t.Fatal(err)
	}
	var progress int
	stats, err := copyPort.Copy(ctx, "", "", func(_ context.Context, p app.TransferProgress) error {
		progress++
		if p.LastKey == "" {
			t.Fatal("copy progress lost lexical cursor")
		}
		return nil
	})
	if err != nil || stats.Objects != 1 || progress != 1 {
		t.Fatalf("copy stats=%+v progress=%d err=%v", stats, progress, err)
	}
	verified, err := copyPort.Reconcile(ctx, "", nil)
	if err != nil || verified.Objects != 1 || verified.Bytes != int64(len("before")) {
		t.Fatalf("reconcile stats=%+v err=%v", verified, err)
	}
	if _, err := router.Put(ctx, "new", []byte("during"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	var nilContext context.Context
	if err := binding.Activate(nilContext, "migration-1", func(barrier context.Context, finalCopy app.ArtifactCopy) error {
		return finalCopy.ApplyAndVerify(barrier, []app.KeyChange{{Key: "new"}})
	}); err != nil {
		t.Fatal(err)
	}
	if router.Label() != "s3" {
		t.Fatalf("active label=%q, want s3", router.Label())
	}
	data, _, err := router.Get(ctx, "new")
	if err != nil || string(data) != "during" {
		t.Fatalf("activated route data=%q err=%v", data, err)
	}
	if len(sink.items) != 1 || sink.items[0].Operation != app.MutationPut || sink.items[0].Key != "new" {
		t.Fatalf("mutation evidence=%+v", sink.items)
	}
	if err := binding.RestoreActive(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBindingSameIDRetryReusesExistingAttachment(t *testing.T) {
	ctx := context.Background()
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	router := storage.NewDynamicObjectStore(local, "local")
	binding, err := NewBinding(router, target, "s3", storage.ArtifactCopyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	firstSink := &recordingMutationSink{}
	if _, err := binding.Bind(ctx, "migration-retry", firstSink); err != nil {
		t.Fatal(err)
	}
	if _, err := binding.Bind(ctx, "migration-retry", &recordingMutationSink{}); err != nil {
		t.Fatalf("same-ID retry failed: %v", err)
	}
	if _, err := router.Put(ctx, "still-recorded", []byte("local"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if len(firstSink.items) != 1 || firstSink.items[0].Key != "still-recorded" {
		t.Fatalf("same-ID retry replaced stable recorder: %#v", firstSink.items)
	}
	binding.Cancel("migration-retry")
}

func TestBindingRejectsWrongIDAttachAndRestoreSwapFailure(t *testing.T) {
	ctx := context.Background()
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	router := storage.NewDynamicObjectStore(local, "local")
	binding, err := NewBinding(router, target, "s3", storage.ArtifactCopyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := binding.Bind(ctx, "migration-current", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := binding.Bind(ctx, "migration-wrong", nil); err == nil {
		t.Fatal("wrong-ID attach unexpectedly succeeded")
	}
	if _, _, err := router.ArtifactMigrationStores("migration-current"); err != nil {
		t.Fatalf("wrong-ID attach disturbed current migration: %v", err)
	}
	if err := binding.RestoreActive(ctx); !errors.Is(err, storage.ErrArtifactMigrationActive) {
		t.Fatalf("restore while attached error=%v", err)
	}
	if router.Label() != "local" {
		t.Fatalf("failed restore changed route to %q", router.Label())
	}
	binding.Cancel("migration-current")
}

func TestBindingActivationFailureKeepsLocalRoute(t *testing.T) {
	ctx := context.Background()
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	router := storage.NewDynamicObjectStore(local, "local")
	binding, err := NewBinding(router, target, "s3", storage.ArtifactCopyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := binding.Bind(ctx, "migration-failed", nil); err != nil {
		t.Fatal(err)
	}
	want := errors.New("verification failed")
	if err := binding.Activate(ctx, "migration-failed", func(context.Context, app.ArtifactCopy) error { return want }); !errors.Is(err, want) {
		t.Fatalf("activation err=%v, want %v", err, want)
	}
	if router.Label() != "local" {
		t.Fatalf("failed activation changed route to %q", router.Label())
	}
	binding.Cancel("migration-failed")
}
