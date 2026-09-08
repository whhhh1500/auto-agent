package artifactmigration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlmigration "github.com/whhhh1500/auto-agent/pkg/adapter/sql/artifactmigration"
	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	storagemigration "github.com/whhhh1500/auto-agent/pkg/adapter/storage/artifactmigration"
	app "github.com/whhhh1500/auto-agent/pkg/app/artifactmigration"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	_ "modernc.org/sqlite"
)

func TestCoordinatorCopiesReplaysAndActivatesAfterPersistentState(t *testing.T) {
	repository, closeDB := newRepository(t)
	defer closeDB()
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := local.Put(ctx, "keep", []byte("before"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	controller, err := app.NewCoordinator(repository, "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	activated := false
	prepared, err := controller.Prepare(ctx, request(dynamic, target, func(_ context.Context, migration app.Migration) error {
		loaded, found, err := repository.Load(context.Background())
		if err != nil || !found || loaded.State != app.StateS3Active || loaded.ID != migration.ID {
			t.Fatalf("callback after durable active: %#v found=%t err=%v", loaded, found, err)
		}
		activated = true
		return nil
	}))
	if err != nil || !prepared.Prepared {
		t.Fatalf("prepare=%#v err=%v", prepared, err)
	}
	if _, err := dynamic.Put(ctx, "keep", []byte("after"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := dynamic.Put(ctx, "new", []byte("new"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := dynamic.Delete(ctx, "keep"); err != nil {
		t.Fatal(err)
	}
	status, err := controller.RunOnce(ctx)
	if err != nil || !activated || status.Migration.State != app.StateS3Active || status.Migration.ActivatedAt.IsZero() || dynamic.Label() != "s3" {
		t.Fatalf("run=%#v callback=%t label=%q err=%v", status, activated, dynamic.Label(), err)
	}
	persisted, found, err := repository.Load(context.Background())
	if err != nil || !found || persisted.ActivatedAt.IsZero() || persisted.ActivatedAt.Location() != time.UTC {
		t.Fatalf("persisted activation=%#v found=%t err=%v", persisted, found, err)
	}
	if _, _, err := dynamic.Get(ctx, "new"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := dynamic.Get(ctx, "keep"); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatalf("deleted key=%v", err)
	}
}

// This restart-boundary test closes and reopens the durable SQLite handle and
// recreates process-local routers/bindings around the same FileObjectStore
// roots. It proves recovery from the prepared state, durable mutation digest
// evidence, and writes made while the resumed streaming copy is live.
func TestCoordinatorRestartsWithDiskSQLiteAndFileStores(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dbPath := filepath.Join(t.TempDir(), "artifact-migration.db")
	firstDB, firstRepository := openPersistentRepository(t, dbPath)
	localRoot := t.TempDir()
	localBase, err := storage.NewFileObjectStore(localRoot)
	if err != nil {
		t.Fatal(err)
	}
	targetRoot := t.TempDir()
	target, err := storage.NewFileObjectStore(targetRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := localBase.Put(ctx, "seed", []byte("seed"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := localBase.Put(ctx, "remove-before-restart", []byte("old"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	firstRouter := storage.NewDynamicObjectStore(localBase, "local")
	first, err := app.NewCoordinator(firstRepository, "worker-before-restart", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	firstRequest := request(firstRouter, target, nil)
	if _, err := first.Prepare(ctx, firstRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := firstRouter.Put(ctx, "before-restart", []byte("durable"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := firstRouter.Delete(ctx, "remove-before-restart"); err != nil {
		t.Fatal(err)
	}
	beforeRestart, found, err := firstRepository.Load(ctx)
	if err != nil || !found || beforeRestart.ListingCursor != "" || beforeRestart.MutationHighWater != 2 {
		t.Fatalf("pre-restart migration=%#v found=%t err=%v", beforeRestart, found, err)
	}
	if err := firstDB.Close(); err != nil {
		t.Fatal(err)
	}

	secondDB, secondRepository := openPersistentRepository(t, dbPath)
	// A fresh DynamicObjectStore models the process-local route attachment
	// disappearing across restart. Blocking its first source read lets writes
	// race the resumed streaming copy and prove the new recorder is durable.
	restartedSource := newBlockingOpenStore(localBase, 0)
	released := false
	defer func() {
		if !released {
			close(restartedSource.release)
		}
	}()
	secondRouter := storage.NewDynamicObjectStore(restartedSource, "local")
	second, err := app.NewCoordinator(secondRepository, "worker-after-restart", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := second.Prepare(ctx, request(secondRouter, target, nil))
	if err != nil || !prepared.Prepared || prepared.Migration.ID != beforeRestart.ID || prepared.Migration.ListingCursor != beforeRestart.ListingCursor || prepared.Migration.MutationHighWater != beforeRestart.MutationHighWater {
		t.Fatalf("restart prepare=%#v err=%v", prepared, err)
	}
	mutations, err := secondRepository.ListMutations(ctx, beforeRestart.ID, 0, 8)
	if err != nil || len(mutations) != 2 {
		t.Fatalf("reopened pending mutations=%#v err=%v", mutations, err)
	}
	for _, mutation := range mutations {
		if mutation.Operation == app.MutationPut && mutation.Digest == "" {
			t.Fatalf("put mutation lost digest after restart: %#v", mutation)
		}
	}
	runDone := make(chan struct {
		status app.Status
		err    error
	}, 1)
	go func() {
		status, runErr := second.RunOnce(ctx)
		runDone <- struct {
			status app.Status
			err    error
		}{status, runErr}
	}()
	select {
	case <-restartedSource.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("restarted migration did not begin streaming copy")
	}
	var writes sync.WaitGroup
	writes.Add(2)
	go func() {
		defer writes.Done()
		if _, err := secondRouter.Put(ctx, "during-restart", []byte("replayed"), storage.PutOptions{}); err != nil {
			t.Errorf("concurrent restarted put: %v", err)
		}
	}()
	go func() {
		defer writes.Done()
		if err := secondRouter.Delete(ctx, "seed"); err != nil {
			t.Errorf("concurrent restarted delete: %v", err)
		}
	}()
	writes.Wait()
	close(restartedSource.release)
	released = true
	var outcome struct {
		status app.Status
		err    error
	}
	select {
	case outcome = <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("restarted migration did not complete")
	}
	if outcome.err != nil || outcome.status.Migration.State != app.StateS3Active || secondRouter.Label() != "s3" || outcome.status.Migration.ListingCursor == "" || outcome.status.Migration.MutationHighWater != 4 {
		t.Fatalf("restart run=%#v label=%q err=%v", outcome.status, secondRouter.Label(), outcome.err)
	}
	for key, want := range map[string]string{"before-restart": "durable", "during-restart": "replayed"} {
		body, _, getErr := secondRouter.Get(ctx, key)
		if getErr != nil || string(body) != want {
			t.Fatalf("activated target key=%q body=%q err=%v", key, body, getErr)
		}
	}
	for _, key := range []string{"remove-before-restart", "seed"} {
		if _, _, getErr := secondRouter.Get(ctx, key); !errors.Is(getErr, storage.ErrObjectNotFound) {
			t.Fatalf("activated target retained deleted key=%q err=%v", key, getErr)
		}
	}
	if err := secondDB.Close(); err != nil {
		t.Fatal(err)
	}

	// Rebuild all process-local storage objects after activation. Prepare must
	// restore the persisted active target instead of assuming the old router is
	// still available in memory.
	thirdDB, thirdRepository := openPersistentRepository(t, dbPath)
	defer thirdDB.Close()
	reopenedLocal, err := storage.NewFileObjectStore(localRoot)
	if err != nil {
		t.Fatal(err)
	}
	reopenedTarget, err := storage.NewFileObjectStore(targetRoot)
	if err != nil {
		t.Fatal(err)
	}
	thirdRouter := storage.NewDynamicObjectStore(reopenedLocal, "local")
	third, err := app.NewCoordinator(thirdRepository, "worker-after-active-restart", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := third.Prepare(ctx, request(thirdRouter, reopenedTarget, nil))
	if err != nil || restored.Prepared || restored.Migration.State != app.StateS3Active || restored.Migration.ListingCursor != outcome.status.Migration.ListingCursor || thirdRouter.Label() != "s3" {
		t.Fatalf("active restart restore=%#v label=%q err=%v", restored, thirdRouter.Label(), err)
	}
	for key, want := range map[string]string{"before-restart": "durable", "during-restart": "replayed"} {
		body, _, getErr := thirdRouter.Get(ctx, key)
		if getErr != nil || string(body) != want {
			t.Fatalf("restored target key=%q body=%q err=%v", key, body, getErr)
		}
	}
	for _, key := range []string{"remove-before-restart", "seed"} {
		if _, _, getErr := thirdRouter.Get(ctx, key); !errors.Is(getErr, storage.ErrObjectNotFound) {
			t.Fatalf("restored target retained deleted key=%q err=%v", key, getErr)
		}
	}
}

func TestCoordinatorMutationAppendFailureLeavesLocalAndRejectsActivation(t *testing.T) {
	base, closeDB := newRepository(t)
	defer closeDB()
	repository := failingAppendRepository{Repository: base, err: errors.New("journal unavailable")}
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	controller, err := app.NewCoordinator(repository, "worker-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Prepare(context.Background(), request(dynamic, target, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := dynamic.Put(context.Background(), "committed-local", []byte("value"), storage.PutOptions{}); !errors.Is(err, repository.err) {
		t.Fatalf("put journal error=%v", err)
	}
	if _, _, err := local.Get(context.Background(), "committed-local"); err != nil {
		t.Fatalf("local write was rolled back: %v", err)
	}
	status, err := controller.RunOnce(context.Background())
	if !errors.Is(err, app.ErrMutationConflict) || !status.Fatal || dynamic.Label() != "local" {
		t.Fatalf("run=%#v label=%q err=%v", status, dynamic.Label(), err)
	}
	stored, found, err := base.Load(context.Background())
	if err != nil || !found || stored.State != app.StateApplyFailed || stored.ErrorCode != "mutation_journal_failed" {
		t.Fatalf("stored=%#v found=%t err=%v", stored, found, err)
	}
	retryTarget, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	retry := request(dynamic, retryTarget, nil)
	retry.MigrationID, retry.DesiredRevision = "migration-after-failure", "rev_after_failure"
	prepared, err := controller.Prepare(context.Background(), retry)
	if err != nil || !prepared.Prepared || prepared.Migration.ID != retry.MigrationID || dynamic.Label() != "local" {
		t.Fatalf("prepare after failure=%#v label=%q err=%v", prepared, dynamic.Label(), err)
	}
}

func TestCoordinatorStaleLeaseCannotRun(t *testing.T) {
	repository, closeDB := newRepository(t)
	defer closeDB()
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	controller, err := app.NewCoordinator(repository, "worker-c", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Prepare(context.Background(), request(dynamic, target, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.ClaimLease(context.Background(), "other", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.RunOnce(context.Background()); !errors.Is(err, app.ErrLeaseConflict) {
		t.Fatalf("stale lease run=%v", err)
	}
}

func TestCoordinatorProjectionFailureDoesNotRollbackActivatedRoute(t *testing.T) {
	repository, closeDB := newRepository(t)
	defer closeDB()
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	controller, err := app.NewCoordinator(repository, "worker-projection", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("projection unavailable")
	if _, err := controller.Prepare(context.Background(), request(dynamic, target, func(context.Context, app.Migration) error { return want })); err != nil {
		t.Fatal(err)
	}
	status, err := controller.RunOnce(context.Background())
	if !errors.Is(err, want) || status.Migration.State != app.StateS3Active || dynamic.Label() != "s3" {
		t.Fatalf("status=%#v label=%q err=%v", status, dynamic.Label(), err)
	}
}

func TestCoordinatorBlockedProjectionUsesBoundedContext(t *testing.T) {
	repository, closeDB := newRepository(t)
	defer closeDB()
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	controller, err := app.NewCoordinator(repository, "worker-blocked-projection", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Prepare(context.Background(), request(dynamic, target, func(ctx context.Context, _ app.Migration) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			return errors.New("projection context has no deadline")
		}
		if remaining := time.Until(deadline); remaining <= 0 || remaining > app.DurableOperationTimeout {
			return errors.New("projection context has an invalid deadline")
		}
		return context.DeadlineExceeded
	})); err != nil {
		t.Fatal(err)
	}
	status, err := controller.RunOnce(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || status.Migration.State != app.StateS3Active || dynamic.Label() != "s3" {
		t.Fatalf("status=%#v label=%q err=%v", status, dynamic.Label(), err)
	}
}

func TestCoordinatorPrepareReplacesPendingDesiredRevisionAndRouter(t *testing.T) {
	repository, closeDB := newRepository(t)
	defer closeDB()
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	targetA, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	targetB, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	controller, err := app.NewCoordinator(repository, "worker-replace", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	first := request(dynamic, targetA, nil)
	first.MigrationID, first.DesiredRevision = "migration-a", "rev_a"
	if _, err := controller.Prepare(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := request(dynamic, targetB, nil)
	second.MigrationID, second.DesiredRevision = "migration-b", "rev_b"
	status, err := controller.Prepare(context.Background(), second)
	if err != nil || !status.Prepared || status.Migration.ID != "migration-b" || dynamic.Label() != "local" {
		t.Fatalf("replace=%#v label=%q err=%v", status, dynamic.Label(), err)
	}
	if _, err := repository.AppendMutation(context.Background(), app.MutationAppend{Token: app.MutationToken{MigrationID: "migration-a", Generation: 1, DesiredRevision: "rev_a"}, Operation: app.MutationPut, ObjectKey: "stale"}); !errors.Is(err, app.ErrMutationConflict) {
		t.Fatalf("old token=%v", err)
	}
}

func TestCoordinatorPrepareDoesNotDetachRouteWhenCancellationCASConflicts(t *testing.T) {
	base, closeDB := newRepository(t)
	defer closeDB()
	repository := &failNextCASRepository{Repository: base}
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	targetA, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	targetB, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	controller, err := app.NewCoordinator(repository, "worker-replace-conflict", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	first := request(dynamic, targetA, nil)
	first.MigrationID, first.DesiredRevision = "migration-replace-a", "rev_a"
	if _, err := controller.Prepare(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	repository.fail.Store(true)
	second := request(dynamic, targetB, nil)
	second.MigrationID, second.DesiredRevision = "migration-replace-b", "rev_b"
	if _, err := controller.Prepare(context.Background(), second); !errors.Is(err, app.ErrMigrationConflict) {
		t.Fatalf("replacement CAS error=%v", err)
	}
	if _, _, err := dynamic.ArtifactMigrationStores("migration-replace-a"); err != nil {
		t.Fatalf("old route detached after failed durable cancellation: %v", err)
	}
	stored, found, err := base.Load(context.Background())
	if err != nil || !found || stored.ID != "migration-replace-a" || stored.State != app.StateSyncing {
		t.Fatalf("durable old migration=%#v found=%t err=%v", stored, found, err)
	}
}

func TestCoordinatorSameIDRetryClearsAttachmentAfterOriginalRecorderFails(t *testing.T) {
	base, closeDB := newRepository(t)
	defer closeDB()
	repository := &toggleAppendFailureRepository{Repository: base}
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	first, err := app.NewCoordinator(repository, "worker-original-recorder", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := app.NewCoordinator(repository, "worker-retry-recorder", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	firstRequest := request(dynamic, target, nil)
	if _, err := first.Prepare(context.Background(), firstRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Prepare(context.Background(), firstRequest); err != nil {
		t.Fatalf("same-ID prepare retry: %v", err)
	}
	repository.fail.Store(true)
	if _, err := dynamic.Put(context.Background(), "committed-before-journal-failure", []byte("local"), storage.PutOptions{}); err == nil {
		t.Fatal("foreground write unexpectedly hid journal failure")
	}
	status, err := second.RunOnce(context.Background())
	if err != nil || status.Migration.State != app.StateApplyFailed || dynamic.Label() != "local" {
		t.Fatalf("terminal observation=%#v label=%q err=%v", status, dynamic.Label(), err)
	}
	if _, _, err := dynamic.ArtifactMigrationStores(firstRequest.MigrationID); err == nil {
		t.Fatal("terminal observation left the migration attachment installed")
	}
	if _, _, err := local.Get(context.Background(), "committed-before-journal-failure"); err != nil {
		t.Fatalf("local authoritative write was lost: %v", err)
	}
	nextTarget, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	next := request(dynamic, nextTarget, nil)
	next.MigrationID, next.DesiredRevision = "migration-after-original-recorder-failure", "rev_after_original_recorder_failure"
	prepared, err := second.Prepare(context.Background(), next)
	if err != nil || !prepared.Prepared || prepared.Migration.ID != next.MigrationID || dynamic.Label() != "local" {
		t.Fatalf("prepare after terminal attachment cleanup=%#v label=%q err=%v", prepared, dynamic.Label(), err)
	}
}

func TestCoordinatorCancelCurrentFencesPendingWorker(t *testing.T) {
	repository, closeDB := newRepository(t)
	defer closeDB()
	localBase, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := localBase.Put(context.Background(), "wait", []byte("local"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	local := newBlockingOpenStore(localBase, 0)
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	controller, err := app.NewCoordinator(repository, "worker-cancel", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Prepare(context.Background(), request(dynamic, target, nil)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := controller.RunOnce(context.Background())
		done <- err
	}()
	select {
	case <-local.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not reach blocked copy")
	}
	cancelled, err := controller.CancelCurrent(context.Background(), "rev_controller")
	if err != nil || cancelled.Migration.State != app.StateCancelled || dynamic.Label() != "local" {
		t.Fatalf("cancel=%#v label=%q err=%v", cancelled, dynamic.Label(), err)
	}
	close(local.release)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stale worker completed after cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stale worker did not stop")
	}
	stored, found, err := repository.Load(context.Background())
	if err != nil || !found || stored.State != app.StateCancelled || dynamic.Label() != "local" {
		t.Fatalf("stored=%#v found=%t label=%q err=%v", stored, found, dynamic.Label(), err)
	}
}

func TestCoordinatorCancelCurrentDoesNotRevertS3Active(t *testing.T) {
	repository, closeDB := newRepository(t)
	defer closeDB()
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	controller, err := app.NewCoordinator(repository, "worker-active-cancel", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Prepare(context.Background(), request(dynamic, target, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err := controller.CancelCurrent(context.Background(), "rev_controller")
	if !errors.Is(err, app.ErrMigrationConflict) || status.Migration.State != app.StateS3Active || dynamic.Label() != "s3" {
		t.Fatalf("cancel active=%#v label=%q err=%v", status, dynamic.Label(), err)
	}
}

func TestCoordinatorCancelCurrentDoesNotCancelReplacedDesiredRevision(t *testing.T) {
	repository, closeDB := newRepository(t)
	defer closeDB()
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	targetA, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	targetB, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	first, err := app.NewCoordinator(repository, "worker-cancel-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := app.NewCoordinator(repository, "worker-cancel-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	requestA := request(dynamic, targetA, nil)
	requestA.MigrationID, requestA.DesiredRevision = "migration-cancel-a", "rev_a"
	if _, err := first.Prepare(context.Background(), requestA); err != nil {
		t.Fatal(err)
	}
	requestB := request(dynamic, targetB, nil)
	requestB.MigrationID, requestB.DesiredRevision = "migration-cancel-b", "rev_b"
	prepared, err := second.Prepare(context.Background(), requestB)
	if err != nil || !prepared.Prepared || prepared.Migration.DesiredRevision != "rev_b" {
		t.Fatalf("replacement=%#v err=%v", prepared, err)
	}
	status, err := first.CancelCurrent(context.Background(), "rev_a")
	if !errors.Is(err, app.ErrMigrationConflict) || status.Migration.DesiredRevision != "rev_b" || status.Migration.State != app.StateSyncing {
		t.Fatalf("stale cancellation=%#v err=%v", status, err)
	}
	stored, found, err := repository.Load(context.Background())
	if err != nil || !found || stored.DesiredRevision != "rev_b" || stored.State != app.StateSyncing || dynamic.Label() != "local" {
		t.Fatalf("stored=%#v found=%t label=%q err=%v", stored, found, dynamic.Label(), err)
	}
}

func TestCoordinatorCancelCurrentValidatesExpectedDesiredRevision(t *testing.T) {
	repository, closeDB := newRepository(t)
	defer closeDB()
	controller, err := app.NewCoordinator(repository, "worker-cancel-validation", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"", string(make([]byte, app.MaxDesiredRevisionBytes+1))} {
		if _, err := controller.CancelCurrent(context.Background(), expected); !errors.Is(err, app.ErrInvalidMigration) {
			t.Fatalf("expected=%q error=%v", expected, err)
		}
	}
}

func TestCoordinatorLargeActivationDeltaReplaysOutsideBarrier(t *testing.T) {
	base, closeDB := newRepository(t)
	defer closeDB()
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	repository := &verificationUpdateHookRepository{Repository: base}
	var injected atomic.Bool
	repository.hook = func() {
		if !injected.CompareAndSwap(false, true) {
			return
		}
		for i := 0; i < storage.MaxArtifactActivationKeys+1; i++ {
			if _, err := dynamic.Put(context.Background(), fmt.Sprintf("delta/%03d", i), []byte("new"), storage.PutOptions{}); err != nil {
				t.Errorf("inject mutation %d: %v", i, err)
				return
			}
		}
	}
	controller, err := app.NewCoordinator(repository, "worker-large-delta", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	prepare := requestWithOptions(dynamic, target, nil, storage.ArtifactCopyOptions{PageSize: 32, BatchSize: 32})
	if _, err := controller.Prepare(context.Background(), prepare); err != nil {
		t.Fatal(err)
	}
	status, err := controller.RunOnce(context.Background())
	if err != nil || status.Migration.State != app.StateS3Active || status.Fatal || !injected.Load() {
		t.Fatalf("large delta run=%#v injected=%t err=%v", status, injected.Load(), err)
	}
	stored, found, err := base.Load(context.Background())
	if err != nil || !found || stored.State != app.StateS3Active || stored.ErrorCode != "" {
		t.Fatalf("stored=%#v found=%t err=%v", stored, found, err)
	}
	if _, _, err := dynamic.Get(context.Background(), "delta/000"); err != nil {
		t.Fatalf("activated target missing replayed key: %v", err)
	}
}

func TestCoordinatorReconcilesAfterConcurrentOverwriteAndDelete(t *testing.T) {
	base, closeDB := newRepository(t)
	defer closeDB()
	localBase, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := localBase.Put(context.Background(), "a", []byte("before"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := localBase.Put(context.Background(), "b", []byte("remove"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	local := newBlockingOpenStore(localBase, 2)
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	controller, err := app.NewCoordinator(base, "worker-high-water", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Prepare(context.Background(), request(dynamic, target, nil)); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan struct{})
	var status app.Status
	var runErr error
	go func() {
		status, runErr = controller.RunOnce(context.Background())
		close(runDone)
	}()
	select {
	case <-local.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reconciliation did not reach the blocked source read")
	}
	if _, err := dynamic.Put(context.Background(), "a", []byte("after"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := dynamic.Delete(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	close(local.release)
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("coordinator did not finish after high-water replay")
	}
	if runErr != nil || status.Migration.State != app.StateS3Active || status.Fatal || dynamic.Label() != "s3" {
		t.Fatalf("high-water run=%#v label=%q err=%v", status, dynamic.Label(), runErr)
	}
	if _, _, err := dynamic.Get(context.Background(), "a"); err != nil {
		t.Fatalf("overwrite missing after activation: %v", err)
	}
	if _, _, err := dynamic.Get(context.Background(), "b"); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatalf("delete missing after activation: %v", err)
	}
}

func TestCoordinatorRepairsUnjournaledSourceMismatchBeforeActivation(t *testing.T) {
	base, closeDB := newRepository(t)
	defer closeDB()
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := local.Put(context.Background(), "gap", []byte("before"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	repository := &firstVerificationHookRepository{Repository: base}
	var injected atomic.Bool
	repository.hook = func() {
		if injected.CompareAndSwap(false, true) {
			// Simulate the crash-window shape: the authoritative local file
			// changes without a journal sequence. The verifier must repair it
			// from local before it can activate.
			if _, err := local.Put(context.Background(), "gap", []byte("after"), storage.PutOptions{}); err != nil {
				t.Errorf("inject unjournaled local change: %v", err)
			}
		}
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	controller, err := app.NewCoordinator(repository, "worker-reconcile-repair", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Prepare(context.Background(), request(dynamic, target, nil)); err != nil {
		t.Fatal(err)
	}
	status, err := controller.RunOnce(context.Background())
	if err != nil || !injected.Load() || status.Migration.State != app.StateS3Active || dynamic.Label() != "s3" {
		t.Fatalf("repair status=%#v injected=%t label=%q err=%v", status, injected.Load(), dynamic.Label(), err)
	}
	body, _, err := dynamic.Get(context.Background(), "gap")
	if err != nil || string(body) != "after" {
		t.Fatalf("repaired body=%q err=%v", body, err)
	}
}

func TestCoordinatorDoesNotDeleteUnprovenTargetExtra(t *testing.T) {
	repository, closeDB := newRepository(t)
	defer closeDB()
	local, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := local.Put(context.Background(), "owned", []byte("local"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Put(context.Background(), "unproven", []byte("target-only"), storage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	dynamic := storage.NewDynamicObjectStore(local, "local")
	controller, err := app.NewCoordinator(repository, "worker-extra-fail-closed", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Prepare(context.Background(), request(dynamic, target, nil)); err != nil {
		t.Fatal(err)
	}
	status, err := controller.RunOnce(context.Background())
	if err == nil || status.Migration.State != app.StateApplyFailed || dynamic.Label() != "local" {
		t.Fatalf("unproven extra status=%#v label=%q err=%v", status, dynamic.Label(), err)
	}
	if _, _, err := target.Get(context.Background(), "unproven"); err != nil {
		t.Fatalf("unproven target extra was removed: %v", err)
	}
}

func request(router *storage.DynamicObjectStore, target storage.ObjectStore, activate app.ActivationCallback) app.PrepareRequest {
	return requestWithOptions(router, target, activate, storage.ArtifactCopyOptions{PageSize: 1, BatchSize: 1})
}

func requestWithOptions(router *storage.DynamicObjectStore, target storage.ObjectStore, activate app.ActivationCallback, options storage.ArtifactCopyOptions) app.PrepareRequest {
	binding, err := storagemigration.NewBinding(router, target, "s3", options)
	if err != nil {
		panic(err)
	}
	return app.PrepareRequest{MigrationID: "migration-controller", DesiredRevision: "rev_controller", Source: app.BackendIdentity{Backend: app.BackendLocal, Identity: "local"}, Target: app.BackendIdentity{Backend: app.BackendS3, Identity: "target"}, Runtime: binding, OnActivated: activate}
}

func newRepository(t *testing.T) (*sqlmigration.Store, func()) {
	t.Helper()
	// Controller tests exercise the SQLite adapter contract, not filesystem
	// durability. A private shared in-memory database avoids one fsync per
	// journal append in the bounded high-water test.
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(t.TempDir(), "controller.db"))+"?mode=memory&cache=shared&_pragma=busy_timeout%285000%29")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	repository, err := sqlmigration.New(db, sqlkit.SQLite)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return repository, func() { _ = db.Close() }
}

func openPersistentRepository(t *testing.T, path string) (*sql.DB, *sqlmigration.Store) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout%285000%29")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	repository, err := sqlmigration.New(db, sqlkit.SQLite)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db, repository
}

type failingAppendRepository struct {
	app.Repository
	err error
}

type failNextCASRepository struct {
	app.Repository
	fail atomic.Bool
}

type toggleAppendFailureRepository struct {
	app.Repository
	fail atomic.Bool
}

func (repository *toggleAppendFailureRepository) AppendMutation(ctx context.Context, command app.MutationAppend) (app.Mutation, error) {
	if repository.fail.Load() {
		return app.Mutation{}, errors.New("journal append intentionally unavailable")
	}
	return repository.Repository.AppendMutation(ctx, command)
}

func (repository *failNextCASRepository) CompareAndSwap(ctx context.Context, expected uint64, next app.Migration) (app.Migration, error) {
	if expected != 0 && repository.fail.CompareAndSwap(true, false) {
		return app.Migration{}, app.ErrMigrationConflict
	}
	return repository.Repository.CompareAndSwap(ctx, expected, next)
}

func (repository failingAppendRepository) AppendMutation(context.Context, app.MutationAppend) (app.Mutation, error) {
	return app.Mutation{}, repository.err
}

type verificationUpdateHookRepository struct {
	app.Repository
	once             sync.Once
	verifyingUpdates atomic.Int32
	hook             func()
}

// firstVerificationHookRepository injects a source-side change immediately
// before the first full reconciliation. It models the local-commit/journal-
// append crash window without producing journal evidence.
type firstVerificationHookRepository struct {
	app.Repository
	once sync.Once
	hook func()
}

func (repository *firstVerificationHookRepository) UpdateLeaseHeld(ctx context.Context, lease app.Lease, expected uint64, next app.Migration) (app.Migration, error) {
	if next.State == app.StateVerifying && repository.hook != nil {
		repository.once.Do(repository.hook)
	}
	return repository.Repository.UpdateLeaseHeld(ctx, lease, expected, next)
}

func (repository *verificationUpdateHookRepository) UpdateLeaseHeld(ctx context.Context, lease app.Lease, expected uint64, next app.Migration) (app.Migration, error) {
	// The first StateVerifying write is the normal state transition from
	// catching_up. The second is the post-Reconcile checkpoint; inject the
	// delta there so finalChanges, rather than the initial replay, sees it.
	if next.State == app.StateVerifying && repository.hook != nil && repository.verifyingUpdates.Add(1) == 2 {
		repository.once.Do(repository.hook)
	}
	return repository.Repository.UpdateLeaseHeld(ctx, lease, expected, next)
}

type blockingOpenStore struct {
	base        storage.ObjectStore
	getter      storage.StreamingObjectGetter
	putter      storage.StreamingObjectPutter
	blockAt     int64
	opens       atomic.Int64
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
}

func newBlockingOpenStore(base storage.ObjectStore, blockAfter int) *blockingOpenStore {
	getter, getterOK := base.(storage.StreamingObjectGetter)
	putter, putterOK := base.(storage.StreamingObjectPutter)
	if !getterOK || !putterOK {
		panic("blocking test store requires streaming base")
	}
	return &blockingOpenStore{base: base, getter: getter, putter: putter, blockAt: int64(blockAfter) + 1, entered: make(chan struct{}), release: make(chan struct{})}
}

func (store *blockingOpenStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	return store.base.Get(ctx, key)
}
func (store *blockingOpenStore) Put(ctx context.Context, key string, data []byte, opts storage.PutOptions) (string, error) {
	return store.base.Put(ctx, key, data, opts)
}
func (store *blockingOpenStore) Delete(ctx context.Context, key string) error {
	return store.base.Delete(ctx, key)
}
func (store *blockingOpenStore) List(ctx context.Context, prefix, startAfter string, limit int) ([]storage.ObjectItem, error) {
	return store.base.List(ctx, prefix, startAfter, limit)
}
func (store *blockingOpenStore) Open(ctx context.Context, key string) (io.ReadCloser, string, int64, error) {
	if store.opens.Add(1) == store.blockAt {
		store.enteredOnce.Do(func() { close(store.entered) })
		select {
		case <-store.release:
		case <-ctx.Done():
			return nil, "", 0, ctx.Err()
		}
	}
	return store.getter.Open(ctx, key)
}
func (store *blockingOpenStore) PutStream(ctx context.Context, key string, body io.Reader, maxBytes int64, opts storage.PutOptions) (string, error) {
	return store.putter.PutStream(ctx, key, body, maxBytes, opts)
}
