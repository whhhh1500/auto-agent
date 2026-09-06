package compositionstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cc-auto-agent/harness-core/pkg/adapter/sql/sqlkit"
	"github.com/cc-auto-agent/harness-core/pkg/runtime"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresCompositionStoreRoundTripAndCAS(t *testing.T) {
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("HARNESS_TEST_PG_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	adminConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	adminDB := stdlib.OpenDB(*adminConfig)
	if err := adminDB.PingContext(ctx); err != nil {
		_ = adminDB.Close()
		t.Fatalf("postgres ping failed: %v", err)
	}
	schema := fmt.Sprintf("compositionstore_test_%d", time.Now().UnixNano())
	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = adminDB.Close()
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = adminDB.ExecContext(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		_ = adminDB.Close()
	}()

	firstDB := openPostgresSchema(t, dsn, schema)
	defer firstDB.Close()
	secondDB := openPostgresSchema(t, dsn, schema)
	defer secondDB.Close()
	if _, err := storage.OpenSQLSessionStore(ctx, firstDB, storage.SQLDialectPostgres); err != nil {
		t.Fatal(err)
	}
	first, err := New(firstDB, sqlkit.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(secondDB, sqlkit.Postgres)
	if err != nil {
		t.Fatal(err)
	}

	initial := validState("pg-revision-1")
	if err := first.CompareAndSwap(ctx, "", initial); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := second.Load(ctx)
	if err != nil || !found || loaded.StoreRevision != initial.StoreRevision {
		t.Fatalf("postgres load state=%#v found=%t err=%v", loaded, found, err)
	}
	updated := validState("pg-revision-2")
	if err := second.CompareAndSwap(ctx, initial.StoreRevision, updated); err != nil {
		t.Fatal(err)
	}
	if err := first.CompareAndSwap(ctx, initial.StoreRevision, validState("pg-stale")); !errors.Is(err, runtime.ErrCompositionConflict) {
		t.Fatalf("postgres stale CAS err=%v", err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var group sync.WaitGroup
	for index, store := range []*Store{first, second} {
		group.Add(1)
		go func(store *Store, revision string) {
			defer group.Done()
			<-start
			errs <- store.CompareAndSwap(context.Background(), updated.StoreRevision, validState(revision))
		}(store, fmt.Sprintf("pg-next-%d", index))
	}
	close(start)
	group.Wait()
	close(errs)
	successes, conflicts := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, runtime.ErrCompositionConflict):
			conflicts++
		default:
			t.Fatalf("postgres concurrent CAS err=%v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("postgres concurrent CAS successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestPostgresCompositionStoreOwnershipGenerationFence(t *testing.T) {
	firstDB, secondDB := newPostgresCompositionStoreHandles(t)
	first, err := New(firstDB, sqlkit.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(secondDB, sqlkit.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	start := make(chan struct{})
	claims := make(chan struct {
		claim runtime.HostOwnershipClaim
		err   error
	}, 2)
	var group sync.WaitGroup
	for index, store := range []*Store{first, second} {
		group.Add(1)
		go func(index int, store *Store) {
			defer group.Done()
			<-start
			claim, err := store.ClaimHostOwnership(ctx, fmt.Sprintf("pg-owner-%d", index), time.Minute)
			claims <- struct {
				claim runtime.HostOwnershipClaim
				err   error
			}{claim: claim, err: err}
		}(index, store)
	}
	close(start)
	group.Wait()
	close(claims)

	var winner runtime.HostOwnershipClaim
	wins, conflicts := 0, 0
	for result := range claims {
		switch {
		case result.err == nil:
			winner = result.claim
			wins++
		case errors.Is(result.err, runtime.ErrHostOwnershipConflict):
			conflicts++
		default:
			t.Fatalf("concurrent ownership claim=%#v err=%v", result.claim, result.err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("ownership wins=%d conflicts=%d, want one each", wins, conflicts)
	}

	winnerStore := first
	if winner.Holder == "pg-owner-1" {
		winnerStore = second
	}
	if err := winnerStore.ReleaseHostOwnership(ctx, winner); err != nil {
		t.Fatalf("release initial ownership: %v", err)
	}

	nextHolder := "pg-next-owner"
	next, err := winnerStore.ClaimHostOwnership(ctx, nextHolder, time.Minute)
	if err != nil {
		t.Fatalf("claim after release: %v", err)
	}
	if next.Generation <= winner.Generation {
		t.Fatalf("ownership generation=%d, want greater than released generation=%d", next.Generation, winner.Generation)
	}

	renewed, err := winnerStore.RenewHostOwnership(ctx, next, time.Minute)
	if err != nil {
		t.Fatalf("renew current ownership: %v", err)
	}
	if renewed.Generation != next.Generation || renewed.Holder != next.Holder {
		t.Fatalf("renewed claim=%#v, want same holder/generation as %#v", renewed, next)
	}
	if err := winnerStore.ValidateHostOwnership(ctx, renewed); err != nil {
		t.Fatalf("validate renewed ownership: %v", err)
	}

	for name, operation := range map[string]func() error{
		"renew stale": func() error {
			_, err := winnerStore.RenewHostOwnership(ctx, winner, time.Minute)
			return err
		},
		"validate stale": func() error {
			return winnerStore.ValidateHostOwnership(ctx, winner)
		},
		"release stale": func() error {
			return winnerStore.ReleaseHostOwnership(ctx, winner)
		},
	} {
		if err := operation(); !errors.Is(err, runtime.ErrHostOwnershipLost) {
			t.Errorf("%s error=%v, want ErrHostOwnershipLost", name, err)
		}
		if err := winnerStore.ValidateHostOwnership(ctx, renewed); err != nil {
			t.Errorf("current ownership after %s: %v", name, err)
		}
	}
}

func newPostgresCompositionStoreHandles(t *testing.T) (*sql.DB, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("HARNESS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("HARNESS_TEST_PG_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	adminConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	adminDB := stdlib.OpenDB(*adminConfig)
	if err := adminDB.PingContext(ctx); err != nil {
		_ = adminDB.Close()
		t.Fatalf("postgres ping failed: %v", err)
	}
	schema := fmt.Sprintf("compositionstore_ownership_test_%d", time.Now().UnixNano())
	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = adminDB.Close()
		t.Fatal(err)
	}
	firstDB := openPostgresSchema(t, dsn, schema)
	secondDB := openPostgresSchema(t, dsn, schema)
	if _, err := storage.OpenSQLSessionStore(ctx, firstDB, storage.SQLDialectPostgres); err != nil {
		_ = firstDB.Close()
		_ = secondDB.Close()
		_, _ = adminDB.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE")
		_ = adminDB.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = firstDB.Close()
		_ = secondDB.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := adminDB.ExecContext(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop postgres test schema: %v", err)
		}
		_ = adminDB.Close()
	})
	return firstDB, secondDB
}

func openPostgresSchema(t *testing.T, dsn, schema string) *sql.DB {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(4)
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db
}
