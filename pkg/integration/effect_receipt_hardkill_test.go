package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/pkg/app/effectreceipt"
	. "github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/storage"
	_ "modernc.org/sqlite"
)

const effectHardKillEnv = "HARNESS_EFFECT_HARDKILL_SCENARIO"

type effectHardKillDriver struct{ path string }

func (d effectHardKillDriver) Ref() effectreceipt.DriverRef {
	return effectreceipt.DriverRef{ID: "hardkill-provider", Version: "v1"}
}
func (d effectHardKillDriver) Dispatch(context.Context, effectreceipt.DispatchRequest) (effectreceipt.Submission, error) {
	return effectreceipt.Submission{}, errors.New("recovery must not dispatch")
}
func (d effectHardKillDriver) ReadBack(_ context.Context, intent effectreceipt.Intent) (effectreceipt.Observation, error) {
	b, err := os.ReadFile(d.path)
	if err != nil && !os.IsNotExist(err) {
		return effectreceipt.Observation{}, err
	}
	if strings.Contains(string(b), "dispatch "+intent.OperationKey) {
		return effectreceipt.Observation{State: effectreceipt.ObservationConfirmed, OperationKey: intent.OperationKey, IntentDigest: intent.IntentDigest, EvidenceDigest: effectreceipt.SHA256Digest([]byte("provider-proof"))}, nil
	}
	return effectreceipt.Observation{State: effectreceipt.ObservationPending, OperationKey: intent.OperationKey, IntentDigest: intent.IntentDigest}, nil
}

type allowEffectRecovery struct{}

func (allowEffectRecovery) AuthorizeEffectRecovery(context.Context, effectreceipt.RecoveryRecord) (bool, error) {
	return true, nil
}

func TestEffectReceiptHardKillHelper(t *testing.T) {
	s := os.Getenv(effectHardKillEnv)
	if s == "" {
		return
	}
	db, err := sql.Open("sqlite", os.Getenv("HARNESS_EFFECT_HARDKILL_DB"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	effects, err := storage.NewSQLExternalEffectReceiptStore(db, storage.SQLDialectSQLite)
	if err != nil {
		t.Fatal(err)
	}
	intent := hardKillIntent(t, effectHardKillDriver{path: os.Getenv("HARNESS_EFFECT_HARDKILL_PROVIDER")})
	if _, err := effects.Ensure(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if _, begun, err := effects.BeginDispatch(context.Background(), intent); err != nil || !begun {
		t.Fatalf("begin=%t err=%v", begun, err)
	}
	if s == "provider_before_accept" || s == "confirmed_before_local" {
		if err := os.WriteFile(os.Getenv("HARNESS_EFFECT_HARDKILL_PROVIDER"), []byte("dispatch "+intent.OperationKey), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if s == "confirmed_before_local" {
		reg := effectreceipt.NewDriverRegistry()
		_ = reg.Register(effectHardKillDriver{path: os.Getenv("HARNESS_EFFECT_HARDKILL_PROVIDER")})
		c, _ := effectreceipt.NewRecoveryCoordinator(effects, effects, reg, allowEffectRecovery{})
		if _, err := c.RecoverPage(context.Background(), effectreceipt.RecoveryQuery{Driver: intent.Driver}, effectreceipt.RecoveryCursor{}, 1); err != nil {
			t.Fatal(err)
		}
		_ = store
	}
	if err := os.WriteFile(os.Getenv("HARNESS_EFFECT_HARDKILL_MARKER"), []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
	os.Exit(91)
}

func hardKillIntent(t *testing.T, d effectHardKillDriver) effectreceipt.Intent {
	t.Helper()
	inv := ToolInvocation{TenantID: "tenant", SubjectID: "subject", SessionID: "hardkill-session", RunID: "hardkill-run", CallID: "hardkill-call", CapabilityID: "payment.capture", ArgsDigest: effectreceipt.SHA256Digest([]byte("args")), Idempotent: true}
	in, err := effectreceipt.NewIntent(inv, d.Ref(), []byte("target"), []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	return in
}
func runEffectHardKill(t *testing.T, scenario, db, provider, marker string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestEffectReceiptHardKillHelper$")
	c.Env = append(os.Environ(), effectHardKillEnv+"="+scenario, "HARNESS_EFFECT_HARDKILL_DB="+db, "HARNESS_EFFECT_HARDKILL_PROVIDER="+provider, "HARNESS_EFFECT_HARDKILL_MARKER="+marker)
	out, err := c.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("child timeout %s", out)
	}
	e, ok := err.(*exec.ExitError)
	if !ok || e.ExitCode() != 91 {
		t.Fatalf("child err=%v out=%s", err, out)
	}
}

func TestEffectReceiptHardKillRecovery(t *testing.T) {
	for _, scenario := range []string{"dispatching_before_provider", "provider_before_accept", "confirmed_before_local"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "effects.db")
			provider := filepath.Join(dir, "provider.log")
			marker := filepath.Join(dir, "marker")
			runEffectHardKill(t, scenario, path, provider, marker)
			if b, _ := os.ReadFile(marker); string(b) != scenario {
				t.Fatalf("marker=%q", b)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			sessions, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite)
			if err != nil {
				t.Fatal(err)
			}
			effects, err := storage.NewSQLExternalEffectReceiptStore(db, storage.SQLDialectSQLite)
			if err != nil {
				t.Fatal(err)
			}
			driver := effectHardKillDriver{path: provider}
			reg := effectreceipt.NewDriverRegistry()
			if err := reg.Register(driver); err != nil {
				t.Fatal(err)
			}
			c, err := effectreceipt.NewRecoveryCoordinator(effects, effects, reg, allowEffectRecovery{})
			if err != nil {
				t.Fatal(err)
			}
			intent := hardKillIntent(t, driver)
			if _, err := c.RecoverPage(context.Background(), effectreceipt.RecoveryQuery{Driver: driver.Ref()}, effectreceipt.RecoveryCursor{}, 8); err != nil {
				t.Fatal(err)
			}
			record, found, err := effects.Get(context.Background(), intent)
			if err != nil || !found {
				t.Fatalf("record found=%t err=%v", found, err)
			}
			if scenario == "dispatching_before_provider" {
				if record.State == effectreceipt.StateConfirmed {
					t.Fatal("readback confirmed without provider dispatch")
				}
			} else if record.State != effectreceipt.StateConfirmed {
				t.Fatalf("state=%s", record.State)
			}
			if scenario == "confirmed_before_local" {
				if _, err := sessions.Load(context.Background(), intent.Invocation.SessionID); err == nil {
					t.Fatal("local Session proof unexpectedly exists after ledger-only crash window")
				}
				journal, err := storage.NewSQLToolInvocationJournal(db, storage.SQLDialectSQLite)
				if err != nil {
					t.Fatal(err)
				}
				if _, found, err := journal.GetToolInvocation(context.Background(), intent.Invocation); err != nil || found {
					t.Fatalf("local Tool Journal proof found=%t err=%v", found, err)
				}
			}
			log, _ := os.ReadFile(provider)
			if strings.Count(string(log), "dispatch ") != map[string]int{"dispatching_before_provider": 0, "provider_before_accept": 1, "confirmed_before_local": 1}[scenario] {
				t.Fatalf("provider log=%q", log)
			}
		})
	}
}
