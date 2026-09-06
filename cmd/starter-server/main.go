// starter-server is a copyable development assembly, not the generic server.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/cc-auto-agent/harness-core/examples/starter"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	openai "github.com/cc-auto-agent/harness-core/pkg/provider/openai"
	"github.com/cc-auto-agent/harness-core/pkg/storage"

	_ "modernc.org/sqlite"
)

func main() {
	must(starterModeAllowed(os.Getenv("HARNESS_MODE")))
	llm, err := openai.NewOpenAIAdapterFromEnv()
	must(err)
	sessions, closeStore := sessionStore()
	defer closeStore()
	api, err := starter.NewServer(starter.Config{
		LLM: llm, Model: os.Getenv("HARNESS_LLM_MODEL"), HTTPURL: os.Getenv("HARNESS_STARTER_HTTP_URL"), Sessions: sessions,
	})
	must(err)
	port := os.Getenv("HARNESS_SERVER_PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("starter server listening on :%s; use X-Harness-Tenant and X-Harness-Subject only for local development", port)
	log.Printf("profile: %s; HTTP example: %s", starter.ProfileID, "read-only public endpoint")
	must(http.ListenAndServe(":"+port, api.Handler()))
}

func sessionStore() (core.SessionStore, func()) {
	path := os.Getenv("HARNESS_STARTER_SQLITE_PATH")
	if path == "" {
		return core.NewMemorySessionStore(), func() {}
	}
	if path != ":memory:" {
		if err := privateMkdirAll(filepath.Dir(path)); err != nil {
			must(fmt.Errorf("create starter SQLite directory: %w", err))
		}
	}
	db, err := sql.Open("sqlite", path)
	must(err)
	store, err := storage.OpenSQLSessionStore(context.Background(), db, storage.SQLDialectSQLite)
	if err != nil {
		_ = db.Close()
		must(err)
	}
	if path != ":memory:" {
		if err := privateChmod(path, 0o600); err != nil {
			_ = db.Close()
			must(fmt.Errorf("secure starter SQLite file: %w", err))
		}
	}
	return store, func() { _ = db.Close() }
}

func starterModeAllowed(raw string) error {
	if mode := strings.ToLower(strings.TrimSpace(raw)); mode != "" && mode != "dev" {
		return fmt.Errorf("starter-server is development-only; HARNESS_MODE must be dev or unset (use cmd/server for production)")
	}
	return nil
}

// privateMkdirAll keeps the starter's local SQLite parent private. Windows
// ACLs are managed by the host; POSIX deployments get an explicit 0700 mode.
func privateMkdirAll(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		return nil
	}
	return privateChmod(path, 0o700)
}

func privateChmod(path string, mode os.FileMode) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	if err := os.Chmod(path, mode); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Mode().Perm() != mode.Perm() {
		return fmt.Errorf("private path %s mode=%#o want %#o", path, info.Mode().Perm(), mode.Perm())
	}
	return nil
}

func must(err error) {
	if err != nil {
		panic(fmt.Sprintf("starter bootstrap failed: %v", err))
	}
}
