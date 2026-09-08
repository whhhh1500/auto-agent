// Package console serves the embedded web admin console. The console is a
// zero-build single-page application (vendored Alpine.js, no CDN, no npm)
// that talks exclusively to the same /v1 JSON API as every other client —
// it is a replaceable UI, never a privileged backdoor.
package console

import (
	"crypto/sha256"
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
)

//go:embed static
var staticFS embed.FS

// consoleScriptPaths are the Console assets referenced by the embedded index.
// Keeping this list next to the versioning code makes a missing digest an
// initialization error rather than a silently stale script.
var consoleScriptPaths = []string{
	"/assets/alpine.min.js",
	"/js/api.js",
	"/js/lib.js",
	"/js/auth.js",
	"/js/accounts.js",
	"/js/audit.js",
	"/js/observability.js",
	"/js/policies.js",
	"/js/credentials.js",
	"/js/resources.js",
	"/js/sessions.js",
	"/js/playground.js",
	"/js/bindings.js",
	"/js/storage-settings.js",
	"/js/model-settings.js",
	"/js/notification-platforms.js",
	"/js/notification-targets.js",
	"/js/sandbox.js",
	"/js/shell.js",
	"/js/capabilities.js",
	"/js/profiles.js",
	"/js/approvals.js",
	"/js/evaluations.js",
	"/js/delegations.js",
	"/js/runner-tasks.js",
	"/js/projections.js",
	"/js/app.js",
}

type consoleStaticBundle struct {
	index    []byte
	versions map[string]string
}

// Handler returns the console subtree handler. Mount it with
// http.StripPrefix("/console", Handler()); it serves "/" (the app) and
// "/assets/*" (vendored libraries) and "/js/*" (local Console domains).
func Handler() http.Handler {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic("console: embedded static missing: " + err.Error())
	}
	bundle, err := buildConsoleStaticBundle(sub, consoleScriptPaths)
	if err != nil {
		panic("console: embedded static invalid: " + err.Error())
	}
	assets := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if bundle.hasVersion(r) {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			// Stable URLs can have been cached by an older Console binary. They
			// remain usable, but must revalidate before being reused.
			w.Header().Set("Cache-Control", "no-cache")
		}
		http.FileServer(http.FS(sub)).ServeHTTP(w, r)
	})
	mux := http.NewServeMux()
	mux.Handle("GET /assets/", assets)
	// Console JavaScript is kept as separate local domain files under static/js.
	// The index supplies a digest key for each file; direct stable URLs revalidate.
	mux.Handle("GET /js/", assets)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// The app itself must always be fresh. Its versioned resource URLs let an
		// upgraded binary bypass old immutable cache entries.
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(bundle.index)
	})
	return mux
}

func buildConsoleStaticBundle(content fs.FS, paths []string) (consoleStaticBundle, error) {
	index, err := fs.ReadFile(content, "index.html")
	if err != nil {
		return consoleStaticBundle{}, fmt.Errorf("read index: %w", err)
	}
	bundle := consoleStaticBundle{index: index, versions: make(map[string]string, len(paths))}
	for _, path := range paths {
		data, err := fs.ReadFile(content, strings.TrimPrefix(path, "/"))
		if err != nil {
			return consoleStaticBundle{}, fmt.Errorf("read %s: %w", path, err)
		}
		version := fmt.Sprintf("%x", sha256.Sum256(data))
		needle := `src="/console` + path + `"`
		replacement := `src="/console` + path + `?v=` + version + `"`
		if strings.Count(string(bundle.index), needle) != 1 {
			return consoleStaticBundle{}, fmt.Errorf("index must reference %s exactly once", path)
		}
		bundle.index = []byte(strings.ReplaceAll(string(bundle.index), needle, replacement))
		bundle.versions[path] = version
	}
	return bundle, nil
}

func (b consoleStaticBundle) hasVersion(r *http.Request) bool {
	version, ok := b.versions[r.URL.Path]
	if !ok {
		return false
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return false
	}
	return len(query) == 1 && len(query["v"]) == 1 && query.Get("v") == version
}
