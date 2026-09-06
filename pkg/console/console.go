// Package console serves the embedded web admin console. The console is a
// zero-build single-page application (vendored Alpine.js, no CDN, no npm)
// that talks exclusively to the same /v1 JSON API as every other client —
// it is a replaceable UI, never a privileged backdoor.
package console

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static
var staticFS embed.FS

// Handler returns the console subtree handler. Mount it with
// http.StripPrefix("/console", Handler()); it serves "/" (the app) and
// "/assets/*" (vendored libraries) and "/js/*" (local Console domains).
func Handler() http.Handler {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic("console: embedded static missing: " + err.Error())
	}
	assets := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Vendored assets are immutable: content never changes without a new
		// binary, so aggressive caching is safe.
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		http.FileServer(http.FS(sub)).ServeHTTP(w, r)
	})
	mux := http.NewServeMux()
	mux.Handle("GET /assets/", assets)
	// Console JavaScript is kept as separate local domain files under static/js
	// and is embedded in the same binary as the HTML and vendored Alpine.js.
	mux.Handle("GET /js/", assets)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		index, err := staticFS.ReadFile("static/index.html")
		if err != nil {
			http.Error(w, "console index missing", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// The app itself must always be fresh; only assets are immutable.
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(index)
	})
	return mux
}
