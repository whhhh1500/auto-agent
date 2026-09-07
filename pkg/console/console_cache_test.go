package console

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"
)

func TestConsoleVersionedAssetsBypassStableCacheEntries(t *testing.T) {
	handler := Handler()
	indexResponse := httptest.NewRecorder()
	handler.ServeHTTP(indexResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if indexResponse.Code != http.StatusOK {
		t.Fatalf("console index status=%d", indexResponse.Code)
	}
	if got := indexResponse.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("console index Cache-Control=%q, want no-store", got)
	}

	references := make(map[string]*url.URL, len(consoleScriptPaths))
	for _, match := range regexp.MustCompile(`(?i)<script[^>]+src="([^"]+)"`).FindAllStringSubmatch(indexResponse.Body.String(), -1) {
		asset, err := url.Parse(match[1])
		if err != nil {
			t.Fatalf("parse script URL %q: %v", match[1], err)
		}
		references[strings.TrimPrefix(asset.Path, "/console")] = asset
	}
	if len(references) != len(consoleScriptPaths) {
		t.Fatalf("versioned script references=%d, want %d", len(references), len(consoleScriptPaths))
	}

	var firstPath, firstVersion string
	checkedCrossResourceVersion := false
	for _, path := range consoleScriptPaths {
		asset := references[path]
		if asset == nil {
			t.Fatalf("index missing versioned script %s", path)
		}
		versions := asset.Query()["v"]
		if len(asset.Query()) != 1 || len(versions) != 1 || len(versions[0]) != 64 {
			t.Fatalf("script %s query=%q, want one full SHA-256 version", path, asset.RawQuery)
		}

		versionedResponse := httptest.NewRecorder()
		handler.ServeHTTP(versionedResponse, httptest.NewRequest(http.MethodGet, path+"?"+asset.RawQuery, nil))
		if versionedResponse.Code != http.StatusOK {
			t.Fatalf("versioned asset %s status=%d", path, versionedResponse.Code)
		}
		if got := versionedResponse.Header().Get("Cache-Control"); got != "public, max-age=31536000, immutable" {
			t.Fatalf("versioned asset %s Cache-Control=%q", path, got)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(versionedResponse.Body.Bytes())); got != versions[0] {
			t.Fatalf("versioned asset %s digest=%s, want %s", path, got, versions[0])
		}

		for _, query := range []string{
			"",
			"?v=" + strings.Repeat("0", 64),
			"?v=" + versions[0] + "&v=" + versions[0],
			"?v=" + versions[0] + "&unexpected=1",
		} {
			stableResponse := httptest.NewRecorder()
			handler.ServeHTTP(stableResponse, httptest.NewRequest(http.MethodGet, path+query, nil))
			if stableResponse.Code != http.StatusOK {
				t.Fatalf("stable asset %s%s status=%d", path, query, stableResponse.Code)
			}
			if got := stableResponse.Header().Get("Cache-Control"); got != "no-cache" {
				t.Fatalf("stable asset %s%s Cache-Control=%q, want no-cache", path, query, got)
			}
		}
		if firstPath == "" {
			firstPath, firstVersion = path, versions[0]
		} else if !checkedCrossResourceVersion && firstVersion != versions[0] {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path+"?v="+firstVersion, nil))
			if got := response.Header().Get("Cache-Control"); got != "no-cache" {
				t.Fatalf("asset %s accepted %s version %q: Cache-Control=%q", path, firstPath, firstVersion, got)
			}
			checkedCrossResourceVersion = true
		}
	}
	if !checkedCrossResourceVersion {
		t.Fatal("test fixture needs two resources with different digests")
	}

	malformed := httptest.NewRequest(http.MethodGet, "/js/app.js", nil)
	malformed.URL.RawQuery = "v=" + references["/js/app.js"].Query().Get("v") + "&bad=%ZZ"
	malformedResponse := httptest.NewRecorder()
	handler.ServeHTTP(malformedResponse, malformed)
	if got := malformedResponse.Header().Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("malformed query Cache-Control=%q, want no-cache", got)
	}
}

func TestConsoleStaticBundleDigestChangesOnlyWithContent(t *testing.T) {
	newBundle := func(script string) consoleStaticBundle {
		t.Helper()
		bundle, err := buildConsoleStaticBundle(fstest.MapFS{
			"index.html": {Data: []byte(`<script src="/console/js/app.js"></script>`)},
			"js/app.js":  {Data: []byte(script)},
		}, []string{"/js/app.js"})
		if err != nil {
			t.Fatalf("build static bundle: %v", err)
		}
		return bundle
	}

	first := newBundle("window.app = 1")
	same := newBundle("window.app = 1")
	changed := newBundle("window.app = 2")
	if first.versions["/js/app.js"] != same.versions["/js/app.js"] {
		t.Fatal("same script content must retain its cache key")
	}
	if first.versions["/js/app.js"] == changed.versions["/js/app.js"] {
		t.Fatal("changed script content must receive a new cache key")
	}
	if !strings.Contains(string(changed.index), `src="/console/js/app.js?v=`+changed.versions["/js/app.js"]+`"`) {
		t.Fatal("index must reference the content-derived cache key")
	}
}
