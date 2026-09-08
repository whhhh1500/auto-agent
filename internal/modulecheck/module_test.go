package modulecheck

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/whhhh1500/auto-agent"

const legacyModulePath = "harness" + "-core"

func TestModulePathAndImportsDoNotUseLegacyModule(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate module check source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	goMod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(goMod), "module "+modulePath+"\n") {
		t.Fatalf("go.mod does not declare %s", modulePath)
	}
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		// The repository may contain local build caches (for example
		// .codex-cache) whose Go fixtures are not repository source and need not
		// be valid for the active compiler. Only scan tracked-source-shaped
		// directories; hidden directories are tooling state, not import owners.
		if entry.IsDir() && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "bin" || entry.Name() == "data") {
			return filepath.SkipDir
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		legacy, parseErr := hasLegacyModuleImport(path, contents)
		if parseErr != nil {
			return parseErr
		}
		if legacy {
			return &legacyImportError{path: path}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// hasLegacyModuleImport examines Go import declarations instead of raw source
// text. Protocol domains and user-facing documentation may legitimately name
// this repository, but they are not source dependencies and must not weaken
// the legacy-module-path guard.
func hasLegacyModuleImport(path string, source []byte) (bool, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.ImportsOnly)
	if err != nil {
		return false, err
	}
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return false, err
		}
		if strings.HasPrefix(importPath, legacyModulePath+"/") {
			return true, nil
		}
	}
	return false, nil
}

func TestLegacyModuleImportDetectionExaminesImportsOnly(t *testing.T) {
	tests := map[string]struct {
		source string
		want   bool
	}{
		"protocol domain is not an import": {
			source: "package test\nconst domain = \"harness-core/sandboxrunner/v1\"\n",
		},
		"legacy import is rejected": {
			source: "package test\nimport \"harness-core/old\"\n",
			want:   true,
		},
		"current module import is allowed": {
			source: "package test\nimport \"github.com/whhhh1500/auto-agent/pkg/core\"\n",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := hasLegacyModuleImport(name+".go", []byte(test.source))
			if err != nil || got != test.want {
				t.Fatalf("hasLegacyModuleImport() = (%v, %v), want (%v, nil)", got, err, test.want)
			}
		})
	}
}

// TestCoreImportsOnlyStandardLibrary is intentionally a family-level check.
// New packages below pkg/core inherit the same stable-foundation constraint.
// The check uses go list rather than source-text matching so aliases, grouped
// imports, and generated formatting cannot evade the rule.
func TestCoreImportsOnlyStandardLibrary(t *testing.T) {
	packages := loadPackageImports(t)
	for _, pkg := range packages {
		if !inFamily(pkg.ImportPath, "core") {
			continue
		}
		for _, imported := range pkg.Imports {
			if isStandardLibrary(imported) {
				continue
			}
			t.Errorf("%s imports non-standard-library package %q; core is the stable stdlib-only foundation", pkg.ImportPath, imported)
		}
	}
}

// TestGraphImportsOnlyStandardLibrary keeps the Graph-G0 contract usable by
// independent extensions. Execution, storage, transport, providers, and
// sandbox implementations must enter only through later verticals.
func TestGraphImportsOnlyStandardLibrary(t *testing.T) {
	packages := loadPackageImports(t)
	for _, pkg := range packages {
		if !inSubtree(pkg.ImportPath, "extensions/graph") {
			continue
		}
		for _, imported := range pkg.Imports {
			if !isStandardLibrary(imported) {
				t.Errorf("%s imports non-standard-library package %q; Graph-G0 is stdlib-only", pkg.ImportPath, imported)
			}
		}
	}
}

// TestSQLKitImportsOnlyStandardLibrary keeps the SQL placeholder adapter
// reusable by database adapters without inheriting product dependencies.
func TestSQLKitImportsOnlyStandardLibrary(t *testing.T) {
	packages := loadPackageImports(t)
	for _, pkg := range packages {
		if !inSubtree(pkg.ImportPath, "adapter/sql/sqlkit") {
			continue
		}
		for _, imported := range pkg.Imports {
			if isStandardLibrary(imported) {
				continue
			}
			t.Errorf("%s imports non-standard-library package %q; sqlkit is a stdlib-only SQL adapter foundation", pkg.ImportPath, imported)
		}
	}
}

func TestNoGenericTopLevelPackages(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate module check source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	entries, err := os.ReadDir(filepath.Join(root, "pkg"))
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]bool{
		"common":  true,
		"enums":   true,
		"helpers": true,
		"models":  true,
		"utils":   true,
	}
	for _, entry := range entries {
		if entry.IsDir() && forbidden[entry.Name()] {
			t.Errorf("generic top-level package pkg/%s is forbidden; place code with its owning domain or adapter", entry.Name())
		}
	}
}

func TestPublicInventoryMatchesPackageList(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate module check source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	raw, err := os.ReadFile(filepath.Join(root, "docs", "superpowers", "inventory", "2026-09-02-public-api-inventory.json"))
	if err != nil {
		t.Fatal(err)
	}
	var inventory struct {
		PublicPackages []struct {
			Package string `json:"package"`
		} `json:"public_packages"`
	}
	if err := json.Unmarshal(raw, &inventory); err != nil {
		t.Fatalf("decode public API inventory: %v", err)
	}
	got := make(map[string]bool, len(inventory.PublicPackages))
	for _, entry := range inventory.PublicPackages {
		if entry.Package == "" || got[entry.Package] {
			t.Fatalf("inventory contains an empty or duplicate package: %q", entry.Package)
		}
		got[entry.Package] = true
	}
	actual := loadPackageImports(t)
	publicCount := 0
	for _, pkg := range actual {
		if strings.Contains(pkg.ImportPath, "/internal/") {
			continue
		}
		publicCount++
	}
	if len(got) != publicCount {
		t.Fatalf("public inventory has %d packages, go list reports %d public packages", len(got), publicCount)
	}
	missing := []string{}
	for _, pkg := range actual {
		if strings.Contains(pkg.ImportPath, "/internal/") {
			continue
		}
		name := strings.TrimPrefix(pkg.ImportPath, modulePath+"/")
		if !got[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("public inventory is missing packages: %s", strings.Join(missing, ", "))
	}
}

// TestFoundationalDependencyBaseline records the dependency direction that is
// already true in the pre-migration tree. It is deliberately narrower than the
// target architecture: server, storage, and other legacy facades are not
// checked yet because their migration is a later phase.
func TestFoundationalDependencyBaseline(t *testing.T) {
	packages := loadPackageImports(t)
	rules := []dependencyRule{
		{
			name:    "shared HTTP JSON codec has no internal product dependency",
			from:    "adapter/httpapi/jsonbody",
			allowed: nil,
		},
		{
			name:    "storage HTTP DTOs have no internal product dependency",
			from:    "adapter/httpapi/storage",
			allowed: nil,
		},
		{
			name:    "application packages other than explicit seams may depend only on core among internal packages",
			from:    "app",
			allowed: []string{"core"},
			except:  []string{"app/settings", "app/modelsettings", "app/secretview", "app/storageconfig", "app/modelexecution"},
		},
		{
			name:    "model execution application contract may depend only on model control evidence",
			from:    "app/modelexecution",
			allowed: []string{"app/modelcontrol"},
		},
		{
			name:    "settings application package may depend only on identity among internal packages",
			from:    "app/settings",
			allowed: []string{"app/identity"},
		},
		{
			name: "model settings application package may depend only on identity, secret view, and the model catalog value seam",
			from: "app/modelsettings",
			allowed: []string{
				"app/identity",
				"app/modelcatalog",
				"app/secretview",
			},
		},
		{
			name:    "storage configuration application package may depend only on identity and secret view",
			from:    "app/storageconfig",
			allowed: []string{"app/identity", "app/secretview"},
		},
		{
			name:    "secret preview application package has no internal product dependency",
			from:    "app/secretview",
			allowed: nil,
		},
		{
			name:    "model settings HTTP DTOs have no internal product dependency",
			from:    "adapter/httpapi/modelsettings",
			allowed: nil,
		},
		{
			name:    "SQL settings adapter may depend only on settings application and sqlkit among internal packages",
			from:    "adapter/sql/settings",
			allowed: []string{"app/settings", "adapter/sql/sqlkit"},
		},
		{
			name:    "model settings persistence adapter may depend only on model settings and generic settings applications",
			from:    "adapter/modelsettings",
			allowed: []string{"app/modelsettings", "app/settings"},
		},
		{
			name:    "evaluation may depend only on core among internal packages",
			from:    "evaluation",
			allowed: []string{"core"},
		},
		{
			name:    "control may depend on core and evaluation among internal packages",
			from:    "control",
			allowed: []string{"core", "evaluation"},
		},
		{
			name:    "legacy OpenAI facade may depend only on the explicit M2 execution bridge and core",
			from:    "provider/openai",
			allowed: []string{"core", "app/modelcontrol", "app/modelexecution", "adapter/modelexecution/corebridge", "adapter/modelexecution/openai"},
		},
		{
			name:    "OTel adapter may depend only on core among internal packages",
			from:    "telemetry/otel",
			allowed: []string{"core"},
		},
		{
			name:    "extensions may depend on core and private extension support",
			from:    "extensions",
			allowed: []string{"core", "extensions/internal/support"},
		},
		{
			name:    "execution keeps its current core, tool-library, and graph-contract edges",
			from:    "execution",
			allowed: []string{"core", "extensions/toollib", "extensions/graph"},
			except:  []string{"execution/sandbox"},
		},
		{
			name:    "Windows local sandbox provider has no setup or helper control-plane edge",
			from:    "execution/sandbox",
			allowed: []string{"core", "extensions/toollib", "extensions/graph"},
		},
		{
			name:      "model provider registration is runtime-only and transport-free",
			from:      "adapter/modelprovider",
			allowed:   []string{"runtime"},
			forbidden: []string{"core", "provider/openai", "net/http"},
		},
		{
			name:      "model protocol registration is runtime-only and transport-free",
			from:      "adapter/modelprotocol",
			allowed:   []string{"runtime"},
			forbidden: []string{"core", "provider/openai", "net/http"},
		},
		{
			name:    "legacy core plugin compatibility facade may depend only on core and runtime",
			from:    "adapter/coreplugin",
			allowed: []string{"core", "runtime"},
		},
	}

	for _, rule := range rules {
		for _, pkg := range packages {
			if !inSubtree(pkg.ImportPath, rule.from) {
				continue
			}
			if containsFamily(rule.except, pkg.ImportPath) {
				continue
			}
			for _, imported := range pkg.Imports {
				for _, forbidden := range rule.forbidden {
					if matchesImport(imported, forbidden) {
						t.Errorf("%s: %s imports forbidden package %s", rule.name, pkg.ImportPath, imported)
					}
				}
				if !isInternal(imported) {
					continue
				}
				if containsFamily(rule.allowed, imported) || containsExactImport(rule.allowedExact, imported) {
					continue
				}
				allowed := append(append([]string{}, rule.allowed...), rule.allowedExact...)
				t.Errorf("%s: %s imports disallowed internal package %s (allowed baseline: %s)", rule.name, pkg.ImportPath, imported, strings.Join(allowed, ", "))
			}
		}
	}
}

type dependencyRule struct {
	name         string
	from         string
	allowed      []string
	allowedExact []string
	except       []string
	forbidden    []string
}

type packageImports struct {
	ImportPath string
	Imports    []string
}

func loadPackageImports(t *testing.T) []packageImports {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate module check source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))

	cmd := exec.Command("go", "list", "-json", "./pkg/...")
	cmd.Dir = root
	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Fatalf("go list ./pkg/... failed: %v\n%s", err, exitErr.Stderr)
		}
		t.Fatalf("go list ./pkg/... failed: %v", err)
	}

	decoder := json.NewDecoder(strings.NewReader(string(output)))
	var packages []packageImports
	for {
		var pkg packageImports
		err := decoder.Decode(&pkg)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode go list output: %v", err)
		}
		if isInternal(pkg.ImportPath) {
			packages = append(packages, pkg)
		}
	}
	if len(packages) == 0 {
		t.Fatal("go list returned no internal packages")
	}
	return packages
}

func isStandardLibrary(importPath string) bool {
	first := importPath
	if slash := strings.IndexByte(first, '/'); slash >= 0 {
		first = first[:slash]
	}
	return !strings.Contains(first, ".")
}

func isInternal(importPath string) bool {
	return importPath == modulePath || strings.HasPrefix(importPath, modulePath+"/")
}

func familyPath(family string) string {
	return modulePath + "/pkg/" + family
}

func inFamily(importPath, family string) bool {
	prefix := familyPath(family)
	return importPath == prefix || strings.HasPrefix(importPath, prefix+"/")
}

func inSubtree(importPath, subtree string) bool {
	prefix := familyPath(subtree)
	return importPath == prefix || strings.HasPrefix(importPath, prefix+"/")
}

func containsFamily(families []string, importPath string) bool {
	for _, family := range families {
		if inSubtree(importPath, family) {
			return true
		}
	}
	return false
}

// containsExactImport is intentionally an equality test. It records a single
// approved private implementation edge without silently allowing a whole
// internal subtree or an arbitrary future internal package.
func containsExactImport(imports []string, importPath string) bool {
	for _, allowed := range imports {
		if importPath == allowed {
			return true
		}
	}
	return false
}

func matchesImport(importPath, family string) bool {
	if strings.Contains(family, "/") && !strings.HasPrefix(family, modulePath+"/") {
		return importPath == family || strings.HasPrefix(importPath, family+"/")
	}
	return inFamily(importPath, family)
}

type legacyImportError struct{ path string }

func (e *legacyImportError) Error() string { return "legacy module import in " + e.path }
