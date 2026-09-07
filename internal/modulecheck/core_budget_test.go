package modulecheck

import (
	"bufio"
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// These limits are deliberately local to the architecture/module checks. They
// are not a compatibility promise and do not replace API review. New domain
// functionality belongs in runtime, extensions, or adapters. This refactor
// accepted Graph/context/composition/schema/performance seams at the stable
// boundary; future growth requires architecture review and should migrate the
// owning concern out of core rather than silently expanding this budget.
const (
	coreBudgetBaselineDate = "2026-09-04"

	// Historical baseline measured from pkg/core production files on 2026-09-04.
	// Lines mean non-blank physical source lines; comments and generated files are
	// still counted/excluded respectively, while blank formatting is not.
	coreProductionLineBaseline = 8615
	coreProductionFileBaseline = 34
	// The HEAD public surface had consumed all three compatibility slots above
	// the 902 baseline. This reviewed optional reader adds one interface and one
	// method, the usage ledger adds one durable identity field, the reviewed
	// post-result continuation adds one Runtime method, and the reviewed
	// ReplaceExact exception adds one registry method. The accepted surface is
	// 910 with no unused public API slot.
	corePublicAPIBaseline = 910
	corePublicAPILimit    = 910
	// The historical hard limit was 8743. The 2026-09-07 ReplaceExact
	// architecture exception requires a core-owned same-pointer publication
	// primitive: one lock preserves layer order and old unmount closures while a
	// detached candidate validates the final projection. It cannot live in
	// control or server. Its accepted implementation measures 8817 lines; 8821
	// retains four fixed lines only. This preserves the 8615 historical baseline
	// and is not a rolling "current baseline + 128" budget.
	coreProductionLineHardLimit = 8821
	coreProductionFileHardLimit = 36
	// A focused split is required before one core source file reaches this size.
	coreProductionSingleFileLineHardLimit = 1500
)

type corePublicAPICount struct {
	TopLevelNames    int
	ExportedMethods  int
	ExportedFields   int
	InterfaceMethods int
}

func (count corePublicAPICount) Total() int {
	return count.TopLevelNames + count.ExportedMethods + count.ExportedFields + count.InterfaceMethods
}

// TestCorePublicAPIAndSizeBudget freezes a small, deterministic budget for the
// stable pkg/core foundation. The AST rule counts exported top-level names,
// exported methods on exported receiver types, named exported struct fields on
// exported types, and named exported interface methods. Embedded unnamed
// fields/methods are intentionally excluded so the rule remains mechanical.
func TestCorePublicAPIAndSizeBudget(t *testing.T) {
	root := moduleRootFromCaller(t)
	coreDir := filepath.Join(root, "pkg", "core")
	files, err := productionCoreFiles(coreDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) > coreProductionFileHardLimit {
		t.Fatalf("pkg/core production Go files=%d exceeds hard limit=%d (baseline %d on %s)", len(files), coreProductionFileHardLimit, coreProductionFileBaseline, coreBudgetBaselineDate)
	}
	lines, err := productionCoreLines(files)
	if err != nil {
		t.Fatal(err)
	}
	if lines > coreProductionLineHardLimit {
		t.Fatalf("pkg/core production Go non-blank physical lines=%d exceeds hard limit=%d (baseline %d on %s)", lines, coreProductionLineHardLimit, coreProductionLineBaseline, coreBudgetBaselineDate)
	}
	for _, path := range files {
		fileLines, err := productionCoreFileLines(path)
		if err != nil {
			t.Fatal(err)
		}
		if fileLines > coreProductionSingleFileLineHardLimit {
			t.Fatalf("pkg/core file %s has %d non-blank physical lines, exceeding hard limit=%d; split the owning concern out of core", filepath.Base(path), fileLines, coreProductionSingleFileLineHardLimit)
		}
	}

	api, err := countCorePublicAPI(files)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("pkg/core budget baseline=%s files=%d lines=%d public_api=%d (top-level=%d methods=%d fields=%d interface_methods=%d)", coreBudgetBaselineDate, len(files), lines, api.Total(), api.TopLevelNames, api.ExportedMethods, api.ExportedFields, api.InterfaceMethods)
	if api.Total() > corePublicAPILimit {
		t.Fatalf("pkg/core exported API=%d exceeds budget=%d (baseline %d on %s; top-level=%d methods=%d fields=%d interface_methods=%d)", api.Total(), corePublicAPILimit, corePublicAPIBaseline, coreBudgetBaselineDate, api.TopLevelNames, api.ExportedMethods, api.ExportedFields, api.InterfaceMethods)
	}
}

func moduleRootFromCaller(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate modulecheck source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}

func productionCoreFiles(coreDir string) ([]string, error) {
	entries, err := os.ReadDir(coreDir)
	if err != nil {
		return nil, fmt.Errorf("read pkg/core: %w", err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(coreDir, entry.Name())
		contents, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		generated, err := isGeneratedGo(path, contents)
		if err != nil {
			return nil, fmt.Errorf("inspect generated marker in %s: %w", path, err)
		}
		if generated {
			continue
		}
		files = append(files, path)
	}
	sort.Strings(files)
	return files, nil
}

func isGeneratedGo(path string, contents []byte) (bool, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, contents, parser.ParseComments)
	if err != nil {
		return false, err
	}
	return ast.IsGenerated(file), nil
}

func TestProductionCoreFilesUsesCanonicalGeneratedMarker(t *testing.T) {
	coreDir := t.TempDir()
	files := map[string]string{
		"handwritten_generated.go": "package core\n\nfunc HandwrittenGenerated() {}\n",
		"canonical.go":             "// Code generated by modulecheck; DO NOT EDIT.\n\npackage core\n",
		"after_package.go":         "package core\n\n// Code generated by modulecheck; DO NOT EDIT.\nfunc AfterPackage() {}\n",
		"near_marker.go":           "// Code generated by modulecheck; DO NOT EDIT\n\npackage core\n",
		"literal_marker.go":        "package core\n\nconst marker = `// Code generated by modulecheck; DO NOT EDIT.`\n",
	}
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(coreDir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	paths, err := productionCoreFiles(coreDir)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(paths))
	for _, path := range paths {
		got = append(got, filepath.Base(path))
	}
	want := []string{"after_package.go", "handwritten_generated.go", "literal_marker.go", "near_marker.go"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("production files=%v, want %v", got, want)
	}
}

func TestProductionCoreFilesRejectsInvalidSyntax(t *testing.T) {
	coreDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(coreDir, "invalid.go"), []byte("package core\nfunc broken(\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := productionCoreFiles(coreDir); err == nil {
		t.Fatal("productionCoreFiles accepted invalid Go syntax")
	}
}

func productionCoreLines(files []string) (int, error) {
	total := 0
	for _, path := range files {
		lines, err := productionCoreFileLines(path)
		if err != nil {
			return 0, err
		}
		total += lines
	}
	return total, nil
}

func productionCoreFileLines(path string) (int, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", path, err)
	}
	lines := 0
	scanner := bufio.NewScanner(bytes.NewReader(contents))
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) != "" {
			lines++
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("count lines in %s: %w", path, err)
	}
	return lines, nil
}

func countCorePublicAPI(files []string) (corePublicAPICount, error) {
	var count corePublicAPICount
	for _, path := range files {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return corePublicAPICount{}, fmt.Errorf("parse %s: %w", path, err)
		}
		for _, declaration := range file.Decls {
			switch declaration := declaration.(type) {
			case *ast.GenDecl:
				for _, specification := range declaration.Specs {
					switch specification := specification.(type) {
					case *ast.TypeSpec:
						if !ast.IsExported(specification.Name.Name) {
							continue
						}
						count.TopLevelNames++
						countTypeSurface(specification.Type, &count)
					case *ast.ValueSpec:
						for _, name := range specification.Names {
							if ast.IsExported(name.Name) {
								count.TopLevelNames++
							}
						}
					}
				}
			case *ast.FuncDecl:
				if !ast.IsExported(declaration.Name.Name) {
					continue
				}
				if declaration.Recv == nil {
					count.TopLevelNames++
					continue
				}
				if receiverTypeExported(declaration.Recv) {
					count.ExportedMethods++
				}
			}
		}
	}
	return count, nil
}

func receiverTypeExported(receiver *ast.FieldList) bool {
	if receiver == nil || len(receiver.List) == 0 {
		return false
	}
	typeExpr := receiver.List[0].Type
	if pointer, ok := typeExpr.(*ast.StarExpr); ok {
		typeExpr = pointer.X
	}
	if index, ok := typeExpr.(*ast.IndexExpr); ok {
		typeExpr = index.X
	}
	if index, ok := typeExpr.(*ast.IndexListExpr); ok {
		typeExpr = index.X
	}
	identifier, ok := typeExpr.(*ast.Ident)
	return ok && ast.IsExported(identifier.Name)
}

func countTypeSurface(expression ast.Expr, count *corePublicAPICount) {
	switch expression := expression.(type) {
	case *ast.StructType:
		for _, field := range expression.Fields.List {
			for _, name := range field.Names {
				if ast.IsExported(name.Name) {
					count.ExportedFields++
				}
			}
			countTypeSurface(field.Type, count)
		}
	case *ast.InterfaceType:
		for _, field := range expression.Methods.List {
			for _, name := range field.Names {
				if ast.IsExported(name.Name) {
					count.InterfaceMethods++
				}
			}
			countTypeSurface(field.Type, count)
		}
	case *ast.ArrayType:
		countTypeSurface(expression.Elt, count)
	case *ast.MapType:
		countTypeSurface(expression.Key, count)
		countTypeSurface(expression.Value, count)
	case *ast.ChanType:
		countTypeSurface(expression.Value, count)
	case *ast.Ellipsis:
		countTypeSurface(expression.Elt, count)
	case *ast.ParenExpr:
		countTypeSurface(expression.X, count)
	case *ast.FuncType:
		// Function parameters and results are not exported fields or methods.
		return
	}
}
