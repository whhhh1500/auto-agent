// Command verify-coverage enforces modest package-level coverage floors.
package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var minimums = map[string]float64{
	"pkg/core":                60,
	"pkg/evaluation":          60,
	"pkg/execution":           50,
	"pkg/server":              55,
	"pkg/storage":             70,
	"pkg/provider/openai":     40,
	"pkg/extensions/workflow": 50,
}

type coverage struct{ statements, covered int }

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: verify-coverage <coverage.out>")
		os.Exit(2)
	}
	actual, err := readProfile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	failed := false
	for pkg, minimum := range minimums {
		got, ok := actual[pkg]
		if !ok || got.statements == 0 {
			fmt.Fprintf(os.Stderr, "coverage missing package %s\n", pkg)
			failed = true
			continue
		}
		percent := 100 * float64(got.covered) / float64(got.statements)
		fmt.Printf("coverage %s: %.1f%% (minimum %.1f%%)\n", pkg, percent, minimum)
		if percent < minimum {
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}

func readProfile(path string) (map[string]coverage, error) {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("open coverage profile: %w", err)
	}
	defer file.Close()

	result := map[string]coverage{}
	scanner := bufio.NewScanner(file)
	first := true
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		if first {
			first = false
			if fields[0] != "mode:" {
				return nil, fmt.Errorf("invalid coverage profile header")
			}
			continue
		}
		if len(fields) != 3 {
			return nil, fmt.Errorf("invalid coverage profile record %q", scanner.Text())
		}
		location := strings.ReplaceAll(fields[0], "\\", "/")
		pkg := packagePath(location)
		if pkg == "" {
			continue
		}
		statements, err := strconv.Atoi(fields[1])
		if err != nil {
			return nil, fmt.Errorf("invalid statement count: %w", err)
		}
		count, err := strconv.Atoi(fields[2])
		if err != nil {
			return nil, fmt.Errorf("invalid execution count: %w", err)
		}
		item := result[pkg]
		item.statements += statements
		if count > 0 {
			item.covered += statements
		}
		result[pkg] = item
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read coverage profile: %w", err)
	}
	return result, nil
}

func packagePath(location string) string {
	file := strings.SplitN(location, ":", 2)[0]
	parts := strings.Split(file, "/")
	for i, part := range parts {
		if part == "pkg" || part == "cmd" {
			if i+1 >= len(parts) {
				return ""
			}
			return strings.Join(parts[i:len(parts)-1], "/")
		}
	}
	return ""
}
