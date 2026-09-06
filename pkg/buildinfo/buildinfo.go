// Package buildinfo exposes the version metadata injected into release builds.
package buildinfo

import "fmt"

// These values remain useful for local builds and are replaced with -ldflags
// by the release workflow.
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

// String returns a compact, log-safe build identifier.
func String() string {
	return fmt.Sprintf("%s (%s, %s)", Version, Commit, BuildDate)
}
