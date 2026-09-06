//go:build !unix && !windows

package sandboxexec

import "os"

// Platforms without a portable link-count API still reject symlinks in the
// generic scanner and enforce SameFile identity before and after upload.
func artifactPlatformPathSafe(_ string, _ os.FileInfo) bool { return true }
