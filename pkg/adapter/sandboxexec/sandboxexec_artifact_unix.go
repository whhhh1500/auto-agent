//go:build unix

package sandboxexec

import (
	"os"
	"syscall"
)

// artifactPlatformPathSafe rejects hard-linked regular artifacts on platforms
// whose stable file metadata exposes the link count. Directories normally have
// more than one link, while symlinks are rejected by the generic scanner.
func artifactPlatformPathSafe(_ string, info os.FileInfo) bool {
	if info.IsDir() {
		return true
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1
}
