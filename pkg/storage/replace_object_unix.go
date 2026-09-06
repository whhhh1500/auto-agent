//go:build !windows

package storage

import "os"

// replaceObjectFile is atomic on the same filesystem. os.Rename replaces an
// existing regular file without deleting it first, so a failed rename leaves
// the previous object intact.
func replaceObjectFile(source, target string) error {
	return os.Rename(source, target)
}
