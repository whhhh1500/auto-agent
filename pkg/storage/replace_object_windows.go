//go:build windows

package storage

import "golang.org/x/sys/windows"

// replaceObjectFile uses the Windows replace-existing and write-through flags.
// MoveFileEx does not remove the old file before attempting the replacement;
// on failure the caller's existing object remains intact and cleans the temp.
func replaceObjectFile(source, target string) error {
	from, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}
