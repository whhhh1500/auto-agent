//go:build windows

package sandbox

import (
	"path/filepath"
	"strings"
)

func newLocalProviderForWorkRoot(absWorkRoot string) (LocalProvider, error) {
	if err := validateLocalWorkRoot(absWorkRoot); err != nil {
		return LocalProvider{}, err
	}
	volume := filepath.VolumeName(absWorkRoot)
	if len(volume) != 2 || volume[1] != ':' || strings.HasPrefix(absWorkRoot, `\\`) || strings.HasPrefix(absWorkRoot, `\\?\`) {
		return LocalProvider{}, errInvalidLocalWorkRoot
	}
	if strings.Contains(absWorkRoot[len(volume):], ":") {
		return LocalProvider{}, errInvalidLocalWorkRoot
	}
	dataRoot := filepath.Dir(absWorkRoot)
	if !windowsPathsEqual(filepath.Join(dataRoot, "sandbox"), absWorkRoot) {
		return LocalProvider{}, errInvalidLocalWorkRoot
	}
	backend, err := newWindowsCurrentUserBackend(absWorkRoot)
	if err != nil {
		return LocalProvider{}, errInvalidLocalWorkRoot
	}
	return LocalProvider{
		workRoot: absWorkRoot,
		backend:  backend,
	}, nil
}
