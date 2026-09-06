package sandbox

import (
	"errors"
	"path/filepath"
	"strings"
)

var errInvalidLocalWorkRoot = errors.New("invalid local sandbox work root")

// validateLocalWorkRoot performs only portable lexical validation. Platform
// factories may add filesystem- and syntax-specific checks before retaining
// the configured root. It intentionally does not resolve symlinks: the setup
// adapter owns that privileged verification and the root may be created just
// before registration.
func validateLocalWorkRoot(absWorkRoot string) error {
	if absWorkRoot == "" || strings.IndexByte(absWorkRoot, 0) >= 0 || !filepath.IsAbs(absWorkRoot) {
		return errInvalidLocalWorkRoot
	}
	clean := filepath.Clean(absWorkRoot)
	if clean != absWorkRoot || filepath.Dir(clean) == clean {
		return errInvalidLocalWorkRoot
	}
	return nil
}
