//go:build linux

package sandbox

func newLocalProviderForWorkRoot(absWorkRoot string) (LocalProvider, error) {
	if err := validateLocalWorkRoot(absWorkRoot); err != nil {
		return LocalProvider{}, err
	}
	// Linux's established bwrap provider derives each session root from the
	// SessionSpec. Keep that behavior intact while making server composition
	// validate its configured root consistently with Windows.
	return LocalProvider{}, nil
}
