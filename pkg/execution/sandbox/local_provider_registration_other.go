//go:build !linux && !windows

package sandbox

func newLocalProviderForWorkRoot(absWorkRoot string) (LocalProvider, error) {
	if err := validateLocalWorkRoot(absWorkRoot); err != nil {
		return LocalProvider{}, err
	}
	return LocalProvider{}, nil
}
