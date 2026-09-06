//go:build !linux && !windows

package sandbox

func localProviderRevision() string { return "unavailable-v1" }
