package main

import (
	"errors"
	"path/filepath"

	executionsandbox "github.com/cc-auto-agent/harness-core/pkg/execution/sandbox"
)

var errInvalidSandboxDataRoot = errors.New("invalid sandbox data root")

// sandboxWorkRootForDataRoot derives the one local-sandbox work root from the
// server's canonical data root. Keeping this derivation here means callers
// cannot accidentally pass the data root itself where the provider contract
// requires its sandbox child.
func sandboxWorkRootForDataRoot(dataRoot string) (string, error) {
	if dataRoot == "" || !filepath.IsAbs(dataRoot) || filepath.Clean(dataRoot) != dataRoot {
		return "", errInvalidSandboxDataRoot
	}
	return filepath.Join(dataRoot, "sandbox"), nil
}

// newSandboxProviderRegistry composes discovery from the server's resolved
// data root. Registration is valid on every supported platform; local
// execution is still rejected by its live Probe when its backend is absent.
func newSandboxProviderRegistry(dataRoot string) (*executionsandbox.Registry, error) {
	sandboxRoot, err := sandboxWorkRootForDataRoot(dataRoot)
	if err != nil {
		return nil, err
	}
	registration, err := executionsandbox.NewLocalRegistrationForWorkRoot(sandboxRoot)
	if err != nil {
		return nil, err
	}
	return executionsandbox.NewRegistry(executionsandbox.MaxRegistrations, registration)
}
