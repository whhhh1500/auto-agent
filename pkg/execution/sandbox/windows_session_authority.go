//go:build windows

package sandbox

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/windows"
)

const windowsSessionPrefix = "session-"

// windowsSessionAuthority binds one adapter-supplied candidate layout to the
// configured provider root. It owns only retained directory handles and their
// identities; capability ACL installation, process creation, and Job lifecycle
// work remain in the direct current-user backend.
type windowsSessionAuthority struct {
	mu     sync.Mutex
	closed bool

	workRoot  *windowsRetainedDirectory
	sessions  *windowsRetainedDirectory
	session   *windowsRetainedDirectory
	workspace *windowsRetainedDirectory
	artifacts *windowsRetainedDirectory
	home      *windowsRetainedDirectory
	temporary *windowsRetainedDirectory
	cache     *windowsRetainedDirectory
}

var windowsSessionAuthorityCloseDirectory = func(directory *windowsRetainedDirectory) error {
	return directory.close()
}

func newWindowsSessionAuthority(ctx context.Context, configuredWorkRoot, candidateSessionRoot, candidateWorkspace, candidateArtifacts string) (*windowsSessionAuthority, error) {
	if err := windowsSessionAuthorityContextError(ctx); err != nil {
		return nil, err
	}
	workRoot, sessionRoot, workspace, artifacts, err := validateWindowsSessionAuthorityPaths(configuredWorkRoot, candidateSessionRoot, candidateWorkspace, candidateArtifacts)
	if err != nil {
		return nil, err
	}

	authority := &windowsSessionAuthority{}
	if authority.workRoot, err = openWindowsRetainedDirectory(workRoot); err != nil {
		return nil, ErrArtifactUnverified
	}
	if authority.sessions, err = openWindowsSessionAuthorityChild(authority.workRoot, "sessions"); err != nil {
		_ = authority.closeDirectories(context.Background())
		return nil, ErrArtifactUnverified
	}
	if authority.session, err = openWindowsSessionAuthorityChild(authority.sessions, filepath.Base(sessionRoot)); err != nil {
		_ = authority.closeDirectories(context.Background())
		return nil, ErrArtifactUnverified
	}
	if authority.workspace, err = openWindowsSessionAuthorityChild(authority.session, "workspace"); err != nil {
		_ = authority.closeDirectories(context.Background())
		return nil, ErrArtifactUnverified
	}
	if authority.artifacts, err = openWindowsSessionAuthorityChild(authority.session, "artifacts"); err != nil {
		_ = authority.closeDirectories(context.Background())
		return nil, ErrArtifactUnverified
	}
	if authority.home, err = openWindowsSessionAuthorityChild(authority.session, "home"); err != nil {
		_ = authority.closeDirectories(context.Background())
		return nil, ErrArtifactUnverified
	}
	if authority.temporary, err = openWindowsSessionAuthorityChild(authority.session, "tmp"); err != nil {
		_ = authority.closeDirectories(context.Background())
		return nil, ErrArtifactUnverified
	}
	if authority.cache, err = openWindowsSessionAuthorityChild(authority.session, "cache"); err != nil {
		_ = authority.closeDirectories(context.Background())
		return nil, ErrArtifactUnverified
	}
	if !windowsAuthorityPathsMatch(authority, workspace, artifacts) || !windowsAuthorityIdentitiesDistinct(authority) {
		_ = authority.closeDirectories(context.Background())
		return nil, ErrArtifactUnverified
	}
	if err := authority.Verify(ctx); err != nil {
		_ = authority.Close(context.Background())
		return nil, err
	}
	return authority, nil
}

func validateWindowsSessionAuthorityPaths(configuredWorkRoot, candidateSessionRoot, candidateWorkspace, candidateArtifacts string) (workRoot, sessionRoot, workspace, artifacts string, err error) {
	workRoot, err = cleanWindowsSessionAuthorityPath(configuredWorkRoot)
	if err != nil {
		return "", "", "", "", ErrInvalidSpec
	}
	sessionRoot, err = cleanWindowsSessionAuthorityPath(candidateSessionRoot)
	if err != nil {
		return "", "", "", "", ErrInvalidSpec
	}
	workspace, err = cleanWindowsSessionAuthorityPath(candidateWorkspace)
	if err != nil {
		return "", "", "", "", ErrInvalidSpec
	}
	artifacts, err = cleanWindowsSessionAuthorityPath(candidateArtifacts)
	if err != nil {
		return "", "", "", "", ErrInvalidSpec
	}

	sessionName := filepath.Base(sessionRoot)
	expectedSessionRoot := filepath.Join(workRoot, "sessions", sessionName)
	if !validWindowsSessionName(sessionName) || !windowsPathsEqual(sessionRoot, expectedSessionRoot) || !windowsPathsEqual(workspace, filepath.Join(sessionRoot, "workspace")) || !windowsPathsEqual(artifacts, filepath.Join(sessionRoot, "artifacts")) {
		return "", "", "", "", ErrInvalidSpec
	}
	return workRoot, sessionRoot, workspace, artifacts, nil
}

func cleanWindowsSessionAuthorityPath(path string) (string, error) {
	clean, err := cleanWindowsAbsoluteDirectory(path)
	if err != nil || clean != path {
		return "", ErrInvalidSpec
	}
	return clean, nil
}

func validWindowsSessionName(name string) bool {
	if len(name) != len(windowsSessionPrefix)+32 || !strings.HasPrefix(name, windowsSessionPrefix) {
		return false
	}
	for _, value := range name[len(windowsSessionPrefix):] {
		if (value < '0' || value > '9') && (value < 'a' || value > 'f') {
			return false
		}
	}
	return true
}

func openWindowsSessionAuthorityChild(parent *windowsRetainedDirectory, name string) (*windowsRetainedDirectory, error) {
	if parent == nil || parent.file == nil || !validWindowsArtifactSegment(name) {
		return nil, ErrArtifactUnverified
	}
	handle, identity, err := openWindowsArtifactChild(windows.Handle(parent.file.Fd()), name)
	if err != nil || identity.validateDirectory() != nil {
		if err == nil {
			_ = windows.CloseHandle(handle)
		}
		return nil, ErrArtifactUnverified
	}
	file := os.NewFile(uintptr(handle), "sandbox-windows-session-authority")
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, ErrArtifactUnverified
	}
	return &windowsRetainedDirectory{path: filepath.Join(parent.path, name), file: file, identity: identity}, nil
}

func windowsAuthorityPathsMatch(authority *windowsSessionAuthority, workspace, artifacts string) bool {
	return authority != nil && authority.workspace != nil && authority.artifacts != nil && windowsPathsEqual(authority.workspace.path, workspace) && windowsPathsEqual(authority.artifacts.path, artifacts)
}

func windowsAuthorityIdentitiesDistinct(authority *windowsSessionAuthority) bool {
	if authority == nil {
		return false
	}
	directories := []*windowsRetainedDirectory{authority.workRoot, authority.sessions, authority.session, authority.workspace, authority.artifacts, authority.home, authority.temporary, authority.cache}
	for index, directory := range directories {
		if directory == nil || directory.file == nil || directory.identity.validateDirectory() != nil {
			return false
		}
		for _, other := range directories[index+1:] {
			if other == nil || directory.identity.sameObject(other.identity) {
				return false
			}
		}
	}
	return true
}

// Verify reopens every retained path through the reparse-safe handle primitive
// and compares its handle identity. A path replacement is never accepted.
func (authority *windowsSessionAuthority) Verify(ctx context.Context) error {
	if err := windowsSessionAuthorityContextError(ctx); err != nil {
		return err
	}
	if authority == nil {
		return ErrArtifactUnverified
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.closed || !windowsAuthorityIdentitiesDistinct(authority) {
		return ErrArtifactUnverified
	}
	for _, directory := range []*windowsRetainedDirectory{authority.workRoot, authority.sessions, authority.session, authority.workspace, authority.artifacts, authority.home, authority.temporary, authority.cache} {
		if err := windowsSessionAuthorityContextError(ctx); err != nil {
			return err
		}
		if err := directory.verifyPath(); err != nil {
			return ErrArtifactUnverified
		}
	}
	return nil
}

// Close releases only authority-owned handles. A handle is cleared only after
// its close succeeds; cancellation or a close failure leaves the remaining
// authority state available for a later serialized retry.
func (authority *windowsSessionAuthority) Close(ctx context.Context) error {
	if err := windowsSessionAuthorityContextError(ctx); err != nil {
		return err
	}
	if authority == nil {
		return ErrArtifactUnverified
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.closed {
		return nil
	}
	closeErr := authority.closeDirectories(ctx)
	if closeErr != nil {
		if errors.Is(closeErr, context.Canceled) || errors.Is(closeErr, context.DeadlineExceeded) {
			return closeErr
		}
		return ErrArtifactUnverified
	}
	authority.closed = true
	return nil
}

func (authority *windowsSessionAuthority) closeDirectories(ctx context.Context) error {
	if authority == nil {
		return ErrArtifactUnverified
	}
	directories := []**windowsRetainedDirectory{
		&authority.cache,
		&authority.temporary,
		&authority.home,
		&authority.artifacts,
		&authority.workspace,
		&authority.session,
		&authority.sessions,
		&authority.workRoot,
	}
	var results []error
	for _, directory := range directories {
		if err := windowsSessionAuthorityContextError(ctx); err != nil {
			return err
		}
		if *directory == nil {
			continue
		}
		if err := windowsSessionAuthorityCloseDirectory(*directory); err != nil {
			results = append(results, err)
			continue
		}
		*directory = nil
	}
	return errors.Join(results...)
}

func windowsSessionAuthorityContextError(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	return ctx.Err()
}
