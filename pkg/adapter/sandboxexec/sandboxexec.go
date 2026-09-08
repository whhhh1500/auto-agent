// Package sandboxexec exposes the provider-neutral sandbox.exec capability.
// It deliberately owns the session lifecycle: callers supply a trusted
// sandbox provider and an already validated, server-owned SessionSpec.
package sandboxexec

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/execution/sandbox"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

var (
	ErrInvalidRequest = errors.New("sandbox.exec request is invalid")
	ErrUnavailable    = errors.New("sandbox.exec is unavailable")
)

type artifactRef struct {
	Key    string `json:"key"`
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

// Capability is an execution provider for the fixed sandbox.exec contract.
// The provider and spec must be composed by a trusted server boundary; no
// caller-provided mounts, working directory, or shell string are accepted.
type Config struct {
	Registry   *sandbox.Registry
	ProviderID sandbox.ProviderID
	Version    string
	// Network is a trusted server-selected network policy. An empty value
	// preserves the historical disabled-network default.
	Network            sandbox.NetworkPolicy
	RequestedIsolation bool
	Limits             sandbox.Limits
	ArtifactPolicy     sandbox.ArtifactPolicy
	WorkRoot           string
	ObjectStore        storage.ObjectStore
}

type Capability struct{ config Config }

func New(config Config) (*Capability, error) {
	if config.Registry == nil || config.WorkRoot == "" || config.ObjectStore == nil {
		return nil, ErrUnavailable
	}
	if _, err := config.Registry.Resolve(config.ProviderID, config.Version); err != nil {
		return nil, ErrUnavailable
	}
	if config.Network == "" {
		config.Network = sandbox.NetworkDisabled
		config.RequestedIsolation = true
	}
	if config.Network != sandbox.NetworkHost && config.Network != sandbox.NetworkDisabled && config.Network != sandbox.NetworkIsolated {
		return nil, ErrInvalidRequest
	}
	if config.RequestedIsolation != (config.Network == sandbox.NetworkDisabled || config.Network == sandbox.NetworkIsolated) {
		return nil, ErrInvalidRequest
	}
	if config.Limits.WallTime <= 0 || config.Limits.WallTime > sandbox.DefaultExecWallTime || config.Limits.MaxCPUTime <= 0 || config.Limits.MaxCPUTime > sandbox.DefaultExecWallTime || config.Limits.MaxMemoryBytes <= 0 || config.Limits.MaxMemoryBytes > sandbox.DefaultExecMemoryBytes || config.Limits.MaxOutputBytes <= 0 || config.Limits.MaxOutputBytes > sandbox.DefaultExecCombinedBytes || config.ArtifactPolicy.MaxArtifacts <= 0 || config.ArtifactPolicy.MaxArtifacts > sandbox.DefaultExecArtifactCount || config.ArtifactPolicy.MaxTotalBytes <= 0 || config.ArtifactPolicy.MaxTotalBytes > sandbox.DefaultExecArtifactBytes {
		return nil, ErrInvalidRequest
	}
	if !filepath.IsAbs(config.WorkRoot) {
		return nil, ErrInvalidRequest
	}
	config.WorkRoot = filepath.Clean(config.WorkRoot)
	info, err := os.Lstat(config.WorkRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || filepath.Dir(config.WorkRoot) == config.WorkRoot {
		return nil, ErrInvalidRequest
	}
	resolved, err := filepath.EvalSymlinks(config.WorkRoot)
	if err != nil || filepath.Clean(resolved) != config.WorkRoot {
		return nil, ErrInvalidRequest
	}
	// No component of the server-owned root may be a symlink. This prevents
	// changing the destination after composition has validated the root.
	cur := filepath.VolumeName(config.WorkRoot) + string(filepath.Separator)
	for _, part := range strings.Split(strings.TrimPrefix(config.WorkRoot, cur), string(filepath.Separator)) {
		if part == "" {
			continue
		}
		cur = filepath.Join(cur, part)
		st, statErr := os.Lstat(cur)
		if statErr != nil || st.Mode()&os.ModeSymlink != 0 {
			return nil, ErrInvalidRequest
		}
	}
	return &Capability{config: config}, nil
}

func (c *Capability) Manifest() core.CapabilityManifest {
	return core.CapabilityManifest{
		ID: "sandbox.exec", Version: "1", Name: "Sandbox Exec",
		Description: "Execute an argv vector in a server-managed local sandbox.",
		Kind:        core.KindTool, Contract: "sandbox.exec/v1", Idempotent: false,
		RequiredPermissions: []core.Permission{core.PermWrite},
		MaxOutputBytes:      int(sandbox.DefaultExecCombinedBytes), TimeoutMs: int(sandbox.DefaultExecWallTime / time.Millisecond),
		Tool: &core.ToolExposure{Description: "Execute argv without a shell.", Parameters: map[string]any{
			"type": "object", "required": []any{"argv"}, "additionalProperties": false,
			"properties": map[string]any{"argv": map[string]any{"type": "array", "minItems": 1, "maxItems": sandbox.MaxArgs, "items": map[string]any{"type": "string", "minLength": 1, "maxLength": sandbox.MaxArgBytes}}},
		}},
		Execution: &core.ExecutionSpec{Runtime: "sandbox", Sandbox: core.SandboxPolicy{Mode: core.SandboxWorkspaceWrite}, Writes: true},
	}
}

func (c *Capability) Execute(ctx context.Context, request core.CapabilityRequest) (out core.CapabilityResult, err error) {
	if ctx == nil {
		return core.CapabilityResult{}, context.Canceled
	}
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	if c == nil || c.config.Registry == nil {
		return core.CapabilityResult{}, ErrUnavailable
	}
	argv, err := decodeArgv(request.Args)
	if err != nil {
		return core.CapabilityResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return core.CapabilityResult{}, err
	}
	tenant := request.Context.Principal.TenantID
	sessionsRoot := filepath.Join(c.config.WorkRoot, "sessions")
	if err := ensureSessionsRoot(sessionsRoot); err != nil {
		return core.CapabilityResult{}, mapProviderError(ctx, err)
	}
	sessionRoot, workspace, artifacts, sessionRootInfo, err := createSessionRoot(sessionsRoot)
	if err != nil {
		return core.CapabilityResult{}, mapProviderError(ctx, err)
	}
	cleanup := func() error {
		return removeSessionRoot(sessionRoot, sessionsRoot, sessionRootInfo)
	}
	var session sandbox.Session
	var finalizer sandbox.PublicationFinalizer
	sessionStarted := false
	startOwnsRoot := false
	finalizationSucceeded := false
	terminalOutcome := sandbox.PublicationFailed
	defer func() {
		if !sessionStarted {
			// A RootOwnershipStarter reports true only when this Start invocation
			// durably adopted the root or cleanup is uncertain. That provider is
			// then the only component allowed to recover or remove it.
			if startOwnsRoot {
				return
			}
			if cleanupErr := cleanup(); cleanupErr != nil {
				out = core.CapabilityResult{}
				err = mapProviderError(ctx, cleanupErr)
			}
			return
		}

		if finalizer != nil && !finalizationSucceeded {
			if finalizeErr := finalizeSession(finalizer, terminalOutcome); finalizeErr != nil {
				if err == nil {
					out = core.CapabilityResult{}
					err = mapProviderError(ctx, finalizeErr)
				}
			} else {
				finalizationSucceeded = true
			}
		}
		closeErr := closeSession(session)
		if closeErr != nil {
			// A failed Close cannot prove process termination or ownership-safe
			// cleanup. Preserve the root as evidence for both legacy and aware
			// providers.
			if err == nil {
				out = core.CapabilityResult{}
				err = mapProviderError(ctx, closeErr)
			}
			return
		}
		if finalizer != nil || startOwnsRoot {
			// A publication-aware provider, or a provider that reported dynamic
			// root adoption for this invocation, owns the root from Start onward.
			// The adapter must never RemoveAll an orphan it cannot prove it owns.
			return
		}
		if cleanupErr := cleanup(); cleanupErr != nil {
			out = core.CapabilityResult{}
			err = mapProviderError(ctx, cleanupErr)
		}
	}()
	if err := os.Chmod(workspace, 0o700); err != nil {
		return core.CapabilityResult{}, mapProviderError(ctx, err)
	}
	if err := os.Chmod(artifacts, 0o700); err != nil {
		return core.CapabilityResult{}, mapProviderError(ctx, err)
	}
	token, err := randomToken()
	if err != nil {
		return core.CapabilityResult{}, mapProviderError(ctx, err)
	}
	composition := request.Context.CompositionRevision
	if composition == "" {
		return core.CapabilityResult{}, ErrInvalidRequest
	}
	spec := sandbox.SessionSpec{RequestedAssurance: sandbox.Assurance{Level: sandbox.AssuranceProcess, SharedKernel: true, NetworkIsolation: c.config.RequestedIsolation}, Limits: c.config.Limits, Network: c.config.Network, ArtifactPolicy: c.config.ArtifactPolicy}
	spec.Mounts = []sandbox.Mount{{Source: workspace, Target: "/workspace"}, {Source: artifacts, Target: "/artifacts"}}
	spec.Lease = sandbox.LeaseIdentity{RunID: request.Context.Invocation.RunID, SegmentID: request.Context.Invocation.CallID, ModuleID: request.Context.CapabilityID, CompositionRev: composition, Token: token}
	if tenant == "" {
		return core.CapabilityResult{}, ErrInvalidRequest
	}
	session, startOwnsRoot, err = c.config.Registry.StartWithOwnership(ctx, c.config.ProviderID, c.config.Version, spec)
	if err != nil {
		return core.CapabilityResult{}, mapProviderError(ctx, err)
	}
	sessionStarted = true
	finalizer, _ = session.(sandbox.PublicationFinalizer)
	execSession, ok := session.(sandbox.ExecSession)
	if !ok {
		return core.CapabilityResult{}, mapProviderError(ctx, ErrUnavailable)
	}
	execRequest := sandbox.ExecRequest{Args: argv, Limits: spec.Limits, Network: spec.Network, ArtifactPolicy: spec.ArtifactPolicy, Lease: spec.Lease}
	result, execErr := execSession.Exec(ctx, execRequest)
	if err := ctx.Err(); err != nil {
		return core.CapabilityResult{}, err
	}
	if execErr != nil && !errors.Is(execErr, sandbox.ErrExecFailed) && !errors.Is(execErr, sandbox.ErrExecTimeout) && !errors.Is(execErr, sandbox.ErrExecOutputLimit) {
		return core.CapabilityResult{}, mapProviderError(ctx, execErr)
	}
	if result.Artifacts.Validate(c.config.ArtifactPolicy) != nil {
		return core.CapabilityResult{}, mapProviderError(ctx, ErrUnavailable)
	}
	refs, err := publishArtifacts(ctx, c.config.ObjectStore, artifacts, tenant, request.Context.Invocation.RunID, request.Context.Invocation.CallID, result.Artifacts)
	if err != nil {
		return core.CapabilityResult{}, mapProviderError(ctx, err)
	}
	// Publication has committed even if the caller cancels immediately after
	// it. Its provider-visible terminal outcome must therefore remain published.
	terminalOutcome = sandbox.PublicationPublished
	if finalizer != nil {
		if finalizeErr := finalizeSession(finalizer, terminalOutcome); finalizeErr != nil {
			return core.CapabilityResult{}, mapProviderError(ctx, finalizeErr)
		}
		finalizationSucceeded = true
	}
	if err := ctx.Err(); err != nil {
		return core.CapabilityResult{}, err
	}
	contentPayload := struct {
		Stdout    outputPayload `json:"stdout"`
		Stderr    outputPayload `json:"stderr"`
		ExitCode  int           `json:"exit_code"`
		Signal    string        `json:"signal,omitempty"`
		TimedOut  bool          `json:"timed_out"`
		Code      string        `json:"code"`
		Artifacts []artifactRef `json:"artifacts"`
	}{
		Stdout: outputContent(result.Stdout), Stderr: outputContent(result.Stderr),
		ExitCode: result.ExitCode, Signal: result.Signal, TimedOut: result.TimedOut, Code: execCode(execErr), Artifacts: refs,
	}
	contentBytes, marshalErr := json.Marshal(contentPayload)
	if marshalErr != nil {
		return core.CapabilityResult{}, ErrUnavailable
	}
	return core.CapabilityResult{Content: string(contentBytes), OK: execErr == nil, Metadata: map[string]any{
		"exit_code": result.ExitCode, "stdout_size": result.Stdout.Size,
		"stderr_size": result.Stderr.Size, "timed_out": result.TimedOut,
		"stdout_digest": result.Stdout.Digest, "stderr_digest": result.Stderr.Digest,
		"code": execCode(execErr), "signal": result.Signal,
	}}, nil
}

func mapProviderError(ctx context.Context, _ error) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return ErrUnavailable
}

func createSessionRoot(sessionsRoot string) (sessionRoot, workspace, artifacts string, info os.FileInfo, err error) {
	stagingRoot, err := os.MkdirTemp(sessionsRoot, ".session-staging-")
	if err != nil {
		return "", "", "", nil, err
	}
	keepRoot := false
	defer func() {
		if !keepRoot {
			_ = os.RemoveAll(stagingRoot)
		}
	}()
	for _, name := range []string{"workspace", "artifacts", "home", "tmp", "cache"} {
		path := filepath.Join(stagingRoot, name)
		if err := os.Mkdir(path, 0o700); err != nil {
			return "", "", "", nil, err
		}
		if name == "workspace" {
			workspace = path
		}
		if name == "artifacts" {
			artifacts = path
		}
	}
	token, err := randomToken()
	if err != nil {
		return "", "", "", nil, err
	}
	sessionRoot = filepath.Join(sessionsRoot, "session-"+token)
	if err := os.Rename(stagingRoot, sessionRoot); err != nil {
		return "", "", "", nil, err
	}
	keepRoot = true
	workspace = filepath.Join(sessionRoot, "workspace")
	artifacts = filepath.Join(sessionRoot, "artifacts")
	info, err = os.Lstat(sessionRoot)
	if err != nil {
		return "", "", "", nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", "", "", nil, errors.New("sandbox session root is not a regular directory")
	}
	return sessionRoot, workspace, artifacts, info, nil
}

func ensureSessionsRoot(root string) error {
	info, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(root, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		info, err = os.Lstat(root)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("sandbox sessions root is not a regular directory")
	}
	return nil
}

func removeSessionRoot(root, sessionsRoot string, expected os.FileInfo) error {
	if filepath.Clean(filepath.Dir(root)) != filepath.Clean(sessionsRoot) {
		return errors.New("sandbox session root is outside the sessions directory")
	}
	info, err := os.Lstat(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || expected == nil || !os.SameFile(info, expected) {
		return errors.New("sandbox session root identity changed")
	}
	return os.RemoveAll(root)
}

type outputPayload struct {
	PreviewBase64 string `json:"preview_base64"`
	Size          int64  `json:"size"`
	Digest        string `json:"digest"`
	Truncated     bool   `json:"truncated"`
}

func outputContent(output sandbox.Output) outputPayload {
	return outputPayload{PreviewBase64: base64.StdEncoding.EncodeToString(output.Preview), Size: output.Size, Digest: output.Digest, Truncated: output.Truncated}
}

func closeSession(session sandbox.Session) error {
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return session.Close(closeCtx)
}

func finalizeSession(finalizer sandbox.PublicationFinalizer, outcome sandbox.PublicationOutcome) error {
	finalizeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return finalizer.FinalizePublication(finalizeCtx, outcome)
}

func publishArtifacts(ctx context.Context, store storage.ObjectStore, root, tenant, runID, callID string, verifiedSet sandbox.ArtifactSet) ([]artifactRef, error) {
	return publishArtifactsWithHooks(ctx, store, root, tenant, runID, callID, verifiedSet, artifactPublishHooks{})
}

// artifactPublishHooks makes race tests deterministic without exposing a
// provider or storage-facing seam. Production publication always passes the
// zero value.
type artifactPublishHooks struct {
	afterPreflightDigest func(path string)
	beforeUploadRead     func(path string)
	afterFirstRead       func(path string)
}

func publishArtifactsWithHooks(ctx context.Context, store storage.ObjectStore, root, tenant, runID, callID string, verifiedSet sandbox.ArtifactSet, hooks artifactPublishHooks) ([]artifactRef, error) {
	files, err := collectArtifactFiles(root, "")
	if err != nil {
		return nil, sandbox.ErrArtifactUnverified
	}
	verified, err := verifiedArtifactsByKey(files, verifiedSet)
	if err != nil {
		return nil, sandbox.ErrArtifactUnverified
	}
	refs := make([]artifactRef, 0, len(files))
	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, ref := range refs {
			_ = store.Delete(cleanupCtx, ref.Key)
		}
	}
	committed := false
	defer func() {
		if !committed {
			cleanup()
		}
	}()
	for _, fileInfo := range files {
		ref, err := publishVerifiedArtifact(ctx, store, fileInfo, verified[fileInfo.key], tenant, runID, callID, hooks)
		if ref.Key != "" {
			// A successful create-only write belongs to this call. Record it before
			// returning a post-upload verification failure so cleanup deletes it.
			refs = append(refs, ref)
		}
		if err != nil {
			return nil, err
		}
	}
	// A provider cannot smuggle an additional, deleted, or renamed artifact
	// between its validation and publication. Compare the entire key set again,
	// rather than merely the number of entries.
	finalFiles, err := collectArtifactFiles(root, "")
	if err != nil || !sameArtifactKeySet(files, finalFiles) {
		return nil, sandbox.ErrArtifactUnverified
	}
	committed = true
	return refs, nil
}

func verifiedArtifactsByKey(files []artifactFile, verifiedSet sandbox.ArtifactSet) (map[string]sandbox.Artifact, error) {
	if len(files) != len(verifiedSet.Items) {
		return nil, sandbox.ErrArtifactUnverified
	}
	artifacts := make(map[string]sandbox.Artifact, len(verifiedSet.Items))
	for _, artifact := range verifiedSet.Items {
		if !validArtifactKey(artifact.Key) || !artifact.Verified {
			return nil, sandbox.ErrArtifactUnverified
		}
		if _, exists := artifacts[artifact.Key]; exists {
			return nil, sandbox.ErrArtifactUnverified
		}
		artifacts[artifact.Key] = artifact
	}
	for _, file := range files {
		if _, exists := artifacts[file.key]; !exists {
			return nil, sandbox.ErrArtifactUnverified
		}
	}
	return artifacts, nil
}

func sameArtifactKeySet(before, after []artifactFile) bool {
	if len(before) != len(after) {
		return false
	}
	keys := make(map[string]struct{}, len(before))
	for _, file := range before {
		keys[file.key] = struct{}{}
	}
	for _, file := range after {
		if _, exists := keys[file.key]; !exists {
			return false
		}
	}
	return true
}

func publishVerifiedArtifact(ctx context.Context, store storage.ObjectStore, fileInfo artifactFile, verified sandbox.Artifact, tenant, runID, callID string, hooks artifactPublishHooks) (artifactRef, error) {
	if !validArtifactKey(fileInfo.key) || !sameArtifactPathSnapshot(fileInfo, fileInfo.path) {
		return artifactRef{}, sandbox.ErrArtifactUnverified
	}

	// The preflight digest binds the path to the provider's verified item. It is
	// deliberately not reused for the object reference: the upload reader below
	// records the bytes the object store actually consumed.
	preflight, err := os.Open(fileInfo.path)
	if err != nil {
		return artifactRef{}, sandbox.ErrArtifactUnverified
	}
	preflightInfo, statErr := preflight.Stat()
	if statErr != nil || !sameArtifactFileInfo(fileInfo.info, preflightInfo) {
		_ = preflight.Close()
		return artifactRef{}, sandbox.ErrArtifactUnverified
	}
	digest, size, digestErr := digestFile(preflight)
	postDigestInfo, postDigestStatErr := preflight.Stat()
	closeErr := preflight.Close()
	if digestErr != nil || postDigestStatErr != nil || closeErr != nil || !sameArtifactFileInfo(fileInfo.info, postDigestInfo) {
		return artifactRef{}, sandbox.ErrArtifactUnverified
	}
	if verified.Digest != digest || verified.Size != size {
		return artifactRef{}, sandbox.ErrArtifactUnverified
	}
	if hooks.afterPreflightDigest != nil {
		hooks.afterPreflightDigest(fileInfo.path)
	}

	file, err := os.Open(fileInfo.path)
	if err != nil {
		return artifactRef{}, sandbox.ErrArtifactUnverified
	}
	openedInfo, statErr := file.Stat()
	if statErr != nil || !sameArtifactFileInfo(fileInfo.info, openedInfo) {
		_ = file.Close()
		return artifactRef{}, sandbox.ErrArtifactUnverified
	}
	if hooks.beforeUploadRead != nil {
		hooks.beforeUploadRead(fileInfo.path)
	}

	reader := newArtifactUploadReader(ctx, file, sandbox.DefaultExecArtifactBytes, func() {
		if hooks.afterFirstRead != nil {
			hooks.afterFirstRead(fileInfo.path)
		}
	})
	key := "tenants/" + tenant + "/artifacts/sandbox/" + runID + "/" + callID + "/" + fileInfo.key
	var putErr error
	if putter, ok := store.(storage.StreamingObjectPutter); ok {
		_, putErr = putter.PutStream(ctx, key, reader, sandbox.DefaultExecArtifactBytes, storage.PutOptions{IfNoneMatchStar: true})
	} else {
		var data []byte
		data, putErr = io.ReadAll(reader)
		if putErr == nil {
			_, putErr = store.Put(ctx, key, data, storage.PutOptions{IfNoneMatchStar: true})
		}
	}
	streamInfo, streamStatErr := file.Stat()
	closeErr = file.Close()
	if putErr != nil {
		return artifactRef{}, putErr
	}

	// PutStream has completed successfully, so this key was created by this
	// call under If-None-Match: *. The caller adds it to its rollback set before
	// acting on a post-upload mismatch.
	ref := artifactRef{Key: key, Digest: reader.Digest(), Size: reader.Size()}
	if reader.Err() != nil || streamStatErr != nil || closeErr != nil || !sameArtifactFileInfo(fileInfo.info, streamInfo) || !sameArtifactPathSnapshot(fileInfo, fileInfo.path) || reader.Size() != verified.Size || reader.Digest() != verified.Digest {
		return ref, sandbox.ErrArtifactUnverified
	}
	return ref, nil
}

func validArtifactKey(key string) bool {
	if key == "" || !utf8.ValidString(key) || strings.HasPrefix(key, "/") || strings.Contains(key, "\\") {
		return false
	}
	for _, part := range strings.Split(key, "/") {
		if part == "" || part == "." || part == ".." || strings.TrimSpace(part) != part || strings.ContainsRune(part, 0) || strings.IndexFunc(part, func(r rune) bool { return unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Cs, unicode.Co) }) >= 0 {
			return false
		}
	}
	return true
}

type artifactFile struct {
	key  string
	path string
	info os.FileInfo
}

const (
	maxArtifactWalkDepth   = 32
	maxArtifactWalkEntries = 4096
)

func collectArtifactFiles(root, prefix string) ([]artifactFile, error) {
	rootInfo, err := os.Lstat(root)
	if err != nil || !safeArtifactFile(root, rootInfo) || !rootInfo.IsDir() {
		return nil, sandbox.ErrArtifactUnverified
	}
	return collectArtifactFilesBounded(root, prefix, 0, new(int))
}

func collectArtifactFilesBounded(root, prefix string, depth int, visited *int) ([]artifactFile, error) {
	if depth > maxArtifactWalkDepth {
		return nil, sandbox.ErrArtifactUnverified
	}
	entries, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(prefix)))
	if err != nil {
		return nil, err
	}
	files := make([]artifactFile, 0, len(entries))
	for _, entry := range entries {
		*visited++
		if *visited > maxArtifactWalkEntries {
			return nil, sandbox.ErrArtifactUnverified
		}
		key := entry.Name()
		if prefix != "" {
			key = prefix + "/" + key
		}
		if !validArtifactKey(key) || entry.Name() == "." || entry.Name() == ".." {
			return nil, sandbox.ErrArtifactUnverified
		}
		path := filepath.Join(root, filepath.FromSlash(key))
		info, err := os.Lstat(path)
		if err != nil || !safeArtifactFile(path, info) {
			return nil, sandbox.ErrArtifactUnverified
		}
		if info.IsDir() {
			nested, err := collectArtifactFilesBounded(root, key, depth+1, visited)
			if err != nil {
				return nil, err
			}
			files = append(files, nested...)
			continue
		}
		files = append(files, artifactFile{key: key, path: path, info: info})
	}
	return files, nil
}

func safeArtifactFile(path string, info os.FileInfo) bool {
	return info != nil && info.Mode()&os.ModeSymlink == 0 && (info.IsDir() || info.Mode().IsRegular()) && artifactPlatformPathSafe(path, info)
}

func sameArtifactPathSnapshot(file artifactFile, path string) bool {
	info, err := os.Lstat(path)
	return err == nil && safeArtifactFile(path, info) && !info.IsDir() && sameArtifactFileInfo(file.info, info)
}

func sameArtifactFileInfo(before, after os.FileInfo) bool {
	return before != nil && after != nil && before.Mode().IsRegular() && after.Mode().IsRegular() && os.SameFile(before, after) && before.Size() == after.Size() && before.ModTime().Equal(after.ModTime())
}

func execCode(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, sandbox.ErrExecFailed):
		return "exec_failed"
	case errors.Is(err, sandbox.ErrExecTimeout):
		return "timeout"
	case errors.Is(err, sandbox.ErrExecOutputLimit):
		return "output_limit"
	default:
		return "unavailable"
	}
}

func digestFile(file *os.File) (string, int64, error) {
	stat, err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() {
		return "", 0, sandbox.ErrArtifactUnverified
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(file, sandbox.DefaultExecArtifactBytes+1))
	if err != nil || n != stat.Size() || n > sandbox.DefaultExecArtifactBytes {
		return "", 0, sandbox.ErrArtifactUnverified
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// artifactUploadReader hashes and bounds the exact byte sequence consumed by
// the object store. A preflight digest is only an admission check; this reader
// is the digest recorded in the published reference.
type artifactUploadReader struct {
	ctx            context.Context
	reader         io.Reader
	limit          int64
	hash           hashWriter
	size           int64
	err            error
	afterFirstRead func()
	fired          bool
}

// hashWriter keeps the reader testable without exposing a storage-facing API.
type hashWriter interface {
	Write([]byte) (int, error)
	Sum([]byte) []byte
}

func newArtifactUploadReader(ctx context.Context, reader io.Reader, limit int64, afterFirstRead func()) *artifactUploadReader {
	return &artifactUploadReader{ctx: ctx, reader: reader, limit: limit, hash: sha256.New(), afterFirstRead: afterFirstRead}
}

func (r *artifactUploadReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if err := r.ctx.Err(); err != nil {
		r.err = err
		return 0, err
	}
	remaining := r.limit - r.size
	if remaining <= 0 {
		var probe [1]byte
		n, err := r.reader.Read(probe[:])
		if n > 0 {
			r.err = sandbox.ErrArtifactUnverified
			return 0, r.err
		}
		if err != nil {
			if err != io.EOF {
				r.err = err
			}
			return 0, err
		}
		return 0, nil
	}
	if int64(len(p)) > remaining {
		p = p[:int(remaining)]
	}
	n, readErr := r.reader.Read(p)
	if n > 0 {
		_, _ = r.hash.Write(p[:n])
		r.size += int64(n)
		if !r.fired {
			r.fired = true
			if r.afterFirstRead != nil {
				r.afterFirstRead()
			}
		}
	}
	if r.size > r.limit {
		r.err = sandbox.ErrArtifactUnverified
		return n, r.err
	}
	if readErr != nil {
		if readErr != io.EOF {
			r.err = readErr
		}
		return n, readErr
	}
	return n, nil
}

func (r *artifactUploadReader) Digest() string { return hex.EncodeToString(r.hash.Sum(nil)) }
func (r *artifactUploadReader) Size() int64    { return r.size }
func (r *artifactUploadReader) Err() error     { return r.err }

func randomToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func decodeArgv(args map[string]any) ([]string, error) {
	value, ok := args["argv"]
	if !ok {
		return nil, ErrInvalidRequest
	}
	values, ok := value.([]any)
	if !ok || len(values) == 0 {
		return nil, ErrInvalidRequest
	}
	argv := make([]string, len(values))
	for i, value := range values {
		text, ok := value.(string)
		if !ok {
			return nil, ErrInvalidRequest
		}
		argv[i] = text
	}
	if err := sandbox.ValidateCommand(sandbox.Command{Args: argv}); err != nil {
		return nil, fmt.Errorf("%w: argv", ErrInvalidRequest)
	}
	return argv, nil
}

var _ core.Capability = (*Capability)(nil)
