//go:build windows

package sandbox

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsFixedMountBindingsRejectsFallbacks(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	artifacts := filepath.Join(root, "artifacts")
	for _, path := range []string{workspace, artifacts} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	bindings, err := windowsFixedMountBindings([]Mount{
		{Source: workspace, Target: "/workspace"},
		{Source: artifacts, Target: "/artifacts"},
	}, workspace, artifacts)
	if err != nil {
		t.Fatalf("valid fixed mappings: %v", err)
	}
	if bindings.workspace != filepath.Clean(workspace) || bindings.artifacts != filepath.Clean(artifacts) {
		t.Fatalf("bindings=%#v", bindings)
	}

	for name, mounts := range map[string][]Mount{
		"missing workspace": {{Source: artifacts, Target: "/artifacts"}},
		"extra host mount": {
			{Source: workspace, Target: "/workspace", ReadOnly: true},
			{Source: artifacts, Target: "/artifacts"},
			{Source: root, Target: "/other"},
		},
		"workspace is readonly": {
			{Source: workspace, Target: "/workspace", ReadOnly: true},
			{Source: artifacts, Target: "/artifacts"},
		},
		"artifacts are readonly": {
			{Source: workspace, Target: "/workspace"},
			{Source: artifacts, Target: "/artifacts", ReadOnly: true},
		},
		"caller source differs": {
			{Source: root, Target: "/workspace"},
			{Source: artifacts, Target: "/artifacts"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := windowsFixedMountBindings(mounts, workspace, artifacts); !errors.Is(err, ErrInvalidSpec) {
				t.Fatalf("windowsFixedMountBindings error=%v", err)
			}
		})
	}
}

func TestWindowsFreshEnvironmentIsFixedAndSessionScoped(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	artifacts := filepath.Join(root, "artifacts")
	for _, path := range []string{workspace, artifacts} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("HTTP_PROXY", "http://caller.invalid")
	t.Setenv("HARNESS_CALLER_SENTINEL", "must-not-leak")
	t.Setenv("NUMBER_OF_PROCESSORS", "31337")
	t.Setenv("PROCESSOR_ARCHITECTURE", "CALLER-ARCH")
	t.Setenv("SystemRoot", `Z:\caller-root`)
	env, err := windowsFreshEnvironment(root, workspace, artifacts)
	if err != nil {
		t.Fatal(err)
	}
	values := windowsEnvironmentMap(t, env)
	if _, ok := values["HTTP_PROXY"]; ok {
		t.Fatalf("caller proxy leaked into fresh environment: %#v", values)
	}
	if _, ok := values["HARNESS_CALLER_SENTINEL"]; ok {
		t.Fatalf("caller environment leaked into fresh environment: %#v", values)
	}
	if _, ok := values["NUMBER_OF_PROCESSORS"]; ok {
		t.Fatalf("caller processor count leaked into fresh environment: %#v", values)
	}
	if values["HARNESS_WORKSPACE"] != filepath.Clean(workspace) || values["HARNESS_ARTIFACTS"] != filepath.Clean(artifacts) {
		t.Fatalf("fixed mount environment=%#v", values)
	}
	for _, key := range []string{"HOME", "USERPROFILE", "TEMP", "TMP", "GOCACHE", "GOMODCACHE", "GOTMPDIR"} {
		value, ok := values[key]
		if !ok || !windowsPathIsStrictChild(root, value) {
			t.Fatalf("%s=%q is not session-scoped below %q", key, value, root)
		}
	}
	for _, key := range []string{"SYSTEMROOT", "WINDIR", "SYSTEMDRIVE", "OS", "PROCESSOR_ARCHITECTURE", "COMSPEC", "PATH"} {
		if values[key] == "" {
			t.Fatalf("missing fixed system environment value %s: %#v", key, values)
		}
	}
	if values["PROCESSOR_ARCHITECTURE"] == "CALLER-ARCH" || values["SYSTEMROOT"] == `Z:\caller-root` {
		t.Fatalf("caller system environment leaked into fixed allowlist: %#v", values)
	}
}

func TestWindowsProviderEnvironmentStaysFreshUntilCurrentUserLaunch(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	artifacts := filepath.Join(root, "artifacts")
	for _, path := range []string{workspace, artifacts} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	environment, err := windowsProviderEnvironment(root, workspace, artifacts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := windowsCurrentUserEnvironmentBlock(environment, true); err != nil {
		t.Fatalf("current-user launch environment: %v", err)
	}
	values := windowsEnvironmentMap(t, environment)
	if values["GOTMPDIR"] != filepath.Join(root, "tmp") {
		t.Fatalf("GOTMPDIR=%q, want durable top-level tmp", values["GOTMPDIR"])
	}
	for _, key := range []string{
		"HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA", "TEMP", "TMP", "GOTMPDIR", "GOCACHE", "GOMODCACHE",
		"XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "XDG_RUNTIME_DIR", "GOPATH", "PNPM_HOME", "NPM_CONFIG_CACHE", "YARN_CACHE_FOLDER", "CARGO_HOME", "RUSTUP_HOME",
	} {
		value, ok := values[key]
		if !ok || !windowsPathIsStrictChild(root, value) {
			t.Fatalf("%s=%q is not session-scoped below %q", key, value, root)
		}
	}
	home := filepath.Join(root, "home")
	if values["HOMEDRIVE"] != filepath.VolumeName(home) || !windowsPathsEqual(values["HOMEDRIVE"]+values["HOMEPATH"], home) {
		t.Fatalf("HOMEDRIVE/HOMEPATH=%q/%q, want %q", values["HOMEDRIVE"], values["HOMEPATH"], home)
	}
	for _, value := range values {
		if windowsPathIsUnderUserProfiles(value) && !windowsPathsEqual(value, root) && !windowsPathIsStrictChild(root, value) {
			t.Fatalf("host profile path leaked outside session root %q: %q", root, value)
		}
	}
	for _, path := range []string{filepath.Join(root, "home"), filepath.Join(root, "tmp"), filepath.Join(root, "cache"), filepath.Join(root, "cache", "go-build")} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("environment policy created nested session path %q: %v", path, err)
		}
	}
}

func windowsPathIsUnderUserProfiles(path string) bool {
	cleanPath := filepath.Clean(path)
	volume := filepath.VolumeName(cleanPath)
	if volume == "" {
		return false
	}
	return windowsPathIsStrictChild(filepath.Join(volume+string(filepath.Separator), "Users"), cleanPath)
}

func TestScanWindowsArtifactsRejectsUnsafeAndChangedFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "ok.txt"), []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nested", "result.txt"), []byte("nested"), 0o600); err != nil {
		t.Fatal(err)
	}
	set, err := scanWindowsArtifacts(root, ArtifactPolicy{MaxArtifacts: 2, MaxTotalBytes: 16})
	if err != nil {
		t.Fatalf("scan valid artifacts: %v", err)
	}
	if len(set.Items) != 2 || set.Items[0].Key != "nested/result.txt" || set.Items[1].Key != "ok.txt" {
		t.Fatalf("artifacts=%#v", set)
	}

	t.Run("hard link", func(t *testing.T) {
		artifactRoot := t.TempDir()
		outside := filepath.Join(t.TempDir(), "secret")
		if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(outside, filepath.Join(artifactRoot, "linked-secret")); err != nil {
			t.Fatal(err)
		}
		if _, err := scanWindowsArtifacts(artifactRoot, ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 16}); !errors.Is(err, ErrArtifactUnverified) {
			t.Fatalf("hard link accepted: %v", err)
		}
	})

	t.Run("symlink when unprivileged creation is enabled", func(t *testing.T) {
		artifactRoot := t.TempDir()
		link := filepath.Join(artifactRoot, "link")
		if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), link); err != nil {
			// Some Windows policies disable unprivileged symbolic-link creation.
			// Metadata validation above still deterministically exercises the
			// scanner's fail-closed reparse branch on those hosts.
			t.Logf("unprivileged symlink creation unavailable: %v", err)
			return
		}
		if _, err := scanWindowsArtifacts(artifactRoot, ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 16}); !errors.Is(err, ErrArtifactUnverified) {
			t.Fatalf("symlink accepted: %v", err)
		}

		linkedRoot := filepath.Join(t.TempDir(), "linked-root")
		if err := os.Symlink(artifactRoot, linkedRoot); err != nil {
			t.Fatal(err)
		}
		if _, err := scanWindowsArtifacts(linkedRoot, ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 16}); !errors.Is(err, ErrArtifactUnverified) {
			t.Fatalf("reparse root accepted: %v", err)
		}
	})

	t.Run("mutation race", func(t *testing.T) {
		artifactRoot := t.TempDir()
		artifact := filepath.Join(artifactRoot, "result")
		if err := os.WriteFile(artifact, []byte("before"), 0o600); err != nil {
			t.Fatal(err)
		}
		previous := windowsArtifactScanBeforeReadHook
		windowsArtifactScanBeforeReadHook = func(string) {
			if err := os.WriteFile(artifact, []byte("after mutation"), 0o600); err != nil {
				t.Fatalf("mutate artifact: %v", err)
			}
		}
		t.Cleanup(func() { windowsArtifactScanBeforeReadHook = previous })
		if _, err := scanWindowsArtifacts(artifactRoot, ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 32}); !errors.Is(err, ErrArtifactUnverified) {
			t.Fatalf("changed artifact accepted: %v", err)
		}
	})

	t.Run("limits", func(t *testing.T) {
		if _, err := scanWindowsArtifacts(root, ArtifactPolicy{MaxArtifacts: 1, MaxTotalBytes: 16}); !errors.Is(err, ErrArtifactUnverified) {
			t.Fatalf("count limit accepted: %v", err)
		}
		if _, err := scanWindowsArtifacts(root, ArtifactPolicy{MaxArtifacts: 2, MaxTotalBytes: 3}); !errors.Is(err, ErrArtifactUnverified) {
			t.Fatalf("byte limit accepted: %v", err)
		}
	})
}

func TestWindowsArtifactMetadataRejectsReparseAndHardLink(t *testing.T) {
	base := windowsArtifactMetadata{volumeSerial: 1, fileIndex: 2, size: 3, writeTime: 4, links: 1}
	if err := base.validateRegularFile(); err != nil {
		t.Fatalf("regular metadata rejected: %v", err)
	}
	for name, metadata := range map[string]windowsArtifactMetadata{
		"reparse":   {volumeSerial: 1, fileIndex: 2, size: 3, writeTime: 4, links: 1, attributes: windows.FILE_ATTRIBUTE_REPARSE_POINT},
		"hard link": {volumeSerial: 1, fileIndex: 2, size: 3, writeTime: 4, links: 2},
		"directory": {volumeSerial: 1, fileIndex: 2, size: 3, writeTime: 4, links: 1, attributes: windows.FILE_ATTRIBUTE_DIRECTORY},
	} {
		t.Run(name, func(t *testing.T) {
			if err := metadata.validateRegularFile(); !errors.Is(err, ErrArtifactUnverified) {
				t.Fatalf("metadata=%#v error=%v", metadata, err)
			}
		})
	}
}

func windowsEnvironmentMap(t *testing.T, env []string) map[string]string {
	t.Helper()
	values := make(map[string]string, len(env))
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			t.Fatalf("invalid environment entry %q", entry)
		}
		upper := strings.ToUpper(key)
		if _, exists := values[upper]; exists {
			t.Fatalf("duplicate environment key %q", key)
		}
		values[upper] = value
	}
	return values
}
