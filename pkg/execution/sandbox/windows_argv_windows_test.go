//go:build windows

package sandbox

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsEncodeArgvTableAndRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name string
		argv []string
		want string
	}{
		{name: "plain", argv: []string{"tool.exe", "plain"}, want: "tool.exe plain"},
		{name: "empty", argv: []string{"tool.exe", ""}, want: `tool.exe ""`},
		{name: "spaces", argv: []string{"tool.exe", "a value"}, want: `tool.exe "a value"`},
		{name: "quote", argv: []string{"tool.exe", `a"b`}, want: `tool.exe "a\"b"`},
		{name: "trailing slash", argv: []string{"tool.exe", `C:\has space\`}, want: `tool.exe "C:\has space\\"`},
		{name: "unicode", argv: []string{"tool.exe", "你好 世界", "é"}, want: `tool.exe "你好 世界" é`},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := windowsEncodeArgv(test.argv)
			if err != nil || got != test.want {
				t.Fatalf("encoded=%q err=%v want=%q", got, err, test.want)
			}
			decoded, err := windows.DecomposeCommandLine(got)
			if err != nil || !reflect.DeepEqual(decoded, test.argv) {
				t.Fatalf("decoded=%q err=%v want=%q", decoded, err, test.argv)
			}
		})
	}
	// Deterministic property set covers quote/backslash runs around all lengths.
	for _, prefix := range []string{"", "\\", "\\\\", "text\\", " text", "你\\"} {
		for _, suffix := range []string{"", "\\", "\\\\", `"`, `\"`, " space"} {
			argv := []string{"tool.exe", prefix + suffix}
			encoded, err := windowsEncodeArgv(argv)
			if err != nil {
				t.Fatalf("encode %q: %v", argv, err)
			}
			decoded, err := windows.DecomposeCommandLine(encoded)
			if err != nil || !reflect.DeepEqual(decoded, argv) {
				t.Fatalf("round trip argv=%q encoded=%q decoded=%q err=%v", argv, encoded, decoded, err)
			}
		}
	}
}

func TestWindowsExecutableResolverRejectsUnsafePathsAndRechecksIdentity(t *testing.T) {
	root := t.TempDir()
	executable := filepath.Join(root, "tool.exe")
	if err := os.WriteFile(executable, []byte("not executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveWindowsExecutable(executable)
	if err != nil || resolved.path != executable || resolved.verify() != nil {
		t.Fatalf("resolve=%#v err=%v verify=%v", resolved, err, resolved.verify())
	}
	replacement := filepath.Join(root, "replacement.exe")
	if err := os.WriteFile(replacement, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(executable); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, executable); err != nil {
		t.Fatal(err)
	}
	if err := resolved.verify(); err == nil {
		t.Fatal("resolver accepted replaced executable identity")
	}
	applicationName, commandLine, fresh, err := windowsCreateProcessArguments([]string{executable, "a value"})
	decoded, decodeErr := windows.DecomposeCommandLine(commandLine)
	if err != nil || decodeErr != nil || applicationName != executable || fresh.path != executable || !reflect.DeepEqual(decoded, []string{executable, "a value"}) {
		t.Fatalf("CreateProcess arguments app=%q command=%q resolved=%#v err=%v", applicationName, commandLine, fresh, err)
	}

	for _, path := range []string{`tool.exe`, `\\server\share\tool.exe`, `\\?\C:\tool.exe`, `\\.\C:\tool.exe`, executable + `:stream`} {
		if _, err := resolveWindowsExecutable(path); err == nil {
			t.Fatalf("unsafe executable path accepted: %q", path)
		}
	}
	if err := os.Symlink(executable, filepath.Join(root, "link.exe")); err == nil {
		if _, err := resolveWindowsExecutable(filepath.Join(root, "link.exe")); err == nil {
			t.Fatal("reparse executable accepted")
		}
	}
	if err := os.Link(executable, filepath.Join(root, "hardlink.exe")); err == nil {
		hardlink := filepath.Join(root, "hardlink.exe")
		if resolved, err := resolveWindowsExecutable(hardlink); err != nil || resolved.verify() != nil {
			t.Fatalf("hardlinked executable resolve=%#v err=%v verify=%v", resolved, err, resolved.verify())
		}
	}
	if strings.Contains(resolved.path, `\\`) {
		t.Fatalf("resolver accepted non-drive test path: %q", resolved.path)
	}
}
