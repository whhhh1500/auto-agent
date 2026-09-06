//go:build windows

package sandbox

import (
	"path/filepath"
	"strings"
	"unicode/utf8"

	"golang.org/x/sys/windows"
)

type windowsResolvedExecutable struct {
	path     string
	identity windowsArtifactMetadata
}

// windowsCreateProcessArguments always supplies ApplicationName separately.
// The command line uses the resolved path as argv[0], never a shell or PATH
// lookup, so CreateProcess cannot reinterpret a caller-selected executable.
func windowsCreateProcessArguments(argv []string) (applicationName, commandLine string, resolved windowsResolvedExecutable, err error) {
	if ValidateCommand(Command{Args: argv}) != nil {
		return "", "", windowsResolvedExecutable{}, ErrInvalidSpec
	}
	resolved, err = resolveWindowsExecutable(argv[0])
	if err != nil {
		return "", "", windowsResolvedExecutable{}, ErrUnavailable
	}
	commandLine, err = windowsEncodeArgv(append([]string{resolved.path}, argv[1:]...))
	if err != nil {
		return "", "", windowsResolvedExecutable{}, ErrInvalidSpec
	}
	return resolved.path, commandLine, resolved, nil
}

func resolveWindowsExecutable(path string) (windowsResolvedExecutable, error) {
	clean, err := cleanWindowsAbsoluteDirectory(path)
	if err != nil {
		return windowsResolvedExecutable{}, ErrInvalidSpec
	}
	metadata, err := windowsExecutableMetadata(clean)
	if err != nil {
		return windowsResolvedExecutable{}, ErrInvalidSpec
	}
	return windowsResolvedExecutable{path: clean, identity: metadata}, nil
}

func (executable windowsResolvedExecutable) verify() error {
	if executable.path == "" || executable.identity.fileIndex == 0 {
		return ErrInvalidSpec
	}
	metadata, err := windowsExecutableMetadata(executable.path)
	if err != nil || !executable.identity.sameFile(metadata) {
		return ErrInvalidSpec
	}
	return nil
}

func windowsExecutableMetadata(path string) (windowsArtifactMetadata, error) {
	parentPath, name := filepath.Dir(path), filepath.Base(path)
	if parentPath == path || !validWindowsArtifactSegment(name) {
		return windowsArtifactMetadata{}, ErrInvalidSpec
	}
	parent, err := openWindowsRetainedDirectory(parentPath)
	if err != nil {
		return windowsArtifactMetadata{}, ErrInvalidSpec
	}
	if err := parent.verifyPath(); err != nil {
		_ = parent.close()
		return windowsArtifactMetadata{}, ErrInvalidSpec
	}
	handle, metadata, err := openWindowsArtifactChild(windows.Handle(parent.file.Fd()), name)
	if err != nil {
		_ = parent.close()
		return windowsArtifactMetadata{}, ErrInvalidSpec
	}
	closeErr := windows.CloseHandle(handle)
	parentCloseErr := parent.close()
	// Executables may legitimately be hard-linked (for example, a Windows
	// system binary). Unlike artifacts, executable admission rejects reparse
	// points but does not require a single link. CreateProcess receives the
	// verified absolute application path with no shell or PATH lookup.
	if closeErr != nil || parentCloseErr != nil || metadata.attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 || metadata.attributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 || metadata.fileIndex == 0 {
		return windowsArtifactMetadata{}, ErrInvalidSpec
	}
	return metadata, nil
}

func windowsEncodeArgv(argv []string) (string, error) {
	if len(argv) == 0 || len(argv) > MaxArgs {
		return "", ErrInvalidSpec
	}
	encoded := make([]string, len(argv))
	for index, argument := range argv {
		if !validWindowsCommandArgument(argument) {
			return "", ErrInvalidSpec
		}
		encoded[index] = windowsQuoteArgument(argument)
	}
	return strings.Join(encoded, " "), nil
}

func validWindowsCommandArgument(argument string) bool {
	return len(argument) <= MaxArgBytes && utf8.ValidString(argument) && strings.IndexByte(argument, 0) < 0
}

// windowsQuoteArgument implements CommandLineToArgvW-compatible quoting: runs
// of backslashes only double when they precede a quote or the closing quote.
func windowsQuoteArgument(argument string) string {
	if argument != "" && !strings.ContainsAny(argument, " \t\n\v\"") {
		return argument
	}
	var builder strings.Builder
	builder.Grow(len(argument) + 2)
	builder.WriteByte('"')
	backslashes := 0
	for _, character := range argument {
		if character == '\\' {
			backslashes++
			continue
		}
		if character == '"' {
			builder.WriteString(strings.Repeat("\\", backslashes*2+1))
			builder.WriteRune(character)
			backslashes = 0
			continue
		}
		builder.WriteString(strings.Repeat("\\", backslashes))
		backslashes = 0
		builder.WriteRune(character)
	}
	builder.WriteString(strings.Repeat("\\", backslashes*2))
	builder.WriteByte('"')
	return builder.String()
}
