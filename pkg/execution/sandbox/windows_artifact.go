//go:build windows

package sandbox

import (
	"crypto/sha256"
	"debug/pe"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

// windowsMountBindings is the private translation from the portable mount
// contract to the two paths owned by a Windows session.  Windows does not
// expose arbitrary caller-selected host mounts to the child.
type windowsMountBindings struct {
	workspace string
	artifacts string
}

func windowsFixedMountBindings(mounts []Mount, workspace, artifacts string) (windowsMountBindings, error) {
	workspace = filepath.Clean(workspace)
	artifacts = filepath.Clean(artifacts)
	if !filepath.IsAbs(workspace) || !filepath.IsAbs(artifacts) || windowsPathsEqual(workspace, artifacts) || len(mounts) != 2 {
		return windowsMountBindings{}, ErrInvalidSpec
	}
	var haveWorkspace, haveArtifacts bool
	for _, mount := range mounts {
		source := filepath.Clean(mount.Source)
		switch mount.Target {
		case "/workspace":
			if haveWorkspace || mount.ReadOnly || !windowsPathsEqual(source, workspace) {
				return windowsMountBindings{}, ErrInvalidSpec
			}
			haveWorkspace = true
		case "/artifacts":
			if haveArtifacts || mount.ReadOnly || !windowsPathsEqual(source, artifacts) {
				return windowsMountBindings{}, ErrInvalidSpec
			}
			haveArtifacts = true
		default:
			return windowsMountBindings{}, ErrInvalidSpec
		}
	}
	if !haveWorkspace || !haveArtifacts {
		return windowsMountBindings{}, ErrInvalidSpec
	}
	return windowsMountBindings{workspace: workspace, artifacts: artifacts}, nil
}

func windowsFreshEnvironment(sessionRoot, workspace, artifacts string) ([]string, error) {
	sessionRoot = filepath.Clean(sessionRoot)
	workspace = filepath.Clean(workspace)
	artifacts = filepath.Clean(artifacts)
	if !filepath.IsAbs(sessionRoot) || !windowsPathIsStrictChild(sessionRoot, workspace) || !windowsPathIsStrictChild(sessionRoot, artifacts) {
		return nil, ErrInvalidSpec
	}
	windowsRoot, err := windows.GetWindowsDirectory()
	if err != nil {
		return nil, ErrUnavailable
	}
	systemDirectory, err := windows.GetSystemDirectory()
	if err != nil {
		return nil, ErrUnavailable
	}
	systemDrive := filepath.VolumeName(windowsRoot)
	architecture, err := windowsNativeArchitecture()
	if err != nil || systemDrive == "" {
		return nil, ErrUnavailable
	}
	home := filepath.Join(sessionRoot, "home")
	temporary := filepath.Join(sessionRoot, "tmp")
	cache := filepath.Join(sessionRoot, "cache")
	volume := filepath.VolumeName(home)
	homePath := strings.TrimPrefix(home, volume)
	if volume == "" || homePath == home {
		return nil, ErrInvalidSpec
	}
	// Do not start from os.Environ: caller proxy, credential, profile, and tool
	// variables are all intentionally excluded.  The fixed system entries make
	// direct Windows executable launch possible without a user profile.
	return []string{
		"SystemRoot=" + windowsRoot,
		"windir=" + windowsRoot,
		"SystemDrive=" + systemDrive,
		"OS=Windows_NT",
		"PROCESSOR_ARCHITECTURE=" + architecture,
		"ComSpec=" + filepath.Join(systemDirectory, "cmd.exe"),
		"PATH=" + systemDirectory,
		"HARNESS_WORKSPACE=" + workspace,
		"HARNESS_ARTIFACTS=" + artifacts,
		"HOME=" + home,
		"USERPROFILE=" + home,
		"HOMEDRIVE=" + volume,
		"HOMEPATH=" + homePath,
		// The session root contains only its fixed direct children at Start. A
		// restricted target can create tool-specific descendants under them,
		// but Start does not pre-authorize additional paths.
		"APPDATA=" + home,
		"LOCALAPPDATA=" + home,
		"TEMP=" + temporary,
		"TMP=" + temporary,
		// The fixed temporary root already exists. The target may create
		// tool-specific descendants under its restricted-token default DACL.
		"GOTMPDIR=" + temporary,
		"GOCACHE=" + cache,
		"GOMODCACHE=" + cache,
	}, nil
}

func windowsNativeArchitecture() (string, error) {
	var processMachine, nativeMachine uint16
	if err := windows.IsWow64Process2(windows.CurrentProcess(), &processMachine, &nativeMachine); err != nil {
		return "", err
	}
	switch nativeMachine {
	case pe.IMAGE_FILE_MACHINE_I386:
		return "x86", nil
	case pe.IMAGE_FILE_MACHINE_AMD64:
		return "AMD64", nil
	case pe.IMAGE_FILE_MACHINE_ARMNT:
		return "ARM", nil
	case pe.IMAGE_FILE_MACHINE_ARM64:
		return "ARM64", nil
	default:
		return "", ErrUnavailable
	}
}

func windowsPathsEqual(left, right string) bool {
	return strings.EqualFold(filepath.Clean(left), filepath.Clean(right))
}

func windowsPathIsStrictChild(root, child string) bool {
	cleanRoot := filepath.Clean(root)
	cleanChild := filepath.Clean(child)
	relative, err := filepath.Rel(cleanRoot, cleanChild)
	if err != nil || relative == "." || filepath.IsAbs(relative) || relative == ".." {
		return false
	}
	return !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

type windowsArtifactMetadata struct {
	attributes   uint32
	volumeSerial uint32
	fileIndex    uint64
	size         int64
	writeTime    uint64
	links        uint32
}

func windowsArtifactMetadataForHandle(handle windows.Handle) (windowsArtifactMetadata, error) {
	var information windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &information); err != nil {
		return windowsArtifactMetadata{}, err
	}
	return windowsArtifactMetadata{
		attributes:   information.FileAttributes,
		volumeSerial: information.VolumeSerialNumber,
		fileIndex:    uint64(information.FileIndexHigh)<<32 | uint64(information.FileIndexLow),
		size:         int64(uint64(information.FileSizeHigh)<<32 | uint64(information.FileSizeLow)),
		writeTime:    uint64(information.LastWriteTime.HighDateTime)<<32 | uint64(information.LastWriteTime.LowDateTime),
		links:        information.NumberOfLinks,
	}, nil
}

func (metadata windowsArtifactMetadata) validateDirectory() error {
	if metadata.attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 || metadata.attributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 || metadata.fileIndex == 0 {
		return ErrArtifactUnverified
	}
	return nil
}

func (metadata windowsArtifactMetadata) validateRegularFile() error {
	if metadata.attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 || metadata.attributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 || metadata.fileIndex == 0 || metadata.size < 0 || metadata.size > MaxArtifactBytes || metadata.links != 1 {
		return ErrArtifactUnverified
	}
	return nil
}

func (metadata windowsArtifactMetadata) sameFile(other windowsArtifactMetadata) bool {
	return metadata.attributes == other.attributes && metadata.volumeSerial == other.volumeSerial && metadata.fileIndex == other.fileIndex && metadata.size == other.size && metadata.writeTime == other.writeTime && metadata.links == other.links
}

func (metadata windowsArtifactMetadata) sameObject(other windowsArtifactMetadata) bool {
	return metadata.volumeSerial == other.volumeSerial && metadata.fileIndex == other.fileIndex
}

// windowsRetainedDirectory owns a reparse-safe directory handle and its stable
// identity.  Callers that later publish files by path must call verifyPath
// immediately before publication to reject a legal (non-reparse) replacement.
type windowsRetainedDirectory struct {
	path     string
	file     *os.File
	identity windowsArtifactMetadata
}

func openWindowsRetainedDirectory(path string) (*windowsRetainedDirectory, error) {
	file, identity, err := openWindowsArtifactDirectory(path)
	if err != nil {
		return nil, ErrArtifactUnverified
	}
	return &windowsRetainedDirectory{path: filepath.Clean(path), file: file, identity: identity}, nil
}

func (directory *windowsRetainedDirectory) close() error {
	if directory == nil || directory.file == nil {
		return ErrArtifactUnverified
	}
	err := directory.file.Close()
	directory.file = nil
	if err != nil {
		return ErrArtifactUnverified
	}
	return nil
}

func (directory *windowsRetainedDirectory) verifyPath() error {
	if directory == nil || directory.file == nil {
		return ErrArtifactUnverified
	}
	retained, err := windowsArtifactMetadataForHandle(windows.Handle(directory.file.Fd()))
	if err != nil || retained.validateDirectory() != nil || !directory.identity.sameObject(retained) {
		return ErrArtifactUnverified
	}
	opened, observed, err := openWindowsArtifactDirectory(directory.path)
	if err != nil {
		return ErrArtifactUnverified
	}
	closeErr := opened.Close()
	if closeErr != nil || observed.validateDirectory() != nil || !directory.identity.sameObject(observed) {
		return ErrArtifactUnverified
	}
	return nil
}

func (directory *windowsRetainedDirectory) duplicate() (*os.File, error) {
	if directory == nil || directory.file == nil {
		return nil, ErrArtifactUnverified
	}
	var duplicate windows.Handle
	if err := windows.DuplicateHandle(windows.CurrentProcess(), windows.Handle(directory.file.Fd()), windows.CurrentProcess(), &duplicate, 0, false, windows.DUPLICATE_SAME_ACCESS); err != nil {
		return nil, ErrArtifactUnverified
	}
	file := os.NewFile(uintptr(duplicate), "sandbox-windows-artifact-directory-copy")
	if file == nil {
		_ = windows.CloseHandle(duplicate)
		return nil, ErrArtifactUnverified
	}
	return file, nil
}

func scanWindowsArtifacts(root string, policy ArtifactPolicy) (ArtifactSet, error) {
	directory, err := openWindowsRetainedDirectory(root)
	if err != nil {
		return ArtifactSet{}, ErrArtifactUnverified
	}
	defer directory.close()
	return scanWindowsArtifactsFromRetained(directory, policy)
}

func scanWindowsArtifactsFromRetained(directory *windowsRetainedDirectory, policy ArtifactPolicy) (ArtifactSet, error) {
	if policy.MaxArtifacts <= 0 || policy.MaxArtifacts > MaxArtifactCount || policy.MaxTotalBytes <= 0 || policy.MaxTotalBytes > MaxArtifactBytes {
		return ArtifactSet{}, ErrArtifactUnverified
	}
	if directory == nil || directory.file == nil {
		return ArtifactSet{}, ErrArtifactUnverified
	}
	copy, err := directory.duplicate()
	if err != nil {
		return ArtifactSet{}, ErrArtifactUnverified
	}
	defer copy.Close()
	result := ArtifactSet{Items: make([]Artifact, 0, policy.MaxArtifacts)}
	var total int64
	if err := walkWindowsArtifactDirectory(copy, "", policy, &result, &total); err != nil {
		return ArtifactSet{}, ErrArtifactUnverified
	}
	after, err := windowsArtifactMetadataForHandle(windows.Handle(copy.Fd()))
	if err != nil || after.validateDirectory() != nil || !directory.identity.sameObject(after) {
		return ArtifactSet{}, ErrArtifactUnverified
	}
	sort.Slice(result.Items, func(left, right int) bool { return result.Items[left].Key < result.Items[right].Key })
	return result, nil
}

func openWindowsArtifactDirectory(path string) (*os.File, windowsArtifactMetadata, error) {
	clean, err := cleanWindowsAbsoluteDirectory(path)
	if err != nil {
		return nil, windowsArtifactMetadata{}, err
	}
	volume := filepath.VolumeName(clean)
	volumeRoot := volume + `\`
	handle, metadata, err := openWindowsDirectoryPath(volumeRoot)
	if err != nil {
		return nil, windowsArtifactMetadata{}, err
	}
	for _, part := range strings.Split(strings.TrimPrefix(clean, volumeRoot), `\`) {
		if !validWindowsArtifactSegment(part) {
			_ = windows.CloseHandle(handle)
			return nil, windowsArtifactMetadata{}, ErrArtifactUnverified
		}
		child, childMetadata, childErr := openWindowsArtifactChild(handle, part)
		closeErr := windows.CloseHandle(handle)
		if childErr != nil || closeErr != nil || childMetadata.validateDirectory() != nil {
			if childErr == nil {
				_ = windows.CloseHandle(child)
			}
			return nil, windowsArtifactMetadata{}, ErrArtifactUnverified
		}
		handle, metadata = child, childMetadata
	}
	file := os.NewFile(uintptr(handle), "sandbox-windows-artifacts")
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, windowsArtifactMetadata{}, ErrArtifactUnverified
	}
	return file, metadata, nil
}

func cleanWindowsAbsoluteDirectory(path string) (string, error) {
	if path == "" || strings.HasPrefix(path, `\\`) || strings.HasPrefix(path, `//`) || strings.HasPrefix(path, `\\?\`) || strings.IndexByte(path, 0) >= 0 {
		return "", ErrArtifactUnverified
	}
	raw := strings.ReplaceAll(path, "/", `\`)
	volume := filepath.VolumeName(raw)
	if len(volume) != 2 || volume[1] != ':' {
		return "", ErrArtifactUnverified
	}
	for _, part := range strings.Split(strings.TrimPrefix(raw, volume+`\`), `\`) {
		if part == "." || part == ".." {
			return "", ErrArtifactUnverified
		}
	}
	clean := filepath.Clean(raw)
	if !filepath.IsAbs(clean) || len(volume) != 2 || volume[1] != ':' || strings.EqualFold(clean, volume+`\`) || strings.Contains(clean[len(volume):], ":") {
		return "", ErrArtifactUnverified
	}
	return clean, nil
}

func openWindowsDirectoryPath(path string) (windows.Handle, windowsArtifactMetadata, error) {
	pointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, windowsArtifactMetadata{}, err
	}
	handle, err := windows.CreateFile(pointer, windows.FILE_LIST_DIRECTORY|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return 0, windowsArtifactMetadata{}, err
	}
	metadata, infoErr := windowsArtifactMetadataForHandle(handle)
	if infoErr != nil || metadata.validateDirectory() != nil {
		_ = windows.CloseHandle(handle)
		return 0, windowsArtifactMetadata{}, ErrArtifactUnverified
	}
	return handle, metadata, nil
}

func openWindowsArtifactChild(parent windows.Handle, name string) (windows.Handle, windowsArtifactMetadata, error) {
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return 0, windowsArtifactMetadata{}, err
	}
	attributes := windows.OBJECT_ATTRIBUTES{RootDirectory: parent, ObjectName: objectName, Attributes: windows.OBJ_CASE_INSENSITIVE}
	attributes.Length = uint32(unsafe.Sizeof(attributes))
	var status windows.IO_STATUS_BLOCK
	var handle windows.Handle
	if err := windows.NtCreateFile(&handle, windows.FILE_READ_DATA|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE, &attributes, &status, nil, 0, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, windows.FILE_OPEN, windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT, 0, 0); err != nil {
		return 0, windowsArtifactMetadata{}, err
	}
	metadata, infoErr := windowsArtifactMetadataForHandle(handle)
	if infoErr != nil || metadata.attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		_ = windows.CloseHandle(handle)
		return 0, windowsArtifactMetadata{}, ErrArtifactUnverified
	}
	return handle, metadata, nil
}

func walkWindowsArtifactDirectory(directory *os.File, prefix string, policy ArtifactPolicy, result *ArtifactSet, total *int64) error {
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return ErrArtifactUnverified
	}
	for _, entry := range entries {
		name := entry.Name()
		if !validWindowsArtifactSegment(name) {
			return ErrArtifactUnverified
		}
		key := name
		if prefix != "" {
			key = prefix + "/" + name
		}
		handle, metadata, err := openWindowsArtifactChild(windows.Handle(directory.Fd()), name)
		if err != nil {
			return ErrArtifactUnverified
		}
		if metadata.attributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
			if metadata.validateDirectory() != nil || !validArtifactKey(key) {
				_ = windows.CloseHandle(handle)
				return ErrArtifactUnverified
			}
			child := os.NewFile(uintptr(handle), "sandbox-windows-artifact-directory")
			if child == nil {
				_ = windows.CloseHandle(handle)
				return ErrArtifactUnverified
			}
			walkErr := walkWindowsArtifactDirectory(child, key, policy, result, total)
			closeErr := child.Close()
			if walkErr != nil || closeErr != nil {
				return ErrArtifactUnverified
			}
			continue
		}
		if err := readWindowsArtifact(handle, key, metadata, policy, result, total); err != nil {
			return ErrArtifactUnverified
		}
	}
	return nil
}

// windowsArtifactScanBeforeReadHook is a package-private test seam for a
// check-to-read mutation.  It receives only a relative artifact key.
var windowsArtifactScanBeforeReadHook = func(string) {}

func readWindowsArtifact(handle windows.Handle, key string, before windowsArtifactMetadata, policy ArtifactPolicy, result *ArtifactSet, total *int64) error {
	file := os.NewFile(uintptr(handle), "sandbox-windows-artifact")
	if file == nil {
		_ = windows.CloseHandle(handle)
		return ErrArtifactUnverified
	}
	defer file.Close()
	if before.validateRegularFile() != nil || !validArtifactKey(key) || len(result.Items) >= policy.MaxArtifacts || *total > policy.MaxTotalBytes-before.size {
		return ErrArtifactUnverified
	}
	windowsArtifactScanBeforeReadHook(key)
	hash := sha256.New()
	copied, copyErr := io.CopyN(hash, file, before.size+1)
	after, infoErr := windowsArtifactMetadataForHandle(handle)
	if infoErr != nil || after.validateRegularFile() != nil || copied != before.size || (copyErr != nil && !errors.Is(copyErr, io.EOF)) || !before.sameFile(after) {
		return ErrArtifactUnverified
	}
	result.Items = append(result.Items, Artifact{Key: key, Digest: hex.EncodeToString(hash.Sum(nil)), Size: before.size, Verified: true})
	*total += before.size
	return nil
}

func validWindowsArtifactSegment(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, `\\/:`) && !strings.ContainsRune(name, 0) && !strings.ContainsAny(name, "\r\n")
}
