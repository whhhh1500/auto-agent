//go:build windows

package sandboxexec

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const windowsFileReadAttributes = 0x00000080

type windowsArtifactPathIdentity struct {
	volumeSerial uint32
	fileIndex    uint64
}

type windowsArtifactPathSegment struct {
	path       string
	directory  bool
	identity   windowsArtifactPathIdentity
	linkCount  uint32
	attributes uint32
}

// artifactPlatformPathSafe opens the root and every subsequent path component
// with FILE_FLAG_OPEN_REPARSE_POINT. os.Open would resolve a junction before
// GetFileInformationByHandle inspected it, so it is deliberately not used for
// this proof.
func artifactPlatformPathSafe(path string, info os.FileInfo) bool {
	if info == nil {
		return false
	}
	_, ok := windowsArtifactPathIdentityFor(path, info.IsDir())
	return ok
}

func windowsArtifactPathIdentityFor(path string, finalDirectory bool) (windowsArtifactPathIdentity, bool) {
	if !windowsCanonicalDrivePath(path) {
		return windowsArtifactPathIdentity{}, false
	}
	root, parts, ok := windowsArtifactPathParts(path)
	if !ok {
		return windowsArtifactPathIdentity{}, false
	}

	segments := make([]windowsArtifactPathSegment, 0, len(parts)+1)
	rootSegment, ok := windowsOpenArtifactPathSegment(root, true)
	if !ok {
		return windowsArtifactPathIdentity{}, false
	}
	segments = append(segments, rootSegment)
	current := root
	for index, part := range parts {
		current = filepath.Join(current, part)
		directory := index < len(parts)-1 || finalDirectory
		segment, ok := windowsOpenArtifactPathSegment(current, directory)
		if !ok {
			return windowsArtifactPathIdentity{}, false
		}
		segments = append(segments, segment)
	}
	// Re-read every handle identity after the whole walk. This catches stable
	// replacement of the root or an intermediate directory while keeping the
	// decision fail-closed if any component becomes a reparse point.
	for _, segment := range segments {
		currentSegment, ok := windowsOpenArtifactPathSegment(segment.path, segment.directory)
		if !ok || currentSegment.identity != segment.identity || currentSegment.linkCount != segment.linkCount || currentSegment.attributes != segment.attributes {
			return windowsArtifactPathIdentity{}, false
		}
	}
	return segments[len(segments)-1].identity, true
}

func windowsCanonicalDrivePath(path string) bool {
	if path == "" || filepath.Clean(path) != path || strings.HasPrefix(path, `\\`) || len(path) < 3 {
		return false
	}
	volume := filepath.VolumeName(path)
	if len(volume) != 2 || volume[1] != ':' || !((volume[0] >= 'a' && volume[0] <= 'z') || (volume[0] >= 'A' && volume[0] <= 'Z')) {
		return false
	}
	root := volume + string(filepath.Separator)
	if !strings.HasPrefix(path, root) {
		return false
	}
	return !strings.Contains(path[len(root):], ":")
}

func windowsArtifactPathParts(path string) (string, []string, bool) {
	if !windowsCanonicalDrivePath(path) {
		return "", nil, false
	}
	volume := filepath.VolumeName(path)
	root := volume + string(filepath.Separator)
	if len(path) < len(root) || !strings.EqualFold(path[:len(root)], root) {
		return "", nil, false
	}
	relative := path[len(root):]
	if relative == "" {
		return root, nil, true
	}
	parts := strings.Split(relative, string(filepath.Separator))
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", nil, false
		}
	}
	return root, parts, true
}

func windowsOpenArtifactPathSegment(path string, directory bool) (windowsArtifactPathSegment, bool) {
	widePath, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return windowsArtifactPathSegment{}, false
	}
	handle, err := syscall.CreateFile(
		widePath,
		windowsFileReadAttributes,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_FLAG_OPEN_REPARSE_POINT|syscall.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return windowsArtifactPathSegment{}, false
	}
	defer syscall.CloseHandle(handle)
	var metadata syscall.ByHandleFileInformation
	if err := syscall.GetFileInformationByHandle(handle, &metadata); err != nil {
		return windowsArtifactPathSegment{}, false
	}
	if metadata.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return windowsArtifactPathSegment{}, false
	}
	isDirectory := metadata.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0
	if isDirectory != directory || (!isDirectory && metadata.NumberOfLinks != 1) {
		return windowsArtifactPathSegment{}, false
	}
	return windowsArtifactPathSegment{
		path:       path,
		directory:  isDirectory,
		identity:   windowsArtifactPathIdentity{volumeSerial: metadata.VolumeSerialNumber, fileIndex: uint64(metadata.FileIndexHigh)<<32 | uint64(metadata.FileIndexLow)},
		linkCount:  metadata.NumberOfLinks,
		attributes: metadata.FileAttributes,
	}, true
}
