package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

func privateFilesystemEnforced() bool { return runtime.GOOS != "windows" }

func privateMkdirAll(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	return privateChmod(path, 0o700)
}

func privateWriteFile(path string, data []byte) error {
	if err := privateMkdirAll(filepath.Dir(path)); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return err
	}
	return privateChmod(path, 0o600)
}

func privateChmod(path string, mode os.FileMode) error {
	if !privateFilesystemEnforced() {
		return nil
	}
	if err := os.Chmod(path, mode); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Mode().Perm() != mode.Perm() {
		return fmt.Errorf("private path %s mode=%#o want %#o", path, info.Mode().Perm(), mode.Perm())
	}
	return nil
}
