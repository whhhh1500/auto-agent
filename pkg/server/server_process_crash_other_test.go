//go:build !windows

package server

import "os/exec"

func hideCrashHelperWindow(*exec.Cmd) {}
