package server

import (
	"os/exec"
	"syscall"
)

func hideCrashHelperWindow(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
}
