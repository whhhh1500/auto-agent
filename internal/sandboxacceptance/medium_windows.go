//go:build windows && sandboxacceptance

// Package sandboxacceptance holds narrow Windows-only helpers used solely by
// native acceptance tests. It is excluded from production builds.
package sandboxacceptance

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	mediumRestrictedFlags = 0x1 | 0x4 // DISABLE_MAX_PRIVILEGE | LUA_TOKEN
	mediumIntegritySID    = "S-1-16-8192"
	maxProgramBytes       = int64(64 << 20)
)

var (
	ErrMediumToken  = errors.New("acceptance medium token")
	ErrMediumLaunch = errors.New("acceptance medium launch")
	ErrMediumExit   = errors.New("acceptance medium exit")
	procCreateToken = windows.NewLazySystemDLL("advapi32.dll").NewProc("CreateRestrictedToken")
)

type tokenDefaultDACL struct {
	DACL *windows.ACL
}

// RunMediumTestProgram launches only an absolute local test executable using
// a short-lived Medium token. It accepts no descriptor, script, helper, or
// external command surface. The target starts suspended until its Medium
// state is verified, and all failure paths terminate and bound-wait it.
func RunMediumTestProgram(ctx context.Context, program string, args []string) (result error) {
	if ctx == nil || !validTestProgram(program, args) {
		return ErrMediumLaunch
	}
	fence, expected, err := openProgramDigest(program)
	if err != nil {
		return ErrMediumLaunch
	}
	defer func() {
		if closeErr := fence.Close(); closeErr != nil && result == nil {
			result = ErrMediumLaunch
		}
	}()
	token, err := newMediumToken()
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := token.Close(); closeErr != nil && result == nil {
			result = ErrMediumToken
		}
	}()
	command := encodeArgv(append([]string{program}, args...))
	application, err := windows.UTF16PtrFromString(program)
	if err != nil {
		return ErrMediumLaunch
	}
	line, err := windows.UTF16FromString(command)
	if err != nil || len(line) == 0 {
		return ErrMediumLaunch
	}
	directory, err := windows.UTF16PtrFromString(filepath.Dir(program))
	if err != nil {
		return ErrMediumLaunch
	}
	startup := windows.StartupInfo{Cb: uint32(unsafe.Sizeof(windows.StartupInfo{}))}
	var child windows.ProcessInformation
	if err := windows.CreateProcessAsUser(token, application, &line[0], nil, nil, false, windows.CREATE_SUSPENDED|windows.CREATE_UNICODE_ENVIRONMENT, nil, directory, &startup, &child); err != nil || child.Process == 0 || child.Thread == 0 {
		return ErrMediumLaunch
	}
	finished := false
	defer func() {
		if child.Thread != 0 {
			if closeErr := windows.CloseHandle(child.Thread); closeErr != nil && result == nil {
				result = ErrMediumLaunch
			}
			child.Thread = 0
		}
		if child.Process != 0 {
			if !finished {
				_ = windows.TerminateProcess(child.Process, 1)
				waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = waitProcess(waitCtx, child.Process)
				cancel()
			}
			if closeErr := windows.CloseHandle(child.Process); closeErr != nil && result == nil {
				result = ErrMediumLaunch
			}
			child.Process = 0
		}
	}()
	if !ordinaryMediumTarget(child.Process) {
		return ErrMediumToken
	}
	if _, err := windows.ResumeThread(child.Thread); err != nil {
		return ErrMediumLaunch
	}
	if err := windows.CloseHandle(child.Thread); err != nil {
		return ErrMediumLaunch
	}
	child.Thread = 0
	if err := waitProcess(ctx, child.Process); err != nil {
		return ErrMediumLaunch
	}
	finished = true
	var exitCode uint32
	if err := windows.GetExitCodeProcess(child.Process, &exitCode); err != nil || exitCode != 0 {
		return ErrMediumExit
	}
	if actual, err := digestHeldFile(fence); err != nil || actual != expected {
		return ErrMediumLaunch
	}
	return nil
}

func validTestProgram(program string, args []string) bool {
	if !filepath.IsAbs(program) || filepath.Clean(program) != program || !strings.EqualFold(filepath.Ext(program), ".exe") || strings.HasPrefix(program, `\\`) || strings.HasPrefix(program, `\\?\`) || strings.HasPrefix(program, `\\.\`) || len(program) < 4 || program[1] != ':' || program[2] != '\\' || strings.Contains(program[3:], ":") || strings.ContainsAny(program, "\x00\r\n") {
		return false
	}
	for _, arg := range args {
		if strings.ContainsRune(arg, 0) {
			return false
		}
	}
	return true
}

func newMediumToken() (windows.Token, error) {
	var source windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(), windows.TOKEN_DUPLICATE|windows.TOKEN_QUERY|windows.TOKEN_ASSIGN_PRIMARY|windows.TOKEN_ADJUST_DEFAULT|windows.TOKEN_ADJUST_PRIVILEGES, &source); err != nil {
		return 0, ErrMediumToken
	}
	defer source.Close()
	var elevated uint32
	var returned uint32
	if err := windows.GetTokenInformation(source, windows.TokenElevation, (*byte)(unsafe.Pointer(&elevated)), uint32(unsafe.Sizeof(elevated)), &returned); err != nil || returned != uint32(unsafe.Sizeof(elevated)) || elevated == 0 {
		return 0, ErrMediumToken
	}
	groups, err := source.GetTokenGroups()
	if err != nil || groups == nil {
		return 0, ErrMediumToken
	}
	var disabled windows.SIDAndAttributes
	for _, group := range groups.AllGroups() {
		if group.Sid != nil && group.Sid.String() == "S-1-5-32-544" && group.Attributes&windows.SE_GROUP_ENABLED != 0 && group.Attributes&windows.SE_GROUP_USE_FOR_DENY_ONLY == 0 {
			disabled = windows.SIDAndAttributes{Sid: group.Sid, Attributes: group.Attributes}
			break
		}
	}
	if disabled.Sid == nil {
		return 0, ErrMediumToken
	}
	user, err := source.GetTokenUser()
	if err != nil || user == nil || user.User.Sid == nil || user.User.Sid.String() == "" {
		return 0, ErrMediumToken
	}
	var target windows.Token
	result, _, callErr := procCreateToken.Call(
		uintptr(source), mediumRestrictedFlags,
		1, uintptr(unsafe.Pointer(&disabled)), 0, 0, 0, 0,
		uintptr(unsafe.Pointer(&target)),
	)
	runtime.KeepAlive(disabled)
	if result == 0 || target == 0 || (callErr != nil && callErr != syscall.Errno(0)) {
		return 0, ErrMediumToken
	}
	if err := setMediumIntegrity(target); err != nil {
		_ = target.Close()
		return 0, ErrMediumToken
	}
	if err := installMediumDefaultDACL(target, user.User.Sid.String()); err != nil {
		_ = target.Close()
		return 0, ErrMediumToken
	}
	return target, nil
}

func setMediumIntegrity(token windows.Token) error {
	medium, err := windows.StringToSid(mediumIntegritySID)
	if err != nil || medium == nil {
		return ErrMediumToken
	}
	label := windows.Tokenmandatorylabel{Label: windows.SIDAndAttributes{Sid: medium, Attributes: windows.SE_GROUP_INTEGRITY | windows.SE_GROUP_INTEGRITY_ENABLED}}
	err = windows.SetTokenInformation(token, windows.TokenIntegrityLevel, (*byte)(unsafe.Pointer(&label)), label.Size())
	runtime.KeepAlive(label)
	return err
}

func installMediumDefaultDACL(token windows.Token, user string) error {
	descriptor, err := windows.SecurityDescriptorFromString("D:P(A;;GA;;;" + user + ")(A;;GA;;;SY)")
	if err != nil || descriptor == nil {
		return ErrMediumToken
	}
	dacl, defaulted, err := descriptor.DACL()
	if err != nil || defaulted || dacl == nil {
		return ErrMediumToken
	}
	info := tokenDefaultDACL{DACL: dacl}
	err = windows.SetTokenInformation(token, windows.TokenDefaultDacl, (*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)))
	runtime.KeepAlive(descriptor)
	return err
}

func ordinaryMediumTarget(process windows.Handle) bool {
	var token windows.Token
	if process == 0 || windows.OpenProcessToken(process, windows.TOKEN_QUERY, &token) != nil {
		return false
	}
	defer token.Close()
	var elevation uint32
	var returned uint32
	if windows.GetTokenInformation(token, windows.TokenElevation, (*byte)(unsafe.Pointer(&elevation)), uint32(unsafe.Sizeof(elevation)), &returned) != nil || returned != uint32(unsafe.Sizeof(elevation)) || elevation != 0 {
		return false
	}
	groups, err := token.GetTokenGroups()
	if err != nil || groups == nil {
		return false
	}
	for _, group := range groups.AllGroups() {
		if group.Sid != nil && group.Sid.String() == "S-1-5-32-544" {
			return group.Attributes&windows.SE_GROUP_ENABLED == 0 && group.Attributes&windows.SE_GROUP_USE_FOR_DENY_ONLY != 0
		}
	}
	return false
}

func openProgramDigest(path string) (*os.File, [32]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, [32]byte{}, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() == 0 || info.Size() > maxProgramBytes {
		_ = file.Close()
		return nil, [32]byte{}, ErrMediumLaunch
	}
	digest, err := digestHeldFile(file)
	if err != nil {
		_ = file.Close()
		return nil, [32]byte{}, err
	}
	return file, digest, nil
}

func digestHeldFile(file *os.File) ([32]byte, error) {
	if file == nil {
		return [32]byte{}, ErrMediumLaunch
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return [32]byte{}, err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, io.LimitReader(file, maxProgramBytes+1)); err != nil {
		return [32]byte{}, err
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

func waitProcess(ctx context.Context, process windows.Handle) error {
	if ctx == nil || process == 0 {
		return ErrMediumLaunch
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		status, err := windows.WaitForSingleObject(process, 25)
		if err != nil || status != uint32(258) && status != windows.WAIT_OBJECT_0 {
			return ErrMediumLaunch
		}
		if status == windows.WAIT_OBJECT_0 {
			return nil
		}
	}
}

func encodeArgv(args []string) string {
	encoded := make([]string, 0, len(args))
	for _, arg := range args {
		encoded = append(encoded, syscall.EscapeArg(arg))
	}
	return strings.Join(encoded, " ")
}
