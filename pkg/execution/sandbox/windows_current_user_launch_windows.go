//go:build windows

package sandbox

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"unicode/utf16"
	"unsafe"

	"golang.org/x/sys/windows"
)

// The new backend deliberately uses the same three write-restricting
// principals as Codex's native Windows path: a capability SID attached only
// to this session, the caller's logon SID, and Everyone.  The capability SID
// is an opaque, valid SID rather than an account or a durable principal.
const (
	windowsCurrentUserRestrictedFlags   = 0x1 | 0x4 | 0x8 // DISABLE_MAX_PRIVILEGE | LUA_TOKEN | WRITE_RESTRICTED
	windowsProcThreadAttributeJobList   = 0x0002000d
	windowsDesktopAllAccess             = 0x000f01ff
	windowsUserObjectName               = 2 // UOI_NAME
	windowsUserObjectNameLimit          = 64 << 10
	windowsCurrentUserAdministratorsSID = "S-1-5-32-544"
)

var (
	windowsCurrentUserCreateRestrictedToken = windows.NewLazySystemDLL("advapi32.dll").NewProc("CreateRestrictedToken")
	windowsCurrentUserCreateDesktopW        = windows.NewLazySystemDLL("user32.dll").NewProc("CreateDesktopW")
	windowsCurrentUserCloseDesktop          = windows.NewLazySystemDLL("user32.dll").NewProc("CloseDesktop")
	windowsCurrentUserGetProcessStation     = windows.NewLazySystemDLL("user32.dll").NewProc("GetProcessWindowStation")
	windowsCurrentUserGetUserObjectInfo     = windows.NewLazySystemDLL("user32.dll").NewProc("GetUserObjectInformationW")
)

type windowsTokenDefaultDacl struct {
	Dacl *windows.ACL
}

type windowsCurrentUserToken struct {
	token      windows.Token
	capability string
	logon      string
}

func (value *windowsCurrentUserToken) Close() error {
	if value == nil || value.token == 0 {
		return nil
	}
	err := value.token.Close()
	value.token = 0
	return err
}

func newWindowsCurrentUserCapabilitySID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	parts := [4]uint32{
		binary.LittleEndian.Uint32(bytes[0:4]),
		binary.LittleEndian.Uint32(bytes[4:8]),
		binary.LittleEndian.Uint32(bytes[8:12]),
		binary.LittleEndian.Uint32(bytes[12:16]),
	}
	for index := range parts {
		if parts[index] == 0 {
			parts[index] = uint32(index + 1)
		}
	}
	return fmt.Sprintf("S-1-5-21-%d-%d-%d-%d", parts[0], parts[1], parts[2], parts[3]), nil
}

func windowsCurrentUserIsElevated() (bool, error) {
	var token windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(), windows.TOKEN_QUERY, &token); err != nil {
		return false, err
	}
	defer token.Close()
	var elevated uint32
	var returned uint32
	if err := windows.GetTokenInformation(token, windows.TokenElevation, (*byte)(unsafe.Pointer(&elevated)), uint32(unsafe.Sizeof(elevated)), &returned); err != nil || returned != uint32(unsafe.Sizeof(elevated)) {
		if err != nil {
			return false, err
		}
		return false, errors.New("unexpected token elevation data")
	}
	return elevated != 0, nil
}

func windowsCurrentUserSourceEligible(elevated bool, err error) bool {
	return err == nil && !elevated
}

func newWindowsCurrentUserRestrictedToken(capability string) (*windowsCurrentUserToken, error) {
	if capability == "" {
		return nil, ErrUnavailable
	}
	if elevated, err := windowsCurrentUserIsElevated(); !windowsCurrentUserSourceEligible(elevated, err) {
		return nil, ErrUnavailable
	}
	var source windows.Token
	access := windows.TOKEN_DUPLICATE | windows.TOKEN_QUERY | windows.TOKEN_ASSIGN_PRIMARY | windows.TOKEN_ADJUST_DEFAULT | windows.TOKEN_ADJUST_PRIVILEGES
	if err := windows.OpenProcessToken(windows.CurrentProcess(), uint32(access), &source); err != nil {
		return nil, ErrUnavailable
	}
	defer source.Close()
	logon, err := windowsCurrentUserLogonSID(source)
	if err != nil {
		return nil, ErrUnavailable
	}
	capSID, err := windows.StringToSid(capability)
	if err != nil || capSID == nil {
		return nil, ErrUnavailable
	}
	logonSID, err := windows.StringToSid(logon)
	if err != nil || logonSID == nil {
		return nil, ErrUnavailable
	}
	everyoneSID, err := windows.CreateWellKnownSid(windows.WinWorldSid)
	if err != nil || everyoneSID == nil {
		return nil, ErrUnavailable
	}
	restricting := []windows.SIDAndAttributes{{Sid: capSID}, {Sid: logonSID}, {Sid: everyoneSID}}
	var restricted windows.Token
	result, _, callErr := windowsCurrentUserCreateRestrictedToken.Call(
		uintptr(source), windowsCurrentUserRestrictedFlags,
		0, 0,
		0, 0,
		uintptr(len(restricting)), uintptr(unsafe.Pointer(&restricting[0])),
		uintptr(unsafe.Pointer(&restricted)),
	)
	if result == 0 {
		if callErr != nil && callErr != syscall.Errno(0) {
			return nil, ErrUnavailable
		}
		return nil, ErrUnavailable
	}
	value := &windowsCurrentUserToken{token: restricted, capability: capability, logon: logon}
	if err := windowsInstallCurrentUserDefaultDACL(value.token, []*windows.SID{capSID, logonSID, everyoneSID}); err != nil {
		_ = value.Close()
		return nil, ErrUnavailable
	}
	if err := windowsEnableCurrentUserChangeNotify(value.token); err != nil {
		_ = value.Close()
		return nil, ErrUnavailable
	}
	return value, nil
}

func windowsCurrentUserLogonSID(token windows.Token) (string, error) {
	var needed uint32
	err := windows.GetTokenInformation(token, windows.TokenLogonSid, nil, 0, &needed)
	if !errors.Is(err, windows.ERROR_INSUFFICIENT_BUFFER) || needed < uint32(unsafe.Sizeof(windows.Tokengroups{})) || needed > 64<<10 {
		return "", ErrUnavailable
	}
	buffer := make([]byte, needed)
	if err := windows.GetTokenInformation(token, windows.TokenLogonSid, &buffer[0], uint32(len(buffer)), &needed); err != nil {
		return "", ErrUnavailable
	}
	groups := (*windows.Tokengroups)(unsafe.Pointer(&buffer[0]))
	if groups.GroupCount != 1 {
		return "", ErrUnavailable
	}
	entry := groups.AllGroups()[0]
	if entry.Sid == nil || entry.Attributes&windows.SE_GROUP_LOGON_ID != windows.SE_GROUP_LOGON_ID {
		return "", ErrUnavailable
	}
	value := entry.Sid.String()
	if value == "" {
		return "", ErrUnavailable
	}
	return value, nil
}

func windowsInstallCurrentUserDefaultDACL(token windows.Token, principals []*windows.SID) error {
	if token == 0 || len(principals) != 3 {
		return ErrUnavailable
	}
	entries := make([]string, 0, len(principals))
	for _, principal := range principals {
		if principal == nil || principal.String() == "" {
			return ErrUnavailable
		}
		// This ACL belongs to the token's *future objects* only. It is the
		// Codex-compatible IPC template, not a token or process object ACL.
		entries = append(entries, "(A;;GA;;;"+principal.String()+")")
	}
	descriptor, err := windows.SecurityDescriptorFromString("D:P" + strings.Join(entries, ""))
	if err != nil || descriptor == nil {
		return ErrUnavailable
	}
	dacl, defaulted, err := descriptor.DACL()
	if err != nil || defaulted || dacl == nil {
		return ErrUnavailable
	}
	info := windowsTokenDefaultDacl{Dacl: dacl}
	if err := windows.SetTokenInformation(token, windows.TokenDefaultDacl, (*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		return ErrUnavailable
	}
	runtime.KeepAlive(descriptor)
	return nil
}

func windowsEnableCurrentUserChangeNotify(token windows.Token) error {
	name, err := windows.UTF16PtrFromString("SeChangeNotifyPrivilege")
	if err != nil {
		return err
	}
	var luid windows.LUID
	if err := windows.LookupPrivilegeValue(nil, name, &luid); err != nil {
		return err
	}
	state := windows.Tokenprivileges{PrivilegeCount: 1}
	state.Privileges[0] = windows.LUIDAndAttributes{Luid: luid, Attributes: windows.SE_PRIVILEGE_ENABLED}
	if err := windows.AdjustTokenPrivileges(token, false, &state, 0, nil, nil); err != nil {
		return err
	}
	return nil
}

func windowsGrantCurrentUserCapabilityRoot(root, owner, capability string) error {
	if root == "" || owner == "" || capability == "" {
		return ErrUnavailable
	}
	// The root was verified as this provider's new session directory before
	// this call. OI/CI is deliberately confined to that root and its children;
	// no parent directory is modified or enumerated.
	sddl := "D:P(A;OICI;0x001f01ff;;;" + owner + ")(A;OICI;0x001f01ff;;;" + capability + ")"
	descriptor, err := windows.SecurityDescriptorFromString(sddl)
	if err != nil || descriptor == nil {
		return ErrUnavailable
	}
	dacl, defaulted, err := descriptor.DACL()
	if err != nil || defaulted || dacl == nil {
		return ErrUnavailable
	}
	if err := windows.SetNamedSecurityInfo(root, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, dacl, nil); err != nil {
		return ErrUnavailable
	}
	runtime.KeepAlive(descriptor)
	return nil
}

type windowsCurrentUserDesktop struct {
	handle windows.Handle
	name   string
}

func (desktop *windowsCurrentUserDesktop) Close() error {
	if desktop == nil || desktop.handle == 0 {
		return nil
	}
	result, _, callErr := windowsCurrentUserCloseDesktop.Call(uintptr(desktop.handle))
	desktop.handle = 0
	if result == 0 && callErr != nil && callErr != syscall.Errno(0) {
		return callErr
	}
	if result == 0 {
		return ErrUnavailable
	}
	return nil
}

func newWindowsCurrentUserDesktop(logon, capability string, enabled bool) (*windowsCurrentUserDesktop, error) {
	if !enabled {
		return &windowsCurrentUserDesktop{name: `Winsta0\Default`}, nil
	}
	station, err := windowsCurrentUserWindowStationName()
	if err != nil {
		return nil, ErrUnavailable
	}
	suffix, err := newWindowsCurrentUserDesktopSuffix()
	if err != nil {
		return nil, ErrUnavailable
	}
	name := "HarnessCurrentUserDesktop-" + suffix
	descriptor, err := windows.SecurityDescriptorFromString("D:P(A;;0x000f01ff;;;" + logon + ")(A;;0x000f01ff;;;" + capability + ")")
	if err != nil || descriptor == nil {
		return nil, ErrUnavailable
	}
	attributes := windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: descriptor}
	encoded, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, ErrUnavailable
	}
	handle, _, callErr := windowsCurrentUserCreateDesktopW.Call(uintptr(unsafe.Pointer(encoded)), 0, 0, 0, windowsDesktopAllAccess, uintptr(unsafe.Pointer(&attributes)))
	runtime.KeepAlive(descriptor)
	if handle == 0 {
		if callErr != nil && callErr != syscall.Errno(0) {
			return nil, ErrUnavailable
		}
		return nil, ErrUnavailable
	}
	return &windowsCurrentUserDesktop{handle: windows.Handle(handle), name: station + `\` + name}, nil
}

func newWindowsCurrentUserDesktopSuffix() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", bytes), nil
}

func windowsCurrentUserWindowStationName() (string, error) {
	handle, _, callErr := windowsCurrentUserGetProcessStation.Call()
	if handle == 0 {
		if callErr != nil && callErr != syscall.Errno(0) {
			return "", callErr
		}
		return "", ErrUnavailable
	}
	var needed uint32
	result, _, callErr := windowsCurrentUserGetUserObjectInfo.Call(handle, windowsUserObjectName, 0, 0, uintptr(unsafe.Pointer(&needed)))
	if result != 0 || !errors.Is(callErr, windows.ERROR_INSUFFICIENT_BUFFER) || needed < 2 || needed > windowsUserObjectNameLimit || needed%2 != 0 {
		return "", ErrUnavailable
	}
	buffer := make([]uint16, needed/2)
	result, _, callErr = windowsCurrentUserGetUserObjectInfo.Call(handle, windowsUserObjectName, uintptr(unsafe.Pointer(&buffer[0])), uintptr(needed), uintptr(unsafe.Pointer(&needed)))
	if result == 0 {
		if callErr != nil && callErr != syscall.Errno(0) {
			return "", callErr
		}
		return "", ErrUnavailable
	}
	name := windows.UTF16ToString(buffer)
	if name == "" || strings.ContainsAny(name, `\/:`+"\x00\r\n") {
		return "", ErrUnavailable
	}
	return name, nil
}

func windowsCurrentUserEnvironmentBlock(environment []string, offline bool) ([]uint16, error) {
	values := make(map[string]string, len(environment)+6)
	for _, entry := range environment {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || key == "" || value == "" || strings.ContainsAny(key, "=\x00\r\n") || strings.ContainsAny(value, "\x00\r\n") {
			return nil, ErrUnavailable
		}
		upper := strings.ToUpper(key)
		if _, exists := values[upper]; exists {
			return nil, ErrUnavailable
		}
		values[upper] = key + "=" + value
	}
	for _, required := range []string{"SYSTEMROOT", "COMSPEC", "PATH", "HARNESS_WORKSPACE", "HARNESS_ARTIFACTS", "HOME", "USERPROFILE", "TEMP", "TMP"} {
		if _, exists := values[required]; !exists {
			return nil, ErrUnavailable
		}
	}
	if offline {
		for _, entry := range []string{
			"HTTP_PROXY=http://127.0.0.1:9",
			"HTTPS_PROXY=http://127.0.0.1:9",
			"ALL_PROXY=http://127.0.0.1:9",
			"CARGO_NET_OFFLINE=true",
			"npm_config_offline=true",
		} {
			key, _, _ := strings.Cut(entry, "=")
			values[strings.ToUpper(key)] = entry
		}
	}
	entries := make([]string, 0, len(values))
	for _, entry := range values {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(left, right int) bool { return strings.ToUpper(entries[left]) < strings.ToUpper(entries[right]) })
	block := utf16.Encode([]rune(strings.Join(entries, "\x00") + "\x00\x00"))
	if len(block) < 2 || block[len(block)-1] != 0 || block[len(block)-2] != 0 {
		return nil, ErrUnavailable
	}
	return block, nil
}
