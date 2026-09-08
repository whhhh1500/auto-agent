//go:build windows && sandboxacceptance

package sandbox

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/internal/sandboxacceptance"
)

// TestAcceptanceCurrentUserLaunchFromMedium starts only this test binary with
// the colocated acceptance Medium launcher. Production has no import of this
// package or of the retired helper/protocol tree.
func TestAcceptanceCurrentUserLaunchFromMedium(t *testing.T) {
	program, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	elevated, err := windowsCurrentUserIsElevated()
	if err != nil {
		t.Fatal(err)
	}
	if !elevated {
		t.Run("native launch", TestNativeWindowsCurrentUserLaunchUsesCapabilityRoot)
		t.Run("native close failure", TestNativeWindowsCurrentUserExecuteCleansJobAfterChildCloseFailure)
		t.Run("native sequential public sessions", TestNativeWindowsCurrentUserSequentialPublicSessions)
		t.Run("native timeout result", TestNativeWindowsCurrentUserTimeoutReturnsTimedResult)
		t.Run("native cancellation result", TestNativeWindowsCurrentUserCancellationRetainsResult)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = sandboxacceptance.RunMediumTestProgram(ctx, program, []string{"-test.run=^(TestNativeWindowsCurrentUserLaunchUsesCapabilityRoot|TestNativeWindowsCurrentUserExecuteCleansJobAfterChildCloseFailure|TestNativeWindowsCurrentUserSequentialPublicSessions|TestNativeWindowsCurrentUserTimeoutReturnsTimedResult|TestNativeWindowsCurrentUserCancellationRetainsResult)$", "-test.v"})
	if err != nil {
		t.Fatalf("current-user Medium launch class=%s", windowsCurrentUserAcceptanceMediumClass(err))
	}
}

func windowsCurrentUserAcceptanceMediumClass(err error) string {
	switch {
	case errors.Is(err, sandboxacceptance.ErrMediumToken):
		return "token"
	case errors.Is(err, sandboxacceptance.ErrMediumExit):
		return "exit"
	default:
		return "launch"
	}
}
