package identity

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRoleAndStatusParsing(t *testing.T) {
	for _, role := range RoleValues() {
		parsed, err := ParseRole(string(role))
		if err != nil || parsed != role || !parsed.Valid() {
			t.Fatalf("role %q parsed as %q, err=%v", role, parsed, err)
		}
	}
	if _, err := ParseRole("operator"); err == nil {
		t.Fatal("unknown role was accepted")
	}
	for _, status := range StatusValues() {
		parsed, err := ParseStatus(string(status))
		if err != nil || parsed != status || !parsed.Valid() {
			t.Fatalf("status %q parsed as %q, err=%v", status, parsed, err)
		}
	}
	if _, err := ParseStatus("deleted"); err == nil {
		t.Fatal("unknown status was accepted")
	}
}

func TestServiceFailsClosedWhenThrottleFails(t *testing.T) {
	credentials := &fakeCredentials{account: Account{ID: "alice", Role: RoleUser, Status: StatusActive}}
	service, err := NewService(credentials, &fakeThrottle{checkErr: errors.New("throttle unavailable")}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Login(context.Background(), LoginCommand{AccountID: "alice", Password: "password-123"}); err == nil {
		t.Fatal("login succeeded while throttle was unavailable")
	}
	if credentials.authenticateCalls != 0 {
		t.Fatal("credential adapter was called after throttle failure")
	}
}

func TestServiceRecordsFailureAndDoesNotIssueToken(t *testing.T) {
	credentials := &fakeCredentials{authenticateErr: ErrInvalidCredentials}
	throttle := &fakeThrottle{}
	service, err := NewService(credentials, throttle, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Login(context.Background(), LoginCommand{AccountID: "alice", Password: "password-123"})
	if !errors.Is(err, ErrInvalidCredentials) || throttle.failureCalls != 1 || credentials.issueCalls != 0 {
		t.Fatalf("result=%v failures=%d tokenIssues=%d", err, throttle.failureCalls, credentials.issueCalls)
	}
}

func TestServiceDoesNotIssueTokenWhenSuccessThrottleRecordFails(t *testing.T) {
	credentials := &fakeCredentials{account: Account{ID: "alice", Role: RoleUser, Status: StatusActive}}
	throttle := &fakeThrottle{successErr: errors.New("throttle unavailable")}
	service, err := NewService(credentials, throttle, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Login(context.Background(), LoginCommand{AccountID: "alice", Password: "password-123"}); err == nil {
		t.Fatal("login succeeded after throttle success-record failure")
	}
	if credentials.issueCalls != 0 {
		t.Fatal("token was issued after throttle success-record failure")
	}
}

func TestServiceClearsThrottleUsingLoginAttemptIdentifier(t *testing.T) {
	credentials := &fakeCredentials{account: Account{ID: "account-id", Role: RoleUser, Status: StatusActive}}
	throttle := &fakeThrottle{}
	service, err := NewService(credentials, throttle, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Login(context.Background(), LoginCommand{AccountID: "legacy@example.test", Password: "password-123"}); err != nil {
		t.Fatal(err)
	}
	if throttle.successAccountID != "legacy@example.test" {
		t.Fatalf("success throttle key=%q; want login attempt identifier", throttle.successAccountID)
	}
}

func TestServiceActivationRequiresActiveFinalState(t *testing.T) {
	credentials := &fakeCredentials{activation: ActivationResult{Token: "token", Account: Account{
		ID: "alice", Status: StatusPendingActivation, MustChangePassword: true,
	}}}
	service, err := NewService(credentials, &fakeThrottle{}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Activate(context.Background(), ActivateCommand{AccountID: "alice", Password: "new-secure-password-456"}); err == nil {
		t.Fatal("activation accepted a non-active final state")
	}
}

type fakeCredentials struct {
	account           Account
	authenticateErr   error
	activation        ActivationResult
	authenticateCalls int
	issueCalls        int
}

func (fake *fakeCredentials) Authenticate(_ context.Context, _ LoginCommand) (Account, error) {
	fake.authenticateCalls++
	return fake.account, fake.authenticateErr
}

func (fake *fakeCredentials) IssueToken(_ context.Context, _ string, _ time.Duration) (string, error) {
	fake.issueCalls++
	return "token", nil
}

func (fake *fakeCredentials) ActivateInitial(_ context.Context, _ ActivateCommand, _ time.Duration) (ActivationResult, error) {
	return fake.activation, nil
}

type fakeThrottle struct {
	checkErr         error
	successErr       error
	failureCalls     int
	successAccountID string
}

func (fake *fakeThrottle) Check(_ context.Context, _ string) error { return fake.checkErr }
func (fake *fakeThrottle) RecordFailure(_ context.Context, _ string) error {
	fake.failureCalls++
	return nil
}
func (fake *fakeThrottle) RecordSuccess(_ context.Context, accountID string) error {
	fake.successAccountID = accountID
	return fake.successErr
}
