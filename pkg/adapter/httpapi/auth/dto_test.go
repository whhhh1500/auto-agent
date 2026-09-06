package auth

import "testing"

func TestLoginRequestCanonicalizesLegacyIdentifier(t *testing.T) {
	accountID, err := (LoginRequest{Email: " legacy@example.test "}).AccountID()
	if err != nil || accountID != "legacy@example.test" {
		t.Fatalf("account=%q err=%v", accountID, err)
	}
	if _, err := (LoginRequest{Account: "alice", Email: "other@example.test"}).AccountID(); err == nil {
		t.Fatal("conflicting identifiers were accepted")
	}
}

func TestActivationRequestRequiresMatchingConfirmation(t *testing.T) {
	if err := (ActivationRequest{Password: "one", Confirm: "two"}).Validate(); err == nil {
		t.Fatal("mismatched confirmation was accepted")
	}
}
