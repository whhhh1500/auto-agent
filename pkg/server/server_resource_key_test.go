package server

import "testing"

func TestTenantResourceKeyRejectsTraversal(t *testing.T) {
	if _, err := tenantResourceKey("acme", "../escape"); err == nil {
		t.Fatal("parent resource key was accepted")
	}
	if _, err := tenantResourceKey("acme/../x", "docs/a"); err == nil {
		t.Fatal("parent tenant was accepted")
	}
	if _, err := tenantResourcePrefix("acme", "../"); err == nil {
		t.Fatal("parent resource prefix was accepted")
	}
	key, err := tenantResourceKey("acme", "docs/readme.md")
	if err != nil || key != "tenants/acme/resources/docs/readme.md" {
		t.Fatalf("valid resource key = %q err=%v", key, err)
	}
	prefix, err := tenantResourcePrefix("acme", "docs/")
	if err != nil || prefix != "tenants/acme/resources/docs/" {
		t.Fatalf("valid resource prefix = %q err=%v", prefix, err)
	}
}
