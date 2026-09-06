package core

import "testing"

func TestPolicyRegistryRejectsBindingOverflow(t *testing.T) {
	_, product, _, _ := testScopes()
	registry := NewPolicyRegistry()
	registry.maxBindings = 2
	unmount, err := registry.Mount(PolicyLayer{Scope: product})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Mount(PolicyLayer{Scope: product}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Mount(PolicyLayer{Scope: product}); err == nil {
		t.Fatal("policy registry overflow was accepted")
	}
	unmount()
	if _, err := registry.Mount(PolicyLayer{Scope: product}); err != nil {
		t.Fatalf("unmount did not free a registry slot: %v", err)
	}
}
