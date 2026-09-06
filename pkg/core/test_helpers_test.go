package core

import (
	"context"
)

func testScopes() (ScopePath, ScopePath, ScopePath, ScopePath) {
	global := MustScopePath(ScopeRef{Kind: ScopeGlobal, ID: "global"})
	product, _ := global.Child(ScopeRef{Kind: ScopeProduct, ID: "product"})
	tenant, _ := product.Child(ScopeRef{Kind: ScopeTenant, ID: "tenant-a"})
	user, _ := tenant.Child(ScopeRef{Kind: ScopeUser, ID: "user-a"})
	return global, product, tenant, user
}

func testPrincipal(user ScopePath) Principal {
	return Principal{
		SubjectID: "user-a", TenantID: "tenant-a", Scope: user,
		Grants: NewPermissionSet(PermRead, PermWrite),
	}
}

type staticTool struct {
	manifest CapabilityManifest
	content  string
	wait     bool
}

func (t staticTool) Manifest() CapabilityManifest { return t.manifest }

func (t staticTool) Execute(ctx context.Context, _ CapabilityRequest) (CapabilityResult, error) {
	if t.wait {
		<-ctx.Done()
		return CapabilityResult{}, ctx.Err()
	}
	return CapabilityResult{Content: t.content, OK: true}, nil
}

func toolManifest(id, version string) CapabilityManifest {
	return CapabilityManifest{
		ID: id, Version: version, Name: id, Kind: KindTool, Contract: "harness.tool/v1",
		RequiredPermissions: []Permission{PermRead}, Tool: &ToolExposure{},
	}
}
