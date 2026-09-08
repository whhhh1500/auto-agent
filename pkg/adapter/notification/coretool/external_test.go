package coretool_test

import (
	"testing"

	"github.com/whhhh1500/auto-agent/pkg/adapter/notification/coretool"
	"github.com/whhhh1500/auto-agent/pkg/app/notification"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

func TestPublicNotificationCoretoolSurfaceCompiles(t *testing.T) {
	registry, err := notification.NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := coretool.New(registry)
	if err != nil || len(capabilities) != 2 {
		t.Fatalf("capabilities=%d err=%v", len(capabilities), err)
	}
	for _, capability := range capabilities {
		var _ core.Capability = capability
	}
	directory, err := notification.NewSnapshotDirectory(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	withDirectory, err := coretool.NewWithDirectory(registry, directory)
	if err != nil || len(withDirectory) != 3 {
		t.Fatalf("directory capability surface failed: len=%d err=%v", len(withDirectory), err)
	}
}
