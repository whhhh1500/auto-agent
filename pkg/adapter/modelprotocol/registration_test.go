package modelprotocol

import (
	"errors"
	"strings"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/runtime"
)

func TestRegistrationExtension(t *testing.T) {
	extension, err := (Registration{ID: "openai-chat", Version: runtime.Version{Major: 1}, Priority: 3}).Extension()
	if err != nil {
		t.Fatal(err)
	}
	if extension.ID != "model-protocol/openai-chat" || extension.Semantic != runtime.SemanticCollection || extension.Contract != Contract || extension.ResourceKey != "openai-chat" || extension.Priority != 3 {
		t.Fatalf("unexpected extension: %#v", extension)
	}
	if err := runtime.ValidateExtension(extension); err != nil {
		t.Fatal(err)
	}
}

func TestRegistrationRejectsInvalidInput(t *testing.T) {
	for _, registration := range []Registration{
		{},
		{ID: " bad", Version: runtime.Version{Major: 1}},
		{ID: "../escape", Version: runtime.Version{Major: 1}},
		{ID: strings.Repeat("x", 128), Version: runtime.Version{Major: 1}},
		{ID: "openai-chat", Version: runtime.Version{Major: -1}},
	} {
		_, err := registration.Extension()
		if err == nil {
			t.Fatalf("registration %#v unexpectedly accepted", registration)
		}
	}
}

func TestDuplicateProtocolsFailClosedAtSnapshot(t *testing.T) {
	first, err := (Registration{ID: "shared", Version: runtime.Version{Major: 1}, Priority: 1}).Extension()
	if err != nil {
		t.Fatal(err)
	}
	second, err := (Registration{ID: "shared", Version: runtime.Version{Major: 2}, Priority: 2}).Extension()
	if err != nil {
		t.Fatal(err)
	}
	_, err = runtime.BuildSnapshot([]runtime.ModuleManifest{
		{ID: "protocol-one", Version: runtime.Version{Major: 1}, Provides: []runtime.ProvidedExtension{first}},
		{ID: "protocol-two", Version: runtime.Version{Major: 1}, Provides: []runtime.ProvidedExtension{second}},
	})
	if !errors.Is(err, runtime.ErrSemanticConflict) {
		t.Fatalf("duplicate protocol snapshot error=%v", err)
	}
}
