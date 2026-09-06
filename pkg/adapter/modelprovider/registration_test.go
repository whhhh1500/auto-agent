package modelprovider

import (
	"errors"
	"strings"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/runtime"
)

func TestRegistrationExtension(t *testing.T) {
	extension, err := (Registration{ID: "acme", Version: runtime.Version{Major: 1}, Priority: 7}).Extension()
	if err != nil {
		t.Fatal(err)
	}
	if extension.ID != "model-provider/acme" || extension.Semantic != runtime.SemanticCollection || extension.Contract != Contract || extension.ResourceKey != "acme" || extension.Priority != 7 {
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
		{ID: "acme", Version: runtime.Version{Major: -1}},
	} {
		if _, err := registration.Extension(); err == nil {
			t.Fatalf("registration %#v error=%v", registration, err)
		}
		if registration.ID != "" {
			if _, err := registration.Extension(); !errors.Is(err, runtime.ErrInvalidExtension) {
				t.Fatalf("registration %#v did not expose invalid extension: %v", registration, err)
			}
		}
	}
}

func TestDuplicateProvidersFailClosedAtSnapshot(t *testing.T) {
	first, err := (Registration{ID: "acme", Version: runtime.Version{Major: 1}, Priority: 1}).Extension()
	if err != nil {
		t.Fatal(err)
	}
	second, err := (Registration{ID: "acme", Version: runtime.Version{Major: 2}, Priority: 2}).Extension()
	if err != nil {
		t.Fatal(err)
	}
	_, err = runtime.BuildSnapshot([]runtime.ModuleManifest{
		{ID: "provider-one", Version: runtime.Version{Major: 1}, Provides: []runtime.ProvidedExtension{first}},
		{ID: "provider-two", Version: runtime.Version{Major: 1}, Provides: []runtime.ProvidedExtension{second}},
	})
	if !errors.Is(err, runtime.ErrSemanticConflict) {
		t.Fatalf("duplicate provider snapshot error=%v", err)
	}
}
