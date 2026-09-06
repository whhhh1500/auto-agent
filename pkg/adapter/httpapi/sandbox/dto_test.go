package sandbox

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	executionsandbox "github.com/cc-auto-agent/harness-core/pkg/execution/sandbox"
)

func TestViewKeepsOnlySanitizedDiscoveryFacts(t *testing.T) {
	metadata := executionsandbox.Metadata{
		ID: "isolated", Version: "1", ImplementationRevision: "revision-1",
		Assurance: executionsandbox.Assurance{Level: executionsandbox.AssuranceProcess, SharedKernel: true},
	}
	view := View(metadata, executionsandbox.AssuranceReport{Readiness: executionsandbox.ProviderReadinessNotInstalled, UnavailableCause: "secret=/private/mount"}, nil)
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	if view.Probe.Available || view.Probe.Status != StatusNotInstalled || view.Probe.UnavailableCause != "sandbox provider unavailable" {
		t.Fatalf("unavailable view=%+v", view)
	}
	for _, forbidden := range []string{"secret", "private", "/sandbox/path"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("sanitized view leaked %q: %s", forbidden, text)
		}
	}
}

func TestViewCopiesSupportedNetworks(t *testing.T) {
	report := executionsandbox.AssuranceReport{
		Available: true, Actual: executionsandbox.Assurance{Level: executionsandbox.AssuranceProcess, SharedKernel: true},
		Network: executionsandbox.NetworkHost, SupportedNetworks: []executionsandbox.NetworkPolicy{executionsandbox.NetworkHost, executionsandbox.NetworkDisabled},
		LimitsEnforced: true, MountsEnforced: true,
	}
	view := View(executionsandbox.Metadata{ID: "isolated", Version: "1", ImplementationRevision: "revision-1", Assurance: report.Actual}, report, nil)
	report.SupportedNetworks[0] = executionsandbox.NetworkIsolated
	if got, want := view.Probe.SupportedNetworks[0], "host"; got != want {
		t.Fatalf("supported networks aliased report: got %q want %q", got, want)
	}
	if view.Probe.Status != StatusReady {
		t.Fatalf("ready status=%q", view.Probe.Status)
	}
}

func TestViewMapsReadinessAndProbeErrorsWithoutLeakingDetails(t *testing.T) {
	metadata := executionsandbox.Metadata{ID: "isolated", Version: "1"}
	for _, test := range []struct {
		name   string
		report executionsandbox.AssuranceReport
		err    error
		want   ProbeStatus
	}{
		{name: "repair readiness", report: executionsandbox.AssuranceReport{Readiness: executionsandbox.ProviderReadinessRepairRequired}, want: StatusRepairRequired},
		{name: "probe failure", report: executionsandbox.AssuranceReport{Readiness: executionsandbox.ProviderReadinessRepairRequired}, err: errors.New("transient"), want: StatusUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			view := View(metadata, test.report, test.err)
			if view.Probe.Status != test.want {
				t.Fatalf("status=%q want %q", view.Probe.Status, test.want)
			}
		})
	}
}
