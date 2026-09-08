// Package sandbox owns the non-secret HTTP presentation models for sandbox
// provider discovery. It exposes provider metadata and sanitized live probe
// facts only; it never returns provider instances, host paths, mounts, or
// configuration.
package sandbox

import executionsandbox "github.com/whhhh1500/auto-agent/pkg/execution/sandbox"

// ProbeStatus is the bounded, sanitized readiness state shown to operators.
type ProbeStatus string

const (
	StatusReady          ProbeStatus = "ready"
	StatusNotInstalled   ProbeStatus = "not_installed"
	StatusRepairRequired ProbeStatus = "repair_required"
	StatusUnavailable    ProbeStatus = "unavailable"
)

// AssuranceView is the stable JSON form of a provider assurance declaration.
type AssuranceView struct {
	Level            uint8 `json:"level"`
	SharedKernel     bool  `json:"shared_kernel"`
	DedicatedKernel  bool  `json:"dedicated_kernel"`
	NetworkIsolation bool  `json:"network_isolation"`
}

// ProbeView contains bounded, live facts collected through the Registry.
// UnavailableCause is always registry-sanitized before this DTO is created.
type ProbeView struct {
	Available         bool          `json:"available"`
	Status            ProbeStatus   `json:"status"`
	Assurance         AssuranceView `json:"assurance"`
	Network           string        `json:"network"`
	SupportedNetworks []string      `json:"supported_networks"`
	LimitsEnforced    bool          `json:"limits_enforced"`
	MountsEnforced    bool          `json:"mounts_enforced"`
	UnavailableCause  string        `json:"unavailable_cause,omitempty"`
}

// ProviderView combines immutable registered metadata with one current Probe.
type ProviderView struct {
	ID                     string        `json:"id"`
	Version                string        `json:"version"`
	ImplementationRevision string        `json:"implementation_revision"`
	AdvertisedAssurance    AssuranceView `json:"advertised_assurance"`
	Probe                  ProbeView     `json:"probe"`
}

// ProvidersResponse is the bounded platform-administrator discovery response.
type ProvidersResponse struct {
	Providers []ProviderView `json:"providers"`
}

// View maps only stable metadata and the Registry's already-sanitized Probe
// report. A probe error deliberately becomes one generic unavailable result.
func View(metadata executionsandbox.Metadata, report executionsandbox.AssuranceReport, probeErr error) ProviderView {
	view := ProviderView{
		ID: string(metadata.ID), Version: metadata.Version, ImplementationRevision: metadata.ImplementationRevision,
		AdvertisedAssurance: assuranceView(metadata.Assurance),
	}
	if probeErr != nil || !report.Available {
		view.Probe = ProbeView{Available: false, Status: probeStatus(report, probeErr), Assurance: assuranceView(report.Actual), SupportedNetworks: []string{}, UnavailableCause: "sandbox provider unavailable"}
		return view
	}
	view.Probe = ProbeView{
		Available: true, Status: probeStatus(report, nil), Assurance: assuranceView(report.Actual), Network: string(report.Network),
		SupportedNetworks: networkViews(report.SupportedNetworks), LimitsEnforced: report.LimitsEnforced,
		MountsEnforced: report.MountsEnforced,
	}
	return view
}

func probeStatus(report executionsandbox.AssuranceReport, probeErr error) ProbeStatus {
	if probeErr != nil {
		return StatusUnavailable
	}
	status := ProbeStatus(report.Readiness)
	switch status {
	case StatusReady, StatusNotInstalled, StatusRepairRequired, StatusUnavailable:
		return status
	}
	if report.Available {
		return StatusReady
	}
	return StatusUnavailable
}

func assuranceView(assurance executionsandbox.Assurance) AssuranceView {
	return AssuranceView{
		Level: uint8(assurance.Level), SharedKernel: assurance.SharedKernel,
		DedicatedKernel: assurance.DedicatedKernel, NetworkIsolation: assurance.NetworkIsolation,
	}
}

func networkViews(networks []executionsandbox.NetworkPolicy) []string {
	if len(networks) == 0 {
		return []string{}
	}
	result := make([]string, 0, len(networks))
	for _, network := range networks {
		result = append(result, string(network))
	}
	return result
}
