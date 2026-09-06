package storageconfig

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"unicode"
)

func (e ResolutionEvidence) Validate() error {
	if !e.Kind.Valid() || !e.Source.Valid() || !e.Status.Valid() || !e.ErrorCode.Valid() {
		return fmt.Errorf("%w: resolution evidence enum is invalid", ErrInvalidConfiguration)
	}
	if e.DesiredType != "" && !e.DesiredType.ValidFor(e.Kind) {
		return fmt.Errorf("%w: resolution desired backend is invalid", ErrInvalidConfiguration)
	}
	if e.ActiveType != "" && !e.ActiveType.ValidFor(e.Kind) {
		return fmt.Errorf("%w: resolution active backend is invalid", ErrInvalidConfiguration)
	}
	if e.DesiredRevision != "" && !validRevision(e.DesiredRevision) {
		return fmt.Errorf("%w: resolution desired revision is invalid", ErrInvalidConfiguration)
	}
	if e.ActiveRevision != "" && !validRevision(e.ActiveRevision) {
		return fmt.Errorf("%w: resolution active revision is invalid", ErrInvalidConfiguration)
	}
	if e.RestartPending != (e.Status == StatusRestartPending) || (e.RestartPending && e.Kind != KindSessions) || (e.Status == StatusMigrationPending && e.Kind != KindResources) {
		return fmt.Errorf("%w: resolution restart pending state is invalid", ErrInvalidConfiguration)
	}
	if (e.Status == StatusInvalid || e.Status == StatusInactive) && e.ErrorCode == "" {
		return fmt.Errorf("%w: resolution error code is required", ErrInvalidConfiguration)
	}
	if e.Status == StatusApplyFailed && e.ErrorCode != ErrorCodeApplyFailed {
		return fmt.Errorf("%w: apply-failed evidence requires the apply-failed error code", ErrInvalidConfiguration)
	}
	return nil
}

// Validate validates a typed configuration without reading environment or
// contacting a backend.
func Validate(kind Kind, configuration StoredConfiguration) error {
	if !kind.Valid() {
		return fmt.Errorf("%w: %q", ErrInvalidKind, kind)
	}
	if configuration.Status != "" && configuration.Status != StatusActive && configuration.Status != StatusInactive {
		return fmt.Errorf("%w: desired status %q is not allowed", ErrInvalidConfiguration, configuration.Status)
	}
	if configuration.Backend != "" && !configuration.Backend.ValidFor(kind) {
		return fmt.Errorf("%w: backend %q is not valid for %s", ErrInvalidConfiguration, configuration.Backend, kind)
	}
	if kind == KindResources && configuration.DisableConditionalWrites {
		return fmt.Errorf("%w: disable_conditional_writes is only valid for sessions", ErrInvalidConfiguration)
	}
	for _, field := range []struct {
		name  string
		value string
		limit int
	}{
		{"path", configuration.Path, maxPathBytes}, {"endpoint", configuration.Endpoint, maxEndpointBytes},
		{"region", configuration.Region, maxRegionBytes}, {"bucket", configuration.Bucket, maxBucketBytes},
		{"access_key", configuration.AccessKey, maxAccessKeyBytes}, {"secret_key", configuration.SecretKey, maxSecretKeyBytes},
		{"desired_revision", configuration.DesiredRevision, maxRevisionBytes},
	} {
		if err := validateText(field.name, field.value, field.limit); err != nil {
			return err
		}
	}
	if configuration.Status == StatusInactive {
		return nil
	}
	if configuration.Backend == "" {
		return fmt.Errorf("%w: backend %q is not valid for %s", ErrInvalidConfiguration, configuration.Backend, kind)
	}
	switch configuration.Backend {
	case BackendEmbedded:
		return nil
	case BackendFile:
		if strings.TrimSpace(configuration.Path) == "" {
			return fmt.Errorf("%w: file sessions require a path", ErrInvalidConfiguration)
		}
	case BackendS3:
		if configuration.SecretKey == "••••" {
			return fmt.Errorf("%w: legacy secret mask is not a credential", ErrInvalidConfiguration)
		}
		if strings.TrimSpace(configuration.Endpoint) == "" || strings.TrimSpace(configuration.Bucket) == "" || strings.TrimSpace(configuration.AccessKey) == "" || strings.TrimSpace(configuration.SecretKey) == "" {
			return fmt.Errorf("%w: s3 requires endpoint, bucket, access_key and secret_key", ErrInvalidConfiguration)
		}
		parsed, err := url.Parse(configuration.Endpoint)
		if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("%w: s3 endpoint must include an http or https scheme", ErrInvalidConfiguration)
		}
	}
	return nil
}

const (
	maxPathBytes      = 4096
	maxEndpointBytes  = 4096
	maxRegionBytes    = 128
	maxBucketBytes    = 256
	maxAccessKeyBytes = 4096
	maxSecretKeyBytes = 4096
	maxRevisionBytes  = 128
)

// Normalize validates a stored configuration and assigns missing source,
// status, and the stable compatibility revision used by old rows. It never
// hashes configuration data. New writes should use PrepareCreate.
func Normalize(kind Kind, configuration StoredConfiguration) (StoredConfiguration, error) {
	if configuration.Source == "" {
		configuration.Source = SourceDB
	}
	if configuration.Status == "" {
		configuration.Status = StatusActive
	}
	if !configuration.Source.Valid() {
		return configuration, fmt.Errorf("%w: unknown source %q", ErrInvalidConfiguration, configuration.Source)
	}
	if err := Validate(kind, configuration); err != nil {
		return configuration, err
	}
	if configuration.DesiredRevision == "" {
		configuration.DesiredRevision = legacyRevision
	} else if !validRevision(configuration.DesiredRevision) {
		return configuration, fmt.Errorf("%w: desired revision is invalid", ErrInvalidConfiguration)
	}
	return configuration, nil
}

const legacyRevision = "rev_legacy"

// NewDesiredRevision returns a random opaque revision for a newly created
// configuration. The value contains no configuration or credential material.
func NewDesiredRevision() (string, error) { return newOpaqueRevision() }

// PrepareCreate validates a new configuration and assigns a random revision
// when the caller has not supplied one. This is the path for env imports,
// bootstrap records, and future write/application services.
func PrepareCreate(kind Kind, configuration StoredConfiguration) (StoredConfiguration, error) {
	if configuration.DesiredRevision == "" {
		revision, err := NewDesiredRevision()
		if err != nil {
			return configuration, err
		}
		configuration.DesiredRevision = revision
	}
	return Normalize(kind, configuration)
}

func validateText(name, value string, limit int) error {
	if len(value) > limit {
		return fmt.Errorf("%w: %s exceeds %d bytes", ErrInvalidConfiguration, name, limit)
	}
	if strings.IndexByte(value, 0) >= 0 || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("%w: %s contains a control character", ErrInvalidConfiguration, name)
	}
	return nil
}

func newOpaqueRevision() (string, error) {
	bytesValue := make([]byte, 18)
	if _, err := rand.Read(bytesValue); err != nil {
		return "", fmt.Errorf("generate storage configuration revision: %w", err)
	}
	return "rev_" + base64.RawURLEncoding.EncodeToString(bytesValue), nil
}

func validRevision(value string) bool {
	return len(value) > 4 && len(value) <= maxRevisionBytes && strings.HasPrefix(value, "rev_") && strings.IndexFunc(value[4:], func(r rune) bool {
		return !(r >= 'A' && r <= 'Z') && !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9') && r != '-' && r != '_'
	}) < 0
}

func normalizeSource(source ConfigSource) ConfigSource {
	if source == "" {
		return SourceDB
	}
	return source
}

func evidenceSource(source ConfigSource) ConfigSource {
	source = normalizeSource(source)
	if !source.Valid() {
		return SourceDB
	}
	return source
}
