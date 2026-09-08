// Package targetresolver bridges the provider-neutral notification target
// service to the private webhook resolver and directory seams.
package targetresolver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/whhhh1500/auto-agent/pkg/adapter/notification/webhook"
	"github.com/whhhh1500/auto-agent/pkg/app/notification"
)

const maxConfigurationBytes = notification.MaxTargetConfigurationBytes

// TargetService is the minimal application seam. List never returns private
// configuration; ResolveConfig is only used by this provider adapter.
type TargetService interface {
	List(context.Context, string) ([]notification.TargetDescriptor, error)
	ResolveConfig(context.Context, string, notification.TargetRef, notification.ChannelRef) ([]byte, error)
}

type Resolver struct {
	service TargetService
	channel notification.ChannelRef
}

// ConfigurationValidator is the provider-owned validation seam usable before
// the service is constructed. It has no repository or network dependency.
type ConfigurationValidator struct{}

func NewConfigurationValidator() *ConfigurationValidator { return &ConfigurationValidator{} }

var _ webhook.TargetResolver = (*Resolver)(nil)
var _ notification.TargetDirectory = (*Resolver)(nil)
var _ notification.ConfigurationValidator = (*Resolver)(nil)
var _ notification.ConfigurationValidator = (*ConfigurationValidator)(nil)

func New(service TargetService) (*Resolver, error) {
	if service == nil {
		return nil, webhook.ErrTargetResolution
	}
	return &Resolver{
		service: service,
		channel: notification.ChannelRef{ID: webhook.ChannelID, Version: webhook.ChannelVersion},
	}, nil
}

// Channel identifies the provider configuration validator's exact channel.
func (resolver *Resolver) Channel() notification.ChannelRef {
	if resolver == nil {
		return notification.ChannelRef{}
	}
	return resolver.channel
}

// ValidateConfiguration validates opaque webhook configuration before it is
// persisted. Secrets are cleared immediately after parsing and never escape
// this provider boundary.
func (resolver *Resolver) ValidateConfiguration(payload []byte) error {
	if resolver == nil {
		return notification.ErrInvalidTargetConfiguration
	}
	parsed, err := decode(payload)
	clearBytes(parsed.Secret)
	if err != nil {
		return notification.ErrInvalidTargetConfiguration
	}
	return nil
}

func (*ConfigurationValidator) Channel() notification.ChannelRef {
	return notification.ChannelRef{ID: webhook.ChannelID, Version: webhook.ChannelVersion}
}

func (*ConfigurationValidator) ValidateConfiguration(payload []byte) error {
	parsed, err := decode(payload)
	clearBytes(parsed.Secret)
	if err != nil {
		return notification.ErrInvalidTargetConfiguration
	}
	return nil
}

func (resolver *Resolver) Resolve(ctx context.Context, tenantID string, target notification.TargetRef) (result webhook.ResolvedTarget, err error) {
	if ctx == nil {
		return webhook.ResolvedTarget{}, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return webhook.ResolvedTarget{}, err
	}
	if err := notification.ValidateTenantID(tenantID); err != nil {
		return webhook.ResolvedTarget{}, webhook.ErrTargetResolution
	}
	if _, err := notification.NewTargetRef(target.String()); err != nil {
		return webhook.ResolvedTarget{}, webhook.ErrTargetResolution
	}
	if resolver == nil || resolver.service == nil {
		return webhook.ResolvedTarget{}, webhook.ErrTargetResolution
	}
	payload, err := safeResolveConfig(resolver.service, ctx, tenantID, target, resolver.channel)
	if err != nil {
		return webhook.ResolvedTarget{}, classifyError(err)
	}
	defer clearBytes(payload)
	result, err = decode(payload)
	if err != nil {
		return webhook.ResolvedTarget{}, webhook.ErrTargetResolution
	}
	return result, nil
}

// List exposes only enabled, non-secret descriptors for the webhook channel.
// The application service remains the authorization and tenant boundary.
func (resolver *Resolver) List(ctx context.Context, tenantID string) ([]notification.TargetDescriptor, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := notification.ValidateTenantID(tenantID); err != nil {
		return nil, notification.ErrTargetDirectoryFailure
	}
	if resolver == nil || resolver.service == nil {
		return nil, notification.ErrTargetDirectoryFailure
	}
	targets, err := safeList(resolver.service, ctx, tenantID)
	if err != nil {
		return nil, classifyDirectoryError(err)
	}
	if len(targets) > notification.MaxTargets {
		return nil, notification.ErrTargetDirectoryCapacity
	}
	known := []notification.ChannelRef{resolver.channel}
	result := make([]notification.TargetDescriptor, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if target.Channel != resolver.channel {
			continue
		}
		if err := target.Validate(known); err != nil {
			return nil, notification.ErrTargetDirectoryFailure
		}
		key := target.Target.String()
		if _, exists := seen[key]; exists {
			return nil, notification.ErrTargetDirectoryFailure
		}
		seen[key] = struct{}{}
		result = append(result, target.Clone())
	}
	return result, nil
}

func decode(payload []byte) (webhook.ResolvedTarget, error) {
	if len(payload) == 0 || len(payload) > maxConfigurationBytes || !utf8.Valid(payload) {
		return webhook.ResolvedTarget{}, webhook.ErrTargetResolution
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return webhook.ResolvedTarget{}, webhook.ErrTargetResolution
	}
	values := make(map[string]string, 2)
	seen := make(map[string]struct{}, 2)
	for decoder.More() {
		keyToken, err := decoder.Token()
		key, ok := keyToken.(string)
		if err != nil || !ok || key == "" {
			return webhook.ResolvedTarget{}, webhook.ErrTargetResolution
		}
		if _, exists := seen[key]; exists {
			return webhook.ResolvedTarget{}, webhook.ErrTargetResolution
		}
		seen[key] = struct{}{}
		if key != "url" && key != "secret" {
			return webhook.ResolvedTarget{}, webhook.ErrTargetResolution
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil || len(raw) == 0 || string(raw) == "null" {
			return webhook.ResolvedTarget{}, webhook.ErrTargetResolution
		}
		var value string
		if err := json.Unmarshal(raw, &value); err != nil || value == "" || !utf8.ValidString(value) {
			return webhook.ResolvedTarget{}, webhook.ErrTargetResolution
		}
		values[key] = value
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		return webhook.ResolvedTarget{}, webhook.ErrTargetResolution
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return webhook.ResolvedTarget{}, webhook.ErrTargetResolution
	}
	urlValue, hasURL := values["url"]
	secretValue, hasSecret := values["secret"]
	if !hasURL || !hasSecret || !validURLText(urlValue) || !validSecret(secretValue) {
		return webhook.ResolvedTarget{}, webhook.ErrTargetResolution
	}
	return webhook.ResolvedTarget{URL: urlValue, Secret: []byte(secretValue)}, nil
}

func validURLText(value string) bool {
	if len(value) == 0 || len(value) > webhook.MaxWebhookEndpointBytes || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Cs, unicode.Co) {
			return false
		}
	}
	return true
}

func validSecret(value string) bool {
	if len(value) == 0 || len(value) > webhook.MaxWebhookSecretBytes {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Cs, unicode.Co) {
			return false
		}
	}
	return true
}

func safeResolveConfig(service TargetService, ctx context.Context, tenantID string, target notification.TargetRef, channel notification.ChannelRef) (result []byte, err error) {
	defer func() {
		if recover() != nil {
			clearBytes(result)
			result, err = nil, webhook.ErrTargetResolution
		}
	}()
	result, err = service.ResolveConfig(ctx, tenantID, target, channel)
	if err != nil {
		clearBytes(result)
		return nil, err
	}
	return result, nil
}

func safeList(service TargetService, ctx context.Context, tenantID string) (result []notification.TargetDescriptor, err error) {
	defer func() {
		if recover() != nil {
			result, err = nil, notification.ErrTargetDirectoryPanic
		}
	}()
	return service.List(ctx, tenantID)
}

func classifyError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return webhook.ErrTargetResolution
}

func classifyDirectoryError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, notification.ErrTargetDirectoryPanic) {
		return notification.ErrTargetDirectoryPanic
	}
	if errors.Is(err, notification.ErrTargetDirectoryCapacity) {
		return notification.ErrTargetDirectoryCapacity
	}
	return notification.ErrTargetDirectoryFailure
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
