// Package notificationtarget owns the provider-neutral HTTP models for
// tenant-scoped notification target administration. Private configuration is
// accepted only by mutation requests and is never represented in responses.
package notificationtarget

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	appnotification "github.com/whhhh1500/auto-agent/pkg/app/notification"
)

var ErrInvalidRequest = errors.New("invalid notification target request")

type TargetView struct {
	TargetRef      string   `json:"target_ref"`
	ChannelID      string   `json:"channel_id"`
	ChannelVersion string   `json:"channel_version"`
	Label          string   `json:"label,omitempty"`
	Formats        []string `json:"formats,omitempty"`
	Enabled        bool     `json:"enabled"`
	Revision       string   `json:"revision"`
}

// ChannelView is a non-secret registered channel reference exposed only for
// management discovery. Provider capabilities and configuration stay private.
type ChannelView struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

type ListResponse struct {
	Targets  []TargetView  `json:"targets"`
	Channels []ChannelView `json:"channels"`
}

type CreateRequest struct {
	TenantID       string          `json:"tenant_id"`
	TargetRef      string          `json:"target_ref"`
	ChannelID      string          `json:"channel_id"`
	ChannelVersion string          `json:"channel_version"`
	Label          string          `json:"label"`
	Formats        []string        `json:"formats"`
	Config         json.RawMessage `json:"config"`
	Enabled        *bool           `json:"enabled"`
}

type UpdateRequest struct {
	TenantID         string          `json:"tenant_id"`
	TargetRef        string          `json:"target_ref"`
	ChannelID        string          `json:"channel_id"`
	ChannelVersion   string          `json:"channel_version"`
	Label            string          `json:"label"`
	Formats          []string        `json:"formats"`
	Config           json.RawMessage `json:"config"`
	Enabled          *bool           `json:"enabled"`
	ExpectedRevision string          `json:"expected_revision"`
}

type DeleteRequest struct {
	TenantID         string `json:"tenant_id"`
	TargetRef        string `json:"target_ref"`
	ExpectedRevision string `json:"expected_revision"`
}

func DecodeRequest(w http.ResponseWriter, r *http.Request, maxBytes int64, target any) error {
	if w == nil || r == nil || r.Body == nil || target == nil || maxBytes <= 0 {
		return ErrInvalidRequest
	}
	limited := io.LimitReader(r.Body, maxBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil || int64(len(body)) > maxBytes {
		return ErrInvalidRequest
	}
	if err := validateUniqueJSON(body); err != nil {
		return ErrInvalidRequest
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ErrInvalidRequest
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrInvalidRequest
	}
	return nil
}

func (request CreateRequest) Validate() error {
	if request.Enabled == nil || request.Config == nil || bytes.Equal(bytes.TrimSpace(request.Config), []byte("null")) || !json.Valid(request.Config) {
		return ErrInvalidRequest
	}
	return validateMutationFields(request.TenantID, request.TargetRef, request.ChannelID, request.ChannelVersion, request.Label, request.Formats, request.Config)
}

func (request UpdateRequest) Validate() error {
	if request.Enabled == nil || request.ExpectedRevision == "" {
		return ErrInvalidRequest
	}
	if request.Config != nil && (bytes.Equal(bytes.TrimSpace(request.Config), []byte("null")) || !json.Valid(request.Config)) {
		return ErrInvalidRequest
	}
	return validateMutationFields(request.TenantID, request.TargetRef, request.ChannelID, request.ChannelVersion, request.Label, request.Formats, request.Config)
}

func (request DeleteRequest) Validate() error {
	if request.TargetRef == "" || request.ExpectedRevision == "" {
		return ErrInvalidRequest
	}
	return nil
}

func validateMutationFields(tenantID, targetRef, channelID, channelVersion, label string, formats []string, config []byte) error {
	if tenantID != "" {
		if err := appnotification.ValidateTenantID(tenantID); err != nil {
			return ErrInvalidRequest
		}
	}
	target, err := appnotification.NewTargetRef(targetRef)
	if err != nil {
		return ErrInvalidRequest
	}
	if config != nil && (len(config) > appnotification.MaxTargetConfigurationBytes || strings.TrimSpace(string(config)) == "") {
		return ErrInvalidRequest
	}
	descriptor := appnotification.TargetDescriptor{Target: target, Channel: appnotification.ChannelRef{ID: channelID, Version: channelVersion}, Label: label, Formats: formats}
	// Channel existence is checked by the application Service. This DTO only
	// enforces the provider-neutral shape and bounded opaque payload.
	if descriptor.Target.String() == "" || len(descriptor.Formats) > appnotification.MaxTargetFormats {
		return ErrInvalidRequest
	}
	return nil
}

func View(record appnotification.TargetRecord) TargetView {
	return TargetView{
		TargetRef: record.Descriptor.Target.String(), ChannelID: record.Descriptor.Channel.ID,
		ChannelVersion: record.Descriptor.Channel.Version, Label: record.Descriptor.Label,
		Formats: append([]string(nil), record.Descriptor.Formats...), Enabled: record.Enabled, Revision: record.Revision,
	}
}

func ChannelViews(channels []appnotification.ChannelRef) []ChannelView {
	views := make([]ChannelView, len(channels))
	for index, channel := range channels {
		views[index] = ChannelView{ID: channel.ID, Version: channel.Version}
	}
	return views
}

func validateUniqueJSON(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := walkJSON(decoder); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrInvalidRequest
	}
	return nil
}

func walkJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch delimiter := token.(type) {
	case json.Delim:
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				key, ok := keyToken.(string)
				if err != nil || !ok {
					return ErrInvalidRequest
				}
				key = strings.ToLower(key)
				if _, exists := seen[key]; exists {
					return ErrInvalidRequest
				}
				seen[key] = struct{}{}
				if err := walkJSON(decoder); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return ErrInvalidRequest
			}
		case '[':
			for decoder.More() {
				if err := walkJSON(decoder); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return ErrInvalidRequest
			}
		default:
			return ErrInvalidRequest
		}
	}
	return nil
}
