// Storage configuration lets the Console re-point resource and session object
// storage between embedded and S3-compatible backends. Secret values persist
// encrypted through settings but are never sent by GET.
package server

import (
	"context"
	"encoding/json"
	"net/http"

	storagehttp "github.com/cc-auto-agent/harness-core/pkg/adapter/httpapi/storage"
	"github.com/cc-auto-agent/harness-core/pkg/app/secretview"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// StorageConfig preserves the legacy persisted S3 configuration shape and
// public type identity. New HTTP handlers use adapter/httpapi/storage DTOs.
type StorageConfig struct {
	Type      string `json:"type"` // "embedded" | "s3"
	Endpoint  string `json:"endpoint,omitempty"`
	Region    string `json:"region,omitempty"`
	Bucket    string `json:"bucket,omitempty"`
	AccessKey string `json:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty"`
	PathStyle bool   `json:"path_style,omitempty"`
}

const (
	storageResourcesKey = "storage.resources"
	storageSessionsKey  = "storage.sessions"
	legacySecretMask    = "••••"
)

// storageSettingsHandler serves GET/PUT for one storage settings key with
// dynamic re-application when the caller supports it.
type storageSettingsHandler struct {
	server  *Server
	key     string
	dynamic bool // resources: swap at runtime; sessions: restart to apply
}

func (h storageSettingsHandler) get(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.server.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if h.server.storageConfigUseCases != nil {
		h.getTyped(w, r, principal)
		return
	}
	if h.server.settingsRepository == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "account store is not configured"})
		return
	}
	config, found, err := h.loadConfig(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	response := storagehttp.GetResponse{
		Key: h.key, Found: found, Config: storageConfigView(config),
	}
	// Current live backend label (for the resources dynamic store).
	if h.dynamic {
		if dynamic, ok := h.server.resources.(*storage.DynamicObjectStore); ok {
			response.Active = dynamic.Label()
		}
	} else {
		response.Active = "applies on restart"
	}
	writeJSON(w, http.StatusOK, response)
}

func (h storageSettingsHandler) put(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.server.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if h.server.storageConfigUseCases != nil {
		h.putTyped(w, r, principal)
		return
	}
	if h.server.settingsRepository == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "account store is not configured"})
		return
	}
	var request storagehttp.PutRequest
	if !h.server.decodeJSON(w, r, &request) {
		return
	}
	mapLegacyStorageSecretMask(&request)
	if request.ClearSecret && request.SecretKey.Present {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "clear_secret and secret_key cannot be used together"})
		return
	}
	if request.SecretKey.Present && request.SecretKey.Value == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "secret_key must be omitted, non-empty, or cleared explicitly"})
		return
	}
	config := storageConfigFromPutRequest(request)
	switch config.Type {
	case "embedded":
		// no fields required
	case "s3":
		if config.Bucket == "" || config.AccessKey == "" || config.Endpoint == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "s3 storage requires endpoint, bucket and access_key"})
			return
		}
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "type must be embedded or s3"})
		return
	}

	previous, _, err := h.loadConfig(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	switch {
	case request.ClearSecret:
		config.SecretKey = ""
	case request.SecretKey.Present:
		config.SecretKey = request.SecretKey.Value
	default:
		config.SecretKey = usableStorageSecret(previous.SecretKey)
	}
	if config.Type == "s3" && config.SecretKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "secret_key is required for s3 storage"})
		return
	}

	// Connectivity test before saving: never swap to a store we cannot reach.
	if config.Type == "s3" {
		store, err := newS3StorageStore(config)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := store.EnsureBucket(r.Context()); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "storage test failed: " + err.Error()})
			return
		}
	}

	encoded, err := json.Marshal(config)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if err := h.server.settingsRepository.SetSetting(r.Context(), h.key, string(encoded)); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	applied := "restart to apply"
	if h.dynamic {
		applied = "active now"
		if config.Type == "s3" {
			store, err := newS3StorageStore(config)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			if dynamic, ok := h.server.resources.(*storage.DynamicObjectStore); ok {
				dynamic.Swap(store, "s3")
			}
		} else if h.server.embeddedResources != nil {
			if dynamic, ok := h.server.resources.(*storage.DynamicObjectStore); ok {
				dynamic.Swap(h.server.embeddedResources, "embedded")
			}
		}
	}
	h.server.recordAudit(r, principal, "storage.config", h.key, map[string]any{"type": config.Type, "applied": applied})
	writeJSON(w, http.StatusOK, storagehttp.PutResponse{Status: "saved", Applied: applied})
}

func (h storageSettingsHandler) loadConfig(ctx context.Context) (StorageConfig, bool, error) {
	raw, found, err := h.server.settingsRepository.GetSetting(ctx, h.key)
	if err != nil || !found {
		return StorageConfig{}, found, err
	}
	var config StorageConfig
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		return StorageConfig{}, true, nil
	}
	return config, true, nil
}

func storageConfigView(config StorageConfig) storagehttp.Configuration {
	// A missing or malformed legacy value has always read as the embedded
	// default. Keep that presentation behavior while omitting its secret.
	if config.Type == "" {
		config.Type = "embedded"
	}
	secret := usableStorageSecret(config.SecretKey)
	view := storagehttp.Configuration{
		Type: config.Type, Endpoint: config.Endpoint, Region: config.Region,
		Bucket: config.Bucket, HasAccessKey: config.AccessKey != "", AccessKeyPreview: secretview.Preview(config.AccessKey), PathStyle: config.PathStyle,
		HasSecret: secret != "",
	}
	if secret != "" {
		view.SecretPreview = secretview.Preview(secret)
	}
	return view
}

func storageConfigFromPutRequest(request storagehttp.PutRequest) StorageConfig {
	return StorageConfig{
		Type: request.Type, Endpoint: request.Endpoint, Region: request.Region,
		Bucket: request.Bucket, AccessKey: request.AccessKey.Value, PathStyle: request.PathStyle,
	}
}

// mapLegacyStorageSecretMask is the sole compatibility mapper for older
// Console clients that send the former fixed mask to preserve a secret.
func mapLegacyStorageSecretMask(request *storagehttp.PutRequest) {
	if request.SecretKey.Present && request.SecretKey.Value == legacySecretMask {
		request.SecretKey = storagehttp.OptionalString{}
	}
}

// usableStorageSecret keeps historical persisted Console mask markers from
// becoming live credentials. New requests are handled only by
// mapLegacyStorageSecretMask; this helper is intentionally limited to reading
// old persisted configurations and never affects the emitted preview format.
func usableStorageSecret(value string) string {
	if value == legacySecretMask {
		return ""
	}
	return value
}

func newS3StorageStore(config StorageConfig) (*storage.S3ObjectStore, error) {
	return storage.NewS3ObjectStore(storage.S3Config{
		Endpoint: config.Endpoint, Region: config.Region, Bucket: config.Bucket,
		AccessKey: config.AccessKey, SecretKey: config.SecretKey, PathStyle: config.PathStyle,
	})
}

// handleStorageTest validates an explicit storage config without saving or
// swapping it. It cannot use a persisted secret, so S3 callers must supply one.
func (s *Server) handleStorageTest(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	var request storagehttp.TestRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if request.Type != "s3" {
		writeJSON(w, http.StatusOK, storagehttp.TestResponse{Status: "ok", Backend: request.Type})
		return
	}
	store, err := newS3StorageStore(StorageConfig{
		Type: request.Type, Endpoint: request.Endpoint, Region: request.Region,
		Bucket: request.Bucket, AccessKey: request.AccessKey, SecretKey: request.SecretKey,
		PathStyle: request.PathStyle,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := store.EnsureBucket(r.Context()); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "connection test failed: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, storagehttp.TestResponse{Status: "ok"})
}

// registerStorageRoutes mounts GET/PUT settings and the connection test.
func (s *Server) registerStorageRoutes(mux *http.ServeMux) {
	resources := storageSettingsHandler{server: s, key: storageResourcesKey, dynamic: true}
	sessions := storageSettingsHandler{server: s, key: storageSessionsKey, dynamic: false}
	mux.HandleFunc("GET /v1/admin/storage/resources", resources.get)
	mux.HandleFunc("PUT /v1/admin/storage/resources", resources.put)
	mux.HandleFunc("GET /v1/admin/storage/sessions", sessions.get)
	mux.HandleFunc("PUT /v1/admin/storage/sessions", sessions.put)
	mux.HandleFunc("POST /v1/admin/storage/test", s.handleStorageTest)
}
