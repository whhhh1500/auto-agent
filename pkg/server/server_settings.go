package server

import (
	"errors"
	"net/http"

	modelsettingshttp "github.com/cc-auto-agent/harness-core/pkg/adapter/httpapi/modelsettings"
	settingshttp "github.com/cc-auto-agent/harness-core/pkg/adapter/httpapi/settings"
	appmodelsettings "github.com/cc-auto-agent/harness-core/pkg/app/modelsettings"
	appsettings "github.com/cc-auto-agent/harness-core/pkg/app/settings"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

// --- settings ---

func (s *Server) handleGetSetting(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	// Keep this coarse route gate before optional-service checks so an
	// unauthorized caller cannot learn whether settings are configured.
	if !requireAdmin(w, principal) {
		return
	}
	if s.settingsUseCases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "settings service is not configured"})
		return
	}
	result, err := s.settingsUseCases.Get(r.Context(), appsettings.GetCommand{
		Actor: accountAdminActor(principal), Key: r.PathValue("key"),
	})
	if err != nil {
		writeSettingsError(w, err, http.StatusInternalServerError)
		return
	}
	response := settingshttp.GetResponse{
		Key: result.Key, Found: result.Found,
		Redacted: result.Redacted, WriteOnly: result.WriteOnly,
	}
	if result.Found && !result.Redacted && !result.WriteOnly {
		value := result.Value
		response.Value = &value
	}
	writeJSON(w, http.StatusOK, response)
}

// handleLegacyLLMSettingPut preserves the historical generic write shape only
// for the exact llm key. Its empty nested api_key compatibility behavior is
// isolated in the DTO mapper; canonical callers must use model-settings.
func (s *Server) handleLegacyLLMSettingPut(w http.ResponseWriter, r *http.Request, principal core.Principal) {
	if s.modelSettingsUseCases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "model settings service is not configured"})
		return
	}
	var request settingshttp.PutRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if request.Value == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "value is required and must not be null"})
		return
	}
	legacy, err := modelsettingshttp.LegacyPutRequest(*request.Value)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.modelSettingsUseCases.Put(r.Context(), modelSettingsCommand(principal, legacy)); err != nil {
		writeModelSettingsError(w, err)
		return
	}
	s.recordAudit(r, principal, "setting.put", "llm", nil)
	writeJSON(w, http.StatusOK, settingshttp.PutResponse{Status: "saved"})
}

func (s *Server) handleGetModelSettings(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if s.modelSettingsUseCases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "model settings service is not configured"})
		return
	}
	result, err := s.modelSettingsUseCases.Get(r.Context(), appmodelsettings.GetCommand{Actor: accountAdminActor(principal)})
	if err != nil {
		writeModelSettingsError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, modelsettingshttp.GetResponse{
		Found: result.Found, BaseURL: result.BaseURL, Model: result.Model,
		Provider: string(result.Provider), Protocol: string(result.Protocol),
		MaxTokens:     result.MaxTokens,
		AllowedModels: result.AllowedModels, HasAPIKey: result.HasAPIKey, APIKeyPreview: result.APIKeyPreview,
		ConfigSource: string(result.ConfigSource),
		Executable:   result.Executable, ExecutionStatus: result.ExecutionStatus, ExecutionReason: result.ExecutionReason,
	})
}

func (s *Server) handlePutModelSettings(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if !requireAdmin(w, principal) {
		return
	}
	if s.modelSettingsUseCases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "model settings service is not configured"})
		return
	}
	var request modelsettingshttp.PutRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if err := s.modelSettingsUseCases.Put(r.Context(), modelSettingsCommand(principal, request)); err != nil {
		writeModelSettingsError(w, err)
		return
	}
	s.recordAudit(r, principal, "model_settings.put", "llm", nil)
	writeJSON(w, http.StatusOK, modelsettingshttp.PutResponse{Status: "saved"})
}

func modelSettingsCommand(principal core.Principal, request modelsettingshttp.PutRequest) appmodelsettings.PutCommand {
	command := appmodelsettings.PutCommand{
		Actor: accountAdminActor(principal), BaseURL: request.BaseURL, Model: request.Model,
		MaxTokens: request.MaxTokens.Value, MaxTokensPresent: request.MaxTokens.Present,
		AllowedModels: request.AllowedModels.Values, AllowedModelsPresent: request.AllowedModels.Present,
		ClearAPIKey: request.ClearAPIKey.Present && request.ClearAPIKey.Value,
	}
	if request.APIKey.Present {
		value := request.APIKey.Value
		command.APIKey = &value
	}
	if request.Provider.Present {
		value := appmodelsettings.ProviderID(request.Provider.Value)
		command.Provider = &value
	}
	if request.Protocol.Present {
		value := appmodelsettings.ProtocolID(request.Protocol.Value)
		command.Protocol = &value
	}
	return command
}

func writeModelSettingsError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, appmodelsettings.ErrForbidden):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
	case errors.Is(err, appmodelsettings.ErrInvalidInput), errors.Is(err, appmodelsettings.ErrInvalidConfiguration):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
}

func (s *Server) handlePutSetting(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	// Preserve authorization-before-availability and authorization-before-
	// decoding ordering for this administrative route.
	if !requireAdmin(w, principal) {
		return
	}
	if r.PathValue("key") == "llm" {
		s.handleLegacyLLMSettingPut(w, r, principal)
		return
	}
	if s.settingsUseCases == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "settings service is not configured"})
		return
	}
	var request settingshttp.PutRequest
	if !s.decodeJSON(w, r, &request) {
		return
	}
	if request.Value == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "value is required and must not be null"})
		return
	}
	if err := s.settingsUseCases.Put(r.Context(), appsettings.PutCommand{
		Actor: accountAdminActor(principal), Key: r.PathValue("key"), Value: string(*request.Value),
	}); err != nil {
		writeSettingsError(w, err, http.StatusInternalServerError)
		return
	}
	s.recordAudit(r, principal, "setting.put", r.PathValue("key"), nil)
	writeJSON(w, http.StatusOK, settingshttp.PutResponse{Status: "saved"})
}

func writeSettingsError(w http.ResponseWriter, err error, operationalStatus int) {
	switch {
	case errors.Is(err, appsettings.ErrForbidden):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
	case errors.Is(err, appsettings.ErrInvalidInput):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, operationalStatus, map[string]string{"error": err.Error()})
	}
}
