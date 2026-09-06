// Typed storage configuration HTTP adaptation. Legacy storage persistence,
// connectivity testing, and route registration remain in server_storage.go.
package server

import (
	"errors"
	"net/http"

	storagehttp "github.com/cc-auto-agent/harness-core/pkg/adapter/httpapi/storage"
	appstorageconfig "github.com/cc-auto-agent/harness-core/pkg/app/storageconfig"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

func (h storageSettingsHandler) getTyped(w http.ResponseWriter, r *http.Request, principal core.Principal) {
	kind, ok := h.kind()
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "storage configuration route is invalid"})
		return
	}
	result, err := h.server.storageConfigUseCases.Get(r.Context(), appstorageconfig.GetCommand{
		Actor: accountAdminActor(principal), Kind: kind,
	})
	if err != nil {
		writeStorageConfigError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, storageConfigGetResponse(h.key, kind, result))
}

func (h storageSettingsHandler) putTyped(w http.ResponseWriter, r *http.Request, principal core.Principal) {
	kind, ok := h.kind()
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "storage configuration route is invalid"})
		return
	}
	var request storagehttp.PutRequest
	if !h.server.decodeJSON(w, r, &request) {
		return
	}
	// Older clients used the fixed mask as a preserve marker. Keep that input
	// compatibility only at the HTTP edge; the application service receives an
	// unambiguous omitted-secret command.
	mapLegacyStorageSecretMask(&request)
	result, err := h.server.storageConfigUseCases.Put(r.Context(), storageConfigPutCommand(principal, kind, request))
	if err != nil {
		// A resource apply may fail after the desired configuration commits. Keep
		// that state transition auditable without treating the response as a
		// success or copying any secret/preview material into the audit trail.
		if result.Config.DesiredRevision != "" {
			h.server.recordAudit(r, principal, "storage.config", h.key, storageConfigFailedAuditDetail(result))
		}
		writeStorageConfigError(w, err)
		return
	}
	response := storageConfigPutResponse(kind, result)
	// Deliberately structural: never put secret material or its preview into an
	// audit detail, even though the response may safely contain a fixed preview.
	h.server.recordAudit(r, principal, "storage.config", h.key, map[string]any{
		"type": response.DesiredType, "applied": response.Applied, "revision": response.DesiredRevision,
	})
	writeJSON(w, http.StatusOK, response)
}

func (h storageSettingsHandler) kind() (appstorageconfig.Kind, bool) {
	switch h.key {
	case storageResourcesKey:
		return appstorageconfig.KindResources, true
	case storageSessionsKey:
		return appstorageconfig.KindSessions, true
	default:
		return "", false
	}
}

func storageConfigPutCommand(principal core.Principal, kind appstorageconfig.Kind, request storagehttp.PutRequest) appstorageconfig.PutCommand {
	command := appstorageconfig.PutCommand{
		Actor:                           accountAdminActor(principal),
		Kind:                            kind,
		Backend:                         appstorageconfig.Backend(request.Type),
		Path:                            request.Path,
		Endpoint:                        request.Endpoint,
		Region:                          request.Region,
		Bucket:                          request.Bucket,
		AccessKey:                       request.AccessKey.Value,
		AccessKeyPresent:                request.AccessKey.Present,
		PathStyle:                       request.PathStyle,
		DisableConditionalWrites:        request.DisableConditionalWrites.Value,
		DisableConditionalWritesPresent: request.DisableConditionalWrites.Present,
		Status:                          appstorageconfig.ConfigStatus(request.Status),
		ClearSecret:                     request.ClearSecret,
		ExpectedRevision:                request.ExpectedRevision,
	}
	if request.SecretKey.Present {
		secret := request.SecretKey.Value
		command.SecretKey = &secret
	}
	return command
}

func storageConfigGetResponse(key string, kind appstorageconfig.Kind, result appstorageconfig.GetResult) storagehttp.GetResponse {
	state := storageConfigState(result.Found, result.Config, result.Evidence, result.Active)
	response := storagehttp.GetResponse{
		Key: key, Found: result.Found,
		Config:      storageConfigHTTPConfiguration(kind, result.Config),
		Active:      storageConfigLegacyActive(kind, state.activeType),
		DesiredType: state.desiredType, ActiveType: state.activeType,
		DesiredRevision: state.desiredRevision, ActiveRevision: state.activeRevision,
		RestartPending: state.restartPending, ErrorCode: state.errorCode,
	}
	if result.Migration.State != "" {
		response.Migration = migrationProgressHTTP(result.Migration)
	}
	return response
}

func storageConfigPutResponse(kind appstorageconfig.Kind, result appstorageconfig.PutResult) storagehttp.PutResponse {
	state := storageConfigState(true, result.Config, result.Evidence, appstorageconfig.ActiveState{
		Backend: result.Evidence.ActiveType, Revision: result.Evidence.ActiveRevision,
	})
	configuration := storageConfigHTTPConfiguration(kind, result.Config)
	response := storagehttp.PutResponse{
		Status: "saved", Applied: storageConfigApplied(kind, result.Evidence.Status), Config: &configuration,
		DesiredType: state.desiredType, ActiveType: state.activeType,
		DesiredRevision: state.desiredRevision, ActiveRevision: state.activeRevision,
		RestartPending: state.restartPending, ErrorCode: state.errorCode,
	}
	if result.Migration.State != "" {
		response.Migration = migrationProgressHTTP(result.Migration)
	}
	return response
}

func migrationProgressHTTP(progress appstorageconfig.MigrationProgress) *storagehttp.MigrationProgress {
	return &storagehttp.MigrationProgress{State: progress.State, CopiedObjects: progress.CopiedObjects, CopiedBytes: progress.CopiedBytes, VerifiedObjects: progress.VerifiedObjects, VerifiedBytes: progress.VerifiedBytes, ErrorCode: progress.ErrorCode, CreatedAt: progress.CreatedAt, UpdatedAt: progress.UpdatedAt, VerifiedAt: progress.VerifiedAt, ActivatedAt: progress.ActivatedAt}
}

type storageConfigResponseState struct {
	desiredType     string
	activeType      string
	desiredRevision string
	activeRevision  string
	restartPending  bool
	errorCode       string
}

func storageConfigState(found bool, config appstorageconfig.ConfigView, evidence appstorageconfig.ResolutionEvidence, active appstorageconfig.ActiveState) storageConfigResponseState {
	state := storageConfigResponseState{
		desiredType: string(evidence.DesiredType), activeType: string(active.Backend),
		desiredRevision: evidence.DesiredRevision, activeRevision: active.Revision,
		restartPending: evidence.RestartPending, errorCode: string(evidence.ErrorCode),
	}
	if state.desiredType == "" && found {
		state.desiredType = string(config.Backend)
	}
	if state.desiredRevision == "" && found {
		state.desiredRevision = config.DesiredRevision
	}
	if state.activeType == "" {
		state.activeType = string(evidence.ActiveType)
	}
	if state.activeRevision == "" {
		state.activeRevision = evidence.ActiveRevision
	}
	return state
}

func storageConfigHTTPConfiguration(kind appstorageconfig.Kind, config appstorageconfig.ConfigView) storagehttp.Configuration {
	response := storagehttp.Configuration{
		Type: string(config.Backend), Path: config.Path, Endpoint: config.Endpoint,
		Region: config.Region, Bucket: config.Bucket,
		PathStyle: config.PathStyle, Source: string(config.Source), Status: string(config.Status),
		DesiredRevision: config.DesiredRevision, HasAccessKey: config.HasAccessKey, AccessKeyPreview: config.AccessKeyPreview,
		HasSecret: config.HasSecret, SecretPreview: config.SecretPreview,
	}
	if kind == appstorageconfig.KindSessions {
		disableConditionalWrites := config.DisableConditionalWrites
		response.DisableConditionalWrites = &disableConditionalWrites
	}
	return response
}

func storageConfigLegacyActive(kind appstorageconfig.Kind, activeType string) string {
	if kind == appstorageconfig.KindSessions {
		return "applies on restart"
	}
	return activeType
}

func storageConfigApplied(kind appstorageconfig.Kind, status appstorageconfig.ConfigStatus) string {
	if status == appstorageconfig.StatusInactive {
		return "inactive"
	}
	if status == appstorageconfig.StatusMigrationPending {
		return "migration pending"
	}
	if kind == appstorageconfig.KindSessions {
		return "restart to apply"
	}
	return "active now"
}

func storageConfigFailedAuditDetail(result appstorageconfig.PutResult) map[string]any {
	desiredType := string(result.Evidence.DesiredType)
	if desiredType == "" {
		desiredType = string(result.Config.Backend)
	}
	revision := result.Evidence.DesiredRevision
	if revision == "" {
		revision = result.Config.DesiredRevision
	}
	status := string(result.Evidence.Status)
	if status == "" {
		status = string(result.Config.Status)
	}
	return map[string]any{
		"type": desiredType, "revision": revision, "status": status,
		"error_code": string(result.Evidence.ErrorCode),
	}
}

func writeStorageConfigError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, appstorageconfig.ErrForbidden):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
	case errors.Is(err, appstorageconfig.ErrConflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	case errors.Is(err, appstorageconfig.ErrUnsupportedTransition):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	case errors.Is(err, appstorageconfig.ErrInvalidKind),
		errors.Is(err, appstorageconfig.ErrInvalidConfiguration),
		errors.Is(err, appstorageconfig.ErrInvalidInput),
		errors.Is(err, appstorageconfig.ErrInactiveConfiguration):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
}
