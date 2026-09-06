package server

import (
	"context"
	"errors"
	"net/http"

	httpnotification "github.com/cc-auto-agent/harness-core/pkg/adapter/httpapi/notificationtarget"
	appnotification "github.com/cc-auto-agent/harness-core/pkg/app/notification"
	core "github.com/cc-auto-agent/harness-core/pkg/core"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

func (s *Server) registerNotificationTargetRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/admin/notification-targets", s.handleNotificationTargetList)
	mux.HandleFunc("POST /v1/admin/notification-targets", s.handleNotificationTargetCreate)
	mux.HandleFunc("PUT /v1/admin/notification-targets", s.handleNotificationTargetUpdate)
	mux.HandleFunc("DELETE /v1/admin/notification-targets", s.handleNotificationTargetDelete)
}

func (s *Server) handleNotificationTargetList(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok || !requireTenantOperator(w, principal) {
		return
	}
	if s.notificationTargets == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "notification targets are not configured"})
		return
	}
	tenantID, status := notificationTargetTenant(principal, r.URL.Query().Get("tenant_id"))
	if status != 0 {
		writeJSON(w, status, map[string]string{"error": notificationTargetTenantError(status)})
		return
	}
	records, err := s.notificationTargets.ListRecords(r.Context(), tenantID)
	if err != nil {
		writeNotificationTargetError(w, err)
		return
	}
	views := make([]httpnotification.TargetView, 0, len(records))
	for _, record := range records {
		views = append(views, httpnotification.View(record))
	}
	writeJSON(w, http.StatusOK, httpnotification.ListResponse{Targets: views})
}

func (s *Server) handleNotificationTargetCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok || !requireTenantOperator(w, principal) {
		return
	}
	if s.notificationTargets == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "notification targets are not configured"})
		return
	}
	var request httpnotification.CreateRequest
	if err := httpnotification.DecodeRequest(w, r, s.maxBody, &request); err != nil || request.Validate() != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": httpnotification.ErrInvalidRequest.Error()})
		return
	}
	tenantID, status := notificationTargetTenant(principal, request.TenantID)
	if status != 0 {
		writeJSON(w, status, map[string]string{"error": notificationTargetTenantError(status)})
		return
	}
	descriptor, config, err := notificationTargetMutation(request.TargetRef, request.ChannelID, request.ChannelVersion, request.Label, request.Formats, request.Config)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": httpnotification.ErrInvalidRequest.Error()})
		return
	}
	defer clearNotificationBytes(config.Payload)
	revision, err := s.notificationTargets.Create(r.Context(), tenantID, descriptor, config, *request.Enabled)
	if err != nil {
		writeNotificationTargetError(w, err)
		return
	}
	s.recordAudit(r, principal, "notification_target.create", descriptor.Target.String(), map[string]any{"tenant": tenantID, "channel": descriptor.Channel.ID})
	writeJSON(w, http.StatusCreated, httpnotification.View(appnotification.TargetRecord{Descriptor: descriptor, Enabled: *request.Enabled, Revision: revision}))
}

func (s *Server) handleNotificationTargetUpdate(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok || !requireTenantOperator(w, principal) {
		return
	}
	if s.notificationTargets == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "notification targets are not configured"})
		return
	}
	var request httpnotification.UpdateRequest
	if err := httpnotification.DecodeRequest(w, r, s.maxBody, &request); err != nil || request.Validate() != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": httpnotification.ErrInvalidRequest.Error()})
		return
	}
	tenantID, status := notificationTargetTenant(principal, request.TenantID)
	if status != 0 {
		writeJSON(w, status, map[string]string{"error": notificationTargetTenantError(status)})
		return
	}
	descriptor, config, err := notificationTargetMutation(request.TargetRef, request.ChannelID, request.ChannelVersion, request.Label, request.Formats, request.Config)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": httpnotification.ErrInvalidRequest.Error()})
		return
	}
	defer clearNotificationBytes(config.Payload)
	revision, err := s.notificationTargets.Update(r.Context(), tenantID, descriptor, config, *request.Enabled, request.ExpectedRevision)
	if err != nil {
		writeNotificationTargetError(w, err)
		return
	}
	s.recordAudit(r, principal, "notification_target.update", descriptor.Target.String(), map[string]any{"tenant": tenantID, "channel": descriptor.Channel.ID})
	writeJSON(w, http.StatusOK, httpnotification.View(appnotification.TargetRecord{Descriptor: descriptor, Enabled: *request.Enabled, Revision: revision}))
}

func (s *Server) handleNotificationTargetDelete(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok || !requireTenantOperator(w, principal) {
		return
	}
	if s.notificationTargets == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "notification targets are not configured"})
		return
	}
	var request httpnotification.DeleteRequest
	if err := httpnotification.DecodeRequest(w, r, s.maxBody, &request); err != nil || request.Validate() != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": httpnotification.ErrInvalidRequest.Error()})
		return
	}
	tenantID, status := notificationTargetTenant(principal, request.TenantID)
	if status != 0 {
		writeJSON(w, status, map[string]string{"error": notificationTargetTenantError(status)})
		return
	}
	target, err := appnotification.NewTargetRef(request.TargetRef)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": httpnotification.ErrInvalidRequest.Error()})
		return
	}
	if err := s.notificationTargets.Delete(r.Context(), tenantID, target, request.ExpectedRevision); err != nil {
		writeNotificationTargetError(w, err)
		return
	}
	s.recordAudit(r, principal, "notification_target.delete", target.String(), map[string]any{"tenant": tenantID})
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func notificationTargetMutation(targetValue, channelID, channelVersion, label string, formats []string, raw []byte) (appnotification.TargetDescriptor, appnotification.TargetConfiguration, error) {
	target, err := appnotification.NewTargetRef(targetValue)
	if err != nil {
		return appnotification.TargetDescriptor{}, appnotification.TargetConfiguration{}, err
	}
	return appnotification.TargetDescriptor{Target: target, Channel: appnotification.ChannelRef{ID: channelID, Version: channelVersion}, Label: label, Formats: append([]string(nil), formats...)}, appnotification.TargetConfiguration{Payload: append([]byte(nil), raw...)}, nil
}

func notificationTargetTenant(principal core.Principal, requested string) (string, int) {
	if requested == "" {
		if roleOf(principal) == storage.RoleAccountAdmin {
			return "", http.StatusBadRequest
		}
		requested = principal.TenantID
	} else if roleOf(principal) != storage.RoleAccountAdmin && requested != principal.TenantID {
		return "", http.StatusForbidden
	}
	if err := appnotification.ValidateTenantID(requested); err != nil {
		return "", http.StatusBadRequest
	}
	return requested, 0
}

func notificationTargetTenantError(status int) string {
	if status == http.StatusForbidden {
		return "principal cannot manage the requested tenant"
	}
	return "tenant_id is required and must be valid"
}

func writeNotificationTargetError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, appnotification.ErrTargetRevisionConflict):
		status = http.StatusConflict
	case errors.Is(err, appnotification.ErrTargetNotFound):
		status = http.StatusNotFound
	case errors.Is(err, appnotification.ErrTargetDisabled):
		status = http.StatusConflict
	case errors.Is(err, appnotification.ErrInvalidTargetDescriptor), errors.Is(err, appnotification.ErrUnknownTargetChannel), errors.Is(err, appnotification.ErrInvalidTargetConfiguration), errors.Is(err, appnotification.ErrInvalidTargetRevision), errors.Is(err, appnotification.ErrTargetChannelChangeRequiresConfiguration):
		status = http.StatusBadRequest
	case errors.Is(err, appnotification.ErrTargetDirectoryCapacity):
		status = http.StatusConflict
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		status = http.StatusServiceUnavailable
	}
	message := "notification target operation failed"
	if status == http.StatusBadRequest {
		message = "invalid notification target request"
	} else if status == http.StatusConflict {
		message = "notification target state conflict"
	} else if status == http.StatusNotFound {
		message = "notification target not found"
	}
	writeJSON(w, status, map[string]string{"error": message})
}

func clearNotificationBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
