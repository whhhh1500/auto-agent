package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/cc-auto-agent/harness-core/pkg/storage"
)

// --- resources (S3-path semantics; embedded filesystem by default) ---

// resourcePrefix maps a tenant to its S3-style key prefix.
func resourcePrefix(tenant string) string {
	return "tenants/" + tenant + "/resources/"
}

func tenantResourceKey(tenant, rel string) (string, error) {
	if strings.TrimSpace(tenant) == "" || strings.ContainsAny(tenant, "/\\") {
		return "", fmt.Errorf("invalid resource tenant")
	}
	if err := storage.ValidateObjectKey(tenant); err != nil {
		return "", fmt.Errorf("invalid resource tenant")
	}
	if err := storage.ValidateObjectKey(rel); err != nil {
		return "", err
	}
	prefix := resourcePrefix(tenant)
	key := prefix + rel
	if err := storage.ValidateObjectKey(key); err != nil {
		return "", err
	}
	if !strings.HasPrefix(key, prefix) {
		return "", fmt.Errorf("invalid resource key %q", rel)
	}
	return key, nil
}

func tenantResourcePrefix(tenant, relPrefix string) (string, error) {
	if strings.TrimSpace(tenant) == "" || strings.ContainsAny(tenant, "/\\") {
		return "", fmt.Errorf("invalid resource tenant")
	}
	if err := storage.ValidateObjectKey(tenant); err != nil {
		return "", fmt.Errorf("invalid resource tenant")
	}
	prefix := resourcePrefix(tenant)
	if strings.TrimSpace(relPrefix) == "" {
		return prefix, nil
	}
	trimmed := strings.TrimSuffix(relPrefix, "/")
	if err := storage.ValidateObjectKey(trimmed); err != nil {
		return "", err
	}
	full := prefix + relPrefix
	if !strings.HasPrefix(full, prefix) {
		return "", fmt.Errorf("invalid resource prefix %q", relPrefix)
	}
	return full, nil
}

// handleResourceList enumerates keys under the caller's tenant prefix.
func (s *Server) handleResourceList(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if s.resources == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "resource store is not configured"})
		return
	}
	tenant := principal.TenantID
	if tenantOverride := r.URL.Query().Get("tenant"); tenantOverride != "" {
		if !requireAdmin(w, principal) {
			return
		}
		tenant = tenantOverride
	}
	prefix, err := tenantResourcePrefix(tenant, r.URL.Query().Get("prefix"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	items, err := s.resources.List(r.Context(), prefix, "", 1000)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]any{
			"key":  strings.TrimPrefix(item.Key, resourcePrefix(tenant)),
			"size": item.Size,
			"etag": item.ETag,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"resources": out, "tenant": tenant})
}

// handleResourceGet streams one object.
func (s *Server) handleResourceGet(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if s.resources == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "resource store is not configured"})
		return
	}
	key, err := tenantResourceKey(principal.TenantID, r.PathValue("key"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if streaming, ok := s.resources.(storage.StreamingObjectGetter); ok {
		body, etag, size, streamErr := streaming.Open(r.Context(), key)
		if streamErr == nil {
			defer body.Close()
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("X-Resource-Key", r.PathValue("key"))
			if etag != "" {
				w.Header().Set("ETag", etag)
			}
			if size >= 0 {
				w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			}
			_, _ = io.Copy(w, body)
			return
		}
		if !errors.Is(streamErr, storage.ErrStreamingUnsupported) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "resource not found"})
			return
		}
	}
	data, _, err := s.resources.Get(r.Context(), key)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "resource not found"})
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Resource-Key", r.PathValue("key"))
	_, _ = w.Write(data)
}

// handleResourcePut stores one object; the body size is capped.
func (s *Server) handleResourcePut(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if s.resources == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "resource store is not configured"})
		return
	}
	key, err := tenantResourceKey(principal.TenantID, r.PathValue("key"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if strings.HasSuffix(key, "/") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "resource key is required"})
		return
	}
	if streaming, ok := s.resources.(storage.StreamingObjectPutter); ok {
		body := &resourceCountingReader{reader: r.Body}
		_, streamErr := streaming.PutStream(r.Context(), key, body, s.maxResourceBytes, storage.PutOptions{})
		if streamErr == nil {
			s.recordAudit(r, principal, "resource.put", key, map[string]any{"size": body.n, "streamed": true})
			writeJSON(w, http.StatusCreated, map[string]string{"key": r.PathValue("key"), "size": strconv.FormatInt(body.n, 10)})
			return
		}
		if !errors.Is(streamErr, storage.ErrStreamingUnsupported) {
			if errors.Is(streamErr, storage.ErrObjectTooLarge) {
				writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "resource exceeds the size limit"})
			} else if errors.Is(streamErr, context.Canceled) || errors.Is(streamErr, context.DeadlineExceeded) {
				return
			} else {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": streamErr.Error()})
			}
			return
		}
	}
	body := io.LimitReader(r.Body, s.maxResourceFallbackBytes+1)
	data, err := io.ReadAll(body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if int64(len(data)) > s.maxResourceFallbackBytes {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "resource exceeds the size limit"})
		return
	}
	if _, err := s.resources.Put(r.Context(), key, data, storage.PutOptions{}); err != nil {
		if errors.Is(err, storage.ErrObjectTooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "resource exceeds the size limit"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.recordAudit(r, principal, "resource.put", key, map[string]any{"size": len(data)})
	writeJSON(w, http.StatusCreated, map[string]string{"key": r.PathValue("key"), "size": strconv.Itoa(len(data))})
}

type resourceCountingReader struct {
	reader io.Reader
	n      int64
}

func (r *resourceCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.n += int64(n)
	return n, err
}

// handleResourceDelete removes one object.
func (s *Server) handleResourceDelete(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	if s.resources == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "resource store is not configured"})
		return
	}
	key, err := tenantResourceKey(principal.TenantID, r.PathValue("key"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.resources.Delete(r.Context(), key); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.recordAudit(r, principal, "resource.delete", key, nil)
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}
