// Package settings owns the HTTP wire models for deployment setting routes.
// The application service owns authorization, validation, and secret masking.
package settings

import "encoding/json"

// PutRequest is the request body for PUT /v1/admin/settings/{key}. Value is
// raw JSON because setting values are persisted in their existing raw form.
type PutRequest struct {
	// A pointer distinguishes a missing value (and JSON null, which also
	// unmarshals to nil) from an explicitly supplied JSON value such as "".
	Value *json.RawMessage `json:"value"`
}

// GetResponse is the response for GET /v1/admin/settings/{key}. The generic
// endpoint omits Value for every key; found settings set Redacted and
// WriteOnly. Value remains in the wire type for compatibility with future
// domain-specific non-sensitive DTOs.
type GetResponse struct {
	Key       string  `json:"key"`
	Found     bool    `json:"found"`
	Value     *string `json:"value,omitempty"`
	Redacted  bool    `json:"redacted,omitempty"`
	WriteOnly bool    `json:"write_only,omitempty"`
}

// PutResponse is the response for a successful settings write.
type PutResponse struct {
	Status string `json:"status"`
}
