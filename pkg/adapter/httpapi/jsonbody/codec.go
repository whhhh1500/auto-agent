// Package jsonbody owns the shared HTTP JSON request-body codec. It keeps
// forward compatibility at the outer object boundary while preserving strict
// validation for every documented field and nested object. Target DTOs must
// use named fields: anonymous embedded fields are rejected rather than being
// silently skipped during top-level compatibility filtering.
package jsonbody

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
)

// Decode reads exactly one JSON object into target. Unknown top-level fields
// are ignored for forward compatibility, but known fields and every nested
// object are decoded with DisallowUnknownFields. Target DTOs may not use
// anonymous embedded JSON fields. The function returns the raw decoding
// error; response status and error envelopes are a server concern.
func Decode(writer http.ResponseWriter, request *http.Request, maxBytes int64, target any) error {
	return decode(writer, request, maxBytes, target, false)
}

// DecodeOptional behaves like Decode but accepts an empty request body and
// leaves target at its zero value. It exists for endpoints whose empty body
// has an explicit legacy meaning.
func DecodeOptional(writer http.ResponseWriter, request *http.Request, maxBytes int64, target any) error {
	return decode(writer, request, maxBytes, target, true)
}

func decode(writer http.ResponseWriter, request *http.Request, maxBytes int64, target any, allowEmpty bool) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxBytes)
	var body map[string]json.RawMessage
	decoder := json.NewDecoder(request.Body)
	if err := decoder.Decode(&body); err != nil {
		if err == io.EOF && allowEmpty {
			return nil
		}
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("request body contains multiple JSON values")
		}
		return err
	}
	// /v1 request bodies are forward-compatible at their top level: clients
	// may send fields introduced by a newer server without breaking an older
	// deployment. Known fields, including nested objects, remain strict.
	body, err := compatibleTopLevelFields(body, target)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	decoder = json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

// compatibleTopLevelFields removes unknown fields only from a struct request's
// outer object. The strict decode in Decode still rejects unknown fields in
// nested DTOs and validates the types of every documented field. Anonymous
// embedded JSON fields are deliberately unsupported: modeling encoding/json's
// promotion rules here would make the forward-compatibility filter subtly
// different from the strict decode that follows.
func compatibleTopLevelFields(body map[string]json.RawMessage, target any) (map[string]json.RawMessage, error) {
	typ := reflect.TypeOf(target)
	for typ != nil && typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ == nil || typ.Kind() != reflect.Struct || body == nil {
		return body, nil
	}
	known := make(map[string]struct{}, typ.NumField())
	for index := 0; index < typ.NumField(); index++ {
		field := typ.Field(index)
		tagName := strings.Split(field.Tag.Get("json"), ",")[0]
		if field.Anonymous {
			if tagName == "-" {
				continue
			}
			return nil, fmt.Errorf("json request target %s contains unsupported anonymous embedded field %s; use named fields", typ, field.Name)
		}
		if field.PkgPath != "" {
			continue
		}
		name := tagName
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		known[strings.ToLower(name)] = struct{}{}
	}
	filtered := make(map[string]json.RawMessage, len(body))
	for name, value := range body {
		if _, ok := known[strings.ToLower(name)]; ok {
			filtered[name] = value
		}
	}
	return filtered, nil
}
