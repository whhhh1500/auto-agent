package support

import (
	"encoding/json"

	core "github.com/whhhh1500/auto-agent/pkg/core"
)

func ValidateCapabilityID(id string) error {
	return core.ValidateNamespacedID(id)
}

func ObjectSchema(properties map[string]any) map[string]any {
	return map[string]any{"type": "object", "properties": properties}
}

func DeniedResult(code, message string) core.CapabilityResult {
	return core.CapabilityResult{
		Content:  message,
		OK:       false,
		Metadata: map[string]any{"code": code},
	}
}

func JSONResult(value any) (core.CapabilityResult, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return core.CapabilityResult{}, err
	}
	return core.CapabilityResult{Content: string(encoded), OK: true}, nil
}

func StringSlice(value any) []string {
	raw, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if text, ok := item.(string); ok {
			out = append(out, text)
		}
	}
	return out
}

func ClonePrincipal(principal core.Principal) core.Principal {
	out := principal
	out.Grants = principal.Grants.Clone()
	out.Attributes = make(map[string]string, len(principal.Attributes))
	for key, value := range principal.Attributes {
		out.Attributes[key] = value
	}
	return out
}

func CloneJSONMap(input map[string]any) (map[string]any, error) {
	if input == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(encoded, &out); err != nil {
		return nil, err
	}
	return out, nil
}
