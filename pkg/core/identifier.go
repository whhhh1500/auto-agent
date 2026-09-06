package core

import (
	"fmt"
	"regexp"
	"strings"
)

var namespacedIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var approvalIDPattern = regexp.MustCompile(`^apr_[a-f0-9]{32,64}$`)

// ValidateNamespacedID validates stable public identifiers such as capability,
// credential and plugin IDs.
func ValidateNamespacedID(id string) error {
	if !namespacedIDPattern.MatchString(id) || !strings.Contains(id, ".") {
		return fmt.Errorf("id %q must be a namespaced identifier of at most 128 safe characters", id)
	}
	return nil
}

// ValidateApprovalID validates deterministic durable approval identifiers.
func ValidateApprovalID(id string) error {
	if !approvalIDPattern.MatchString(id) {
		return fmt.Errorf("approval id %q must use apr_ followed by 32-64 lowercase hex characters", id)
	}
	return nil
}

func validateModelSelection(selection ModelSelection) error {
	provider := strings.TrimSpace(selection.Provider)
	model := strings.TrimSpace(selection.Model)
	if provider == "" || len(provider) > 128 || containsControl(provider) {
		return fmt.Errorf("model provider is empty, too long, or contains control characters")
	}
	if model == "" || len(model) > 256 || containsControl(model) {
		return fmt.Errorf("model name is empty, too long, or contains control characters")
	}
	return nil
}

func containsControl(value string) bool {
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return true
		}
	}
	return false
}
