package openai

import (
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"sort"

	"github.com/whhhh1500/auto-agent/pkg/app/modelexecution"
)

// OpenAI tool names are intentionally an adapter concern. The execution
// contract, catalog prose, and guarded invocation continue to use the
// capability's original name.
const toolAliasPrefix = "fn_"

var toolAliasEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

type toolNameAliases struct {
	toWire     map[string]string
	fromWire   map[string]string // current advertised tools only
	transcoded map[string]bool
}

func newToolNameAliases(request modelexecution.Request) (*toolNameAliases, error) {
	names := make([]string, 0, len(request.Tools))
	advertised := make(map[string]struct{}, len(request.Tools))
	for _, tool := range request.Tools {
		names = append(names, tool.Name)
		advertised[tool.Name] = struct{}{}
	}
	for _, message := range request.Messages {
		for _, call := range message.ToolCalls {
			names = append(names, call.Name)
		}
	}
	return newToolNameAliasesForNames(names, advertised)
}

func newToolNameAliasesForNames(names []string, advertised map[string]struct{}) (*toolNameAliases, error) {
	unique := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			return nil, fmt.Errorf("openai tool name is empty")
		}
		unique[name] = struct{}{}
	}
	sorted := make([]string, 0, len(unique))
	for name := range unique {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)

	aliases := &toolNameAliases{
		toWire:     make(map[string]string, len(sorted)),
		fromWire:   make(map[string]string, len(sorted)),
		transcoded: make(map[string]bool, len(sorted)),
	}
	claimedWire := make(map[string]string, len(sorted))
	for _, name := range sorted {
		wire := name
		if !isOpenAIWireToolName(name) {
			wire = encodedToolName(name)
			aliases.transcoded[name] = true
		}
		if existing, exists := claimedWire[wire]; exists && existing != name {
			return nil, fmt.Errorf("openai tool wire-name collision")
		}
		claimedWire[wire] = name
		aliases.toWire[name] = wire
		if _, ok := advertised[name]; ok {
			if existing, exists := aliases.fromWire[wire]; exists && existing != name {
				return nil, fmt.Errorf("openai advertised tool wire-name collision")
			}
			aliases.fromWire[wire] = name
		}
	}
	return aliases, nil
}

func isOpenAIWireToolName(name string) bool {
	if len(name) == 0 || len(name) > 64 {
		return false
	}
	for _, value := range []byte(name) {
		if (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') || value == '_' || value == '-' {
			continue
		}
		return false
	}
	return true
}

func encodedToolName(name string) string {
	digest := sha256.Sum256([]byte(name))
	return toolAliasPrefix + toolAliasEncoding.EncodeToString(digest[:])
}

func (a *toolNameAliases) wire(name string) (string, error) {
	if a == nil {
		return name, nil
	}
	wire, ok := a.toWire[name]
	if !ok {
		return "", fmt.Errorf("openai tool name is not in request")
	}
	return wire, nil
}

func (a *toolNameAliases) internal(wire string) (string, error) {
	// Parsing helpers used by protocol tests retain their historical identity
	// behavior. Execute always supplies a non-nil mapping and rejects names
	// which were not offered in this invocation.
	if a == nil {
		return wire, nil
	}
	name, ok := a.fromWire[wire]
	if !ok {
		return "", fmt.Errorf("openai response used an unknown tool name")
	}
	return name, nil
}

func (a *toolNameAliases) description(name, description string) string {
	if a != nil && a.transcoded[name] {
		return description + "\n\nInternal capability ID: " + name
	}
	return description
}
