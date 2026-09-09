package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/whhhh1500/auto-agent/internal/executionroute"
	appmodelsettings "github.com/whhhh1500/auto-agent/pkg/app/modelsettings"
	"github.com/whhhh1500/auto-agent/pkg/app/programmatic"
	core "github.com/whhhh1500/auto-agent/pkg/core"
)

const (
	generalProfileID           = "general"
	generalProfileName         = "通用智能体"
	generalProfileDescription  = "A general-purpose assistant that can recall and remember scoped facts, search knowledge, deliver approved notifications, and choose direct tools or bounded program execution when permitted."
	generalProfilePrompt       = "Be helpful, accurate, clear, and honest about uncertainty. Choose among the tools currently exposed: call an ordinary tool directly for a simple action or when the next decision requires your interpretation; use program.execute for deterministic, bounded loops, branches and data processing. When choosing program.execute, first obtain program bindings from program.catalog and follow the program tool description. If program.execute is not exposed, use the available direct tools. Treat returned tool outcomes as evidence before deciding the next step. Use memory recall only when it helps the current request; remember only durable user-relevant facts. Treat memory.forget as destructive: use it only when requested and explain that it requires approval. Search the knowledge base when it is relevant. Before sending a notification, inspect available channels and opaque targets, require a target, and explain that delivery requires approval. sandbox.exec accepts argv only (never a shell), follows the trusted provider's reported network policy, and writes verified artifacts under /artifacts for object-storage publication. Host-network mode provides no network-isolation guarantee; any offline notice is informational only. If the platform sandbox is unavailable, explain that execution is unavailable rather than using a host fallback."
	generalProfileMaxSteps     = core.DefaultMaxSteps
	generalProfileMaxToolCalls = core.DefaultMaxToolCalls
	legacyOpenAIProvider       = "openai"
)

// generalProfileSelectionSource adapts the database-first legacy model setting to
// the executable's base profile. A source only uses the environment after it
// has established that no database row exists.
type generalProfileSelectionSource func(context.Context) (core.ModelSelection, error)

// generalProfileController owns only this executable's base general profile.
// It deliberately has no SQL, HTTP, credential, or provider dependency; the
// composition root supplies the database-first model-selection source.
type generalProfileController struct {
	mu           sync.Mutex
	registry     *core.AgentProfileRegistry
	scope        core.ScopePath
	source       generalProfileSelectionSource
	capabilities []string
	unmount      func()
}

var _ appmodelsettings.PostSaveObserver = (*generalProfileController)(nil)

func newGeneralProfileController(registry *core.AgentProfileRegistry, scope core.ScopePath, source generalProfileSelectionSource, capabilitySets ...[]string) (*generalProfileController, error) {
	if registry == nil {
		return nil, fmt.Errorf("general profile controller requires a profile registry")
	}
	if scope.Depth() == 0 {
		return nil, fmt.Errorf("general profile controller requires a mount scope")
	}
	if source == nil {
		return nil, fmt.Errorf("general profile controller requires a model source")
	}
	if len(capabilitySets) > 1 {
		return nil, fmt.Errorf("general profile controller accepts one capability set")
	}
	var capabilityIDs []string
	if len(capabilitySets) == 1 {
		capabilityIDs = capabilitySets[0]
	}
	capabilities, err := immutableCapabilityIDs(capabilityIDs)
	if err != nil {
		return nil, err
	}
	return &generalProfileController{registry: registry, scope: scope, source: source, capabilities: capabilities}, nil
}

func immutableCapabilityIDs(ids []string) ([]string, error) {
	if len(ids) > core.MaxProfileCapabilities {
		return nil, fmt.Errorf("general profile capabilities exceed %d", core.MaxProfileCapabilities)
	}
	result := append([]string(nil), ids...)
	sort.Strings(result)
	for index, id := range result {
		if err := core.ValidateNamespacedID(id); err != nil {
			return nil, fmt.Errorf("general profile capability %q is invalid: %w", id, err)
		}
		if index > 0 && result[index-1] == id {
			return nil, fmt.Errorf("general profile capability %q is duplicated", id)
		}
	}
	return result, nil
}

// Mount installs a visible base layer when model configuration is absent. The
// empty selection then reaches the existing profile/model configuration
// validation when a run is attempted; the controller never invents a mock
// provider or model alias. A repository read failure is not absence and makes
// startup fail closed.
func (controller *generalProfileController) Mount(ctx context.Context) error {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	selection, err := controller.source(ctx)
	if err != nil {
		return fmt.Errorf("load model for general profile: %w", err)
	}
	return controller.replaceLocked(selection)
}

// AfterSave refreshes from the authoritative repository after a successful
// save. Re-reading under the controller lock makes concurrent post-save calls
// converge on the last durable configuration instead of an earlier observer
// argument. The observer argument contains no configuration, so API keys
// remain only in the settings repository.
func (controller *generalProfileController) AfterSave(ctx context.Context) error {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	selection, err := controller.source(ctx)
	if err != nil {
		return fmt.Errorf("load persisted model for general profile refresh: %w", err)
	}
	return controller.replaceLocked(selection)
}

// Close releases only the controller's current layer. It is useful during a
// graceful executable shutdown and makes a replacement controller explicit.
func (controller *generalProfileController) Close() {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.unmount != nil {
		controller.unmount()
		controller.unmount = nil
	}
}

func (controller *generalProfileController) replaceLocked(selection core.ModelSelection) error {
	name := generalProfileName
	description := generalProfileDescription
	maxSteps := generalProfileMaxSteps
	maxToolCalls := generalProfileMaxToolCalls
	next, err := controller.registry.Mount(core.AgentProfileLayer{
		Scope: controller.scope, ProfileID: generalProfileID,
		Name: &name, Description: &description, Model: &selection,
		MaxSteps: &maxSteps, MaxToolCalls: &maxToolCalls,
		Metadata: map[string]string{
			executionroute.RouteVersionKey: executionroute.RouteVersion,
			executionroute.RouteModeKey:    string(programmatic.RouteDirectOnly),
		},
		AddCapabilities: append([]string(nil), controller.capabilities...),
		PutFragments: []core.PromptFragment{{
			ID: "general.instructions", Section: core.PromptInstructions, Content: generalProfilePrompt,
		}},
	})
	if err != nil {
		return err
	}
	previous := controller.unmount
	controller.unmount = next
	if previous != nil {
		previous()
	}
	return nil
}

func generalProfileModelSource(repository appmodelsettings.Repository) generalProfileSelectionSource {
	return func(ctx context.Context) (core.ModelSelection, error) {
		if repository == nil {
			return unconfiguredGeneralModelSelection(), fmt.Errorf("model settings repository is not configured")
		}
		configuration, found, err := loadPersistedLLMConfig(ctx, repository)
		if err != nil {
			return unconfiguredGeneralModelSelection(), err
		}
		if found {
			return core.ModelSelection{Provider: string(configuration.Provider), Model: configuration.Model}, nil
		}
		return core.ModelSelection{Provider: string(appmodelsettings.ProviderOpenAI), Model: os.Getenv("HARNESS_LLM_MODEL")}, nil
	}
}

func unconfiguredGeneralModelSelection() core.ModelSelection {
	return core.ModelSelection{Provider: legacyOpenAIProvider}
}

func generalCapabilityIDs() []string {
	return []string{
		"memory.forget", "memory.recall", "memory.remember", "notify.channels", "notify.send", "notify.targets", "program.catalog", "program.execute", "rag.search", "sandbox.exec",
	}
}
