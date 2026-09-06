// Restore-time reapplication of journaled dynamic bindings, and the
// retention loop for bounded tables.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cc-auto-agent/harness-core/pkg/extensions/runner"
	"github.com/cc-auto-agent/harness-core/pkg/storage"
	"log/slog"
	"time"

	core "github.com/cc-auto-agent/harness-core/pkg/core"
)

type jsonRawMessage = json.RawMessage

func jsonUnmarshalBinding(raw json.RawMessage, target any) error {
	return json.Unmarshal(raw, target)
}

func slogInt64(k string, v int64) slog.Attr { return slog.Int64(k, v) }

// RestoreBindings re-applies journaled dynamic bindings after a restart.
// Policy layers, capability disables, HTTP and runner capability binds, and env
// credentials are remounted; static credential values are absent from the
// journal by design and must be re-bound by an operator.
func (s *Server) RestoreBindings(ctx context.Context) error {
	if s.journal == nil {
		s.setReadyError(nil)
		return nil
	}
	records, err := s.journal.List(ctx)
	if err != nil {
		s.setReadyError(err)
		return err
	}
	failures := []error{}
	type restoredBinding struct {
		record  storage.BindingRecord
		payload any
		scope   core.ScopePath
		unmount func()
	}
	staged := []restoredBinding{}
	fail := func(record storage.BindingRecord, cause error) {
		wrapped := fmt.Errorf("restore binding %s (%s): %w", record.ID, record.Kind, cause)
		failures = append(failures, wrapped)
		if s.logger != nil {
			s.logger.Error("restore binding failed", slogString("binding", record.ID), slogString("kind", record.Kind), slogString("error", cause.Error()))
		}
	}
	for _, record := range records {
		var payload struct {
			Scope                         string                   `json:"scope"`
			Allow                         []string                 `json:"allow"`
			Deny                          []string                 `json:"deny"`
			MaxSteps                      *int                     `json:"max_steps"`
			MaxToolCalls                  *int                     `json:"max_tool_calls"`
			Capability                    string                   `json:"capability"`
			Ref                           string                   `json:"ref"`
			EnvName                       string                   `json:"env_name"`
			Manifest                      *core.CapabilityManifest `json:"manifest"`
			Runtime                       string                   `json:"runtime"`
			Entrypoint                    string                   `json:"entrypoint"`
			Method                        string                   `json:"method"`
			Headers                       map[string]string        `json:"headers"`
			Workdir                       string                   `json:"workdir"`
			Sandbox                       core.SandboxPolicy       `json:"sandbox"`
			Writes                        bool                     `json:"writes"`
			RuntimeImplementationRevision string                   `json:"runtime_implementation_revision"`
			Replace                       bool                     `json:"replace"`
			Layer                         *core.AgentProfileLayer  `json:"layer"`
		}
		if err := unmarshalBindingPayload(record.Payload, &payload); err != nil {
			fail(record, fmt.Errorf("decode payload: %w", err))
			continue
		}
		scope, err := storage.ParseScopePath(payload.Scope)
		if err != nil {
			fail(record, fmt.Errorf("parse scope: %w", err))
			continue
		}
		restoredPayload := any(payload)
		var unmount func()
		switch record.Kind {
		case profileBindingKind:
			if payload.Layer == nil || payload.Layer.ProfileID == "" {
				fail(record, fmt.Errorf("profile layer is missing"))
				continue
			}
			payload.Layer.Scope = scope
			if unmount, err = s.runtime.Profiles.Mount(*payload.Layer); err != nil {
				fail(record, err)
				continue
			}
			restoredPayload = profileBindingPayload{Scope: scope.String(), Layer: core.CloneAgentProfileLayer(*payload.Layer)}
		case "policy":
			layer := core.PolicyLayer{
				Scope:            scope,
				AllowPermissions: toPermissions(payload.Allow),
				DenyPermissions:  toPermissions(payload.Deny),
				MaxSteps:         payload.MaxSteps,
				MaxToolCalls:     payload.MaxToolCalls,
			}
			registry := s.policyRegistry()
			if registry == nil {
				fail(record, fmt.Errorf("policy registry is unavailable"))
				continue
			}
			if unmount, err = registry.Mount(layer); err != nil {
				fail(record, err)
				continue
			}
		case "disable":
			manifest := core.CapabilityManifest{ID: payload.Capability, Version: "admin"}
			if unmount, err = s.runtime.Capabilities.Mount(core.CapabilityBinding{
				Scope: scope, Mode: core.BindingDisable, Manifest: manifest,
			}); err != nil {
				fail(record, err)
				continue
			}
		case "capability":
			if payload.Manifest == nil {
				fail(record, fmt.Errorf("manifest is missing"))
				continue
			}
			runtimeID, runtimeVersion, selectorErr := capabilityRuntimeSelector(payload.Runtime)
			if selectorErr != nil {
				fail(record, selectorErr)
				continue
			}
			if payload.RuntimeImplementationRevision == "" {
				if runtimeVersion != "1" || (runtimeID != "http" && runtimeID != "runner" && runtimeID != "subagent") {
					fail(record, fmt.Errorf("runtime implementation revision is missing"))
					continue
				}
			} else if s.capabilityRuntimes == nil {
				fail(record, fmt.Errorf("capability runtime registry is unavailable"))
				continue
			} else if current, revErr := s.capabilityRuntimes.Revision(runtimeID, runtimeVersion); revErr != nil || current != payload.RuntimeImplementationRevision {
				fail(record, fmt.Errorf("runtime implementation revision mismatch"))
				continue
			}
			mounted, mountedUnmount, mountErr := s.mountDynamicCapability(ctx, dynamicCapabilityMount{
				Scope: scope, Manifest: *payload.Manifest, Replace: payload.Replace,
				Runtime: payload.Runtime, Entrypoint: payload.Entrypoint,
				Method: payload.Method, Headers: payload.Headers, Workdir: payload.Workdir,
				Sandbox: payload.Sandbox, Writes: payload.Writes, RuntimeImplementationRevision: payload.RuntimeImplementationRevision,
			})
			if mountErr != nil {
				fail(record, mountErr)
				continue
			}
			unmount = mountedUnmount
			restoredPayload = mounted.journalPayload()
			if unmount == nil {
				fail(record, fmt.Errorf("capability mount returned no unmount function"))
				continue
			}
		case "credential":
			registry := s.credentialRegistry()
			if registry == nil || payload.EnvName == "" {
				fail(record, fmt.Errorf("credential registry or environment name is unavailable"))
				continue
			}
			unmount, err = registry.Mount(core.CredentialBinding{
				Scope: scope, Ref: core.CredentialRef(payload.Ref),
				Mode:     core.CredentialProvide,
				Provider: core.EnvironmentCredentialProvider{Name: payload.EnvName},
			})
			if err != nil {
				fail(record, err)
				continue
			}
		default:
			fail(record, fmt.Errorf("unknown binding kind"))
			continue
		}
		if unmount != nil {
			staged = append(staged, restoredBinding{record: record, payload: restoredPayload, scope: scope, unmount: unmount})
		}
	}
	restoreErr := errors.Join(failures...)
	if restoreErr != nil {
		for index := len(staged) - 1; index >= 0; index-- {
			staged[index].unmount()
		}
		s.setReadyError(restoreErr)
		return restoreErr
	}
	for _, restored := range staged {
		if _, err := s.adminStateFor().add(restored.record.Kind, restored.record.ID, restored.payload, restored.scope, restored.unmount); err != nil {
			failures = append(failures, fmt.Errorf("restore binding %s (%s): %w", restored.record.ID, restored.record.Kind, err))
			for index := len(staged) - 1; index >= 0; index-- {
				item := staged[index]
				if binding, ok := s.adminStateFor().remove(item.record.ID); ok {
					if binding.unmount != nil {
						binding.unmount()
					}
					continue
				}
				item.unmount()
			}
			restoreErr = errors.Join(failures...)
			s.setReadyError(restoreErr)
			return restoreErr
		}
	}
	s.setReadyError(restoreErr)
	return restoreErr
}

func unmarshalBindingPayload(raw jsonRawMessage, target any) error {
	return jsonUnmarshalBinding(raw, target)
}

// StartRetentionLoop periodically prunes bounded SQL tables and old,
// non-idempotent terminal private-runner tasks. It starts when either cleanup
// surface is available and stops when ctx is cancelled. Uncertain tool
// outcomes and keyed runner tasks are intentionally retained.
func (s *Server) StartRetentionLoop(ctx context.Context, every time.Duration, auditRetention time.Duration) {
	if !s.retentionLoopEnabled() {
		return
	}
	if every <= 0 {
		every = time.Hour
	}
	if auditRetention <= 0 {
		auditRetention = 90 * 24 * time.Hour
	}
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cutoff := time.Now().UTC().Add(-auditRetention)
				s.pruneRetentionOnce(ctx, cutoff)
			}
		}
	}()
}

func (s *Server) retentionLoopEnabled() bool {
	if s.retention != nil {
		return true
	}
	if s.runners == nil || s.runners.Store == nil {
		return false
	}
	_, ok := s.runners.Store.(runner.TaskRetention)
	return ok
}

// pruneRetentionOnce performs one synchronous cleanup pass. Callers provide a
// single cutoff so all age-based tables observe the same retention boundary.
func (s *Server) pruneRetentionOnce(ctx context.Context, cutoff time.Time) {
	if s.retention != nil {
		deleted, err := s.retention.PruneAudit(ctx, cutoff)
		s.logRetention("audit_events", deleted, err)
		deleted, err = s.retention.PruneHits(ctx, cutoff)
		s.logRetention("obs_hits", deleted, err)
		deleted, err = s.retention.PruneExpiredLeases(ctx)
		s.logRetention("session_leases", deleted, err)
		deleted, err = s.retention.PruneToolInvocations(ctx, cutoff)
		s.logRetention("tool_invocations", deleted, err)
		deleted, err = s.retention.PruneApprovals(ctx, cutoff)
		s.logRetention("approval_requests", deleted, err)
		deleted, err = s.retention.PruneRunSubmissions(ctx, cutoff)
		s.logRetention("run_submissions", deleted, err)
	}
	if s.runners == nil {
		return
	}
	deleted, err := s.runners.PruneTasks(ctx, cutoff)
	if errors.Is(err, runner.ErrRetentionUnsupported) {
		return
	}
	s.logRetention("runner_tasks", deleted, err)
}

func (s *Server) logRetention(table string, deleted int64, err error) {
	if err != nil {
		if s.logger != nil {
			s.logger.Error("retention prune failed", slogString("table", table), slogString("error", err.Error()))
		}
		return
	}
	if deleted > 0 && s.logger != nil {
		s.logger.Info("retention prune", slogString("table", table), slogInt64("deleted", deleted))
	}
}
