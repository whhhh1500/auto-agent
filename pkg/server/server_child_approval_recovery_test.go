package server

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/internal/testdb"
	"github.com/whhhh1500/auto-agent/pkg/core"
	"github.com/whhhh1500/auto-agent/pkg/extensions/subagent"
	"github.com/whhhh1500/auto-agent/pkg/storage"
)

const serialChildApprovalID = "serial.child_approval"

type serialChildApprovalTool struct{ effects *atomic.Int32 }

func serialChildApprovalDecisionStatus(client *http.Client, baseURL, approvalID string) (int, error) {
	request, err := http.NewRequest(http.MethodPost, baseURL+"/v1/admin/approvals/"+approvalID+"/decision", strings.NewReader(`{"decision":"approved"}`))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	return response.StatusCode, nil
}

func (tool serialChildApprovalTool) Manifest() core.CapabilityManifest {
	schema := map[string]any{"type": "object", "properties": map[string]any{
		"n": map[string]any{"type": "integer"},
	}, "required": []any{"n"}, "additionalProperties": false}
	return core.CapabilityManifest{
		ID: serialChildApprovalID, Version: "1", Name: serialChildApprovalID,
		Kind: core.KindTool, Contract: "harness.tool/v1", Idempotent: true,
		RequiresApproval: true, RequiredPermissions: []core.Permission{core.PermWrite},
		InputSchema: schema, Tool: &core.ToolExposure{Parameters: schema},
	}
}

func (tool serialChildApprovalTool) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	tool.effects.Add(1)
	return core.CapabilityResult{Content: "child approval effect complete", OK: true}, nil
}

// serialChildApprovalModel selects the parent delegation and child approval
// calls by the exposed tool surface, then returns the corresponding result.
type serialChildApprovalModel struct{}

func (serialChildApprovalModel) Provider() string { return "serial-child-approval" }

func (serialChildApprovalModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	parent := false
	for _, tool := range options.Tools {
		if tool.Name == "serial.delegate" {
			parent = true
			break
		}
	}
	for _, message := range options.Messages {
		if message.Role == core.RoleTool {
			text := "child approval complete"
			if parent {
				text = "parent approval complete"
			}
			emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: text})
			emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
			return nil
		}
	}
	call := core.ToolCall{ID: "child-approval-call", Name: serialChildApprovalID, Args: map[string]any{"n": 1}}
	if parent {
		call = core.ToolCall{ID: "parent-delegate-call", Name: "serial.delegate", Args: map[string]any{"prompt": "complete the child approval"}}
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}

func configureSerialChildApproval(t *testing.T, api *serialAuditedAPI, effects *atomic.Int32) *storage.SQLDelegationLinkStore {
	t.Helper()
	serialRegister(t, api, serialChildApprovalTool{effects: effects})
	serialProfile(t, api, "serial.child_approval", "Call serial.child_approval exactly once with n 1. After approval, reply with its exact result.", serialChildApprovalID)
	links, err := storage.NewSQLDelegationLinkStore(api.db, storage.SQLDialectPostgres)
	if err != nil {
		t.Fatal(err)
	}
	delegate, err := subagent.NewCapability(api.server.runtime, "serial.delegate", subagent.Options{
		ProfileID: "serial.child_approval", Sessions: api.sessions, Links: links, MaxDepth: 2, DelegationMaxToolCalls: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	serialRegister(t, api, delegate)
	serialProfile(t, api, "serial.parent_approval", "Call serial.delegate exactly once to complete the child approval. After the child returns, reply with its exact result.", "serial.delegate", serialChildApprovalID)
	return links
}

func TestPostgresChildApprovalResumesParentOnReplacementInstance(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "serial-offline-fixture")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	open := testdb.Postgres(t)
	var effects atomic.Int32
	model := serialChildApprovalModel{}
	configure := func() (*serialAuditedAPI, *storage.SQLDelegationLinkStore) {
		api := newSerialAuditedAPI(t, open(), model, &effects)
		return api, configureSerialChildApproval(t, api, &effects)
	}

	first, links := configure()
	client := &http.Client{Timeout: 20 * time.Second}
	parentSession := serialSession(t, first, "serial.parent_approval")
	var parentRun storage.RunRecord
	if err := acceptanceRequest(client, http.MethodPost, first.http.URL+"/v1/sessions/"+parentSession+"/runs/async", map[string]any{"message": "Delegate the required child approval."}, &parentRun); err != nil {
		t.Fatal(err)
	}
	if claimed, err := first.server.RunWorkerOnce(ctx, "child-approval-first"); err != nil || !claimed {
		t.Fatalf("first worker claimed=%t err=%v", claimed, err)
	}
	parentPaused, err := first.queue.GetRun(ctx, parentRun.RunID)
	if err != nil || parentPaused.Status != storage.RunStatusWaitingApproval || effects.Load() != 0 {
		t.Fatalf("parent pause status=%q effects=%d err=%v", parentPaused.Status, effects.Load(), err)
	}
	pending, err := first.approvals.ListApprovals(ctx, storage.ApprovalFilter{TenantID: "acceptance-tenant", Status: core.ApprovalPending})
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending approvals=%d err=%v", len(pending), err)
	}
	rows, err := links.List(ctx, subagent.DelegationLinkFilter{ParentSessionID: parentSession, ParentRunID: parentRun.RunID, TenantID: "acceptance-tenant", Limit: 2})
	if err != nil || len(rows) != 1 {
		t.Fatalf("delegation links=%d err=%v", len(rows), err)
	}
	link := rows[0]
	if pending[0].SessionID != link.ChildSessionID || pending[0].RunID != link.ChildRunID {
		t.Fatalf("approval identity session=%s run=%s differs from child session=%s run=%s", pending[0].SessionID, pending[0].RunID, link.ChildSessionID, link.ChildRunID)
	}
	child, err := first.sessions.Load(ctx, link.ChildSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if status, found := child.RunStatus(link.ChildRunID); !found || status != core.RunWaitingApproval {
		t.Fatalf("child pause status=%q found=%t", status, found)
	}
	first.close(t)

	second, recoveredLinks := configure()
	if err := acceptanceRequest(client, http.MethodPost, second.http.URL+"/v1/admin/approvals/"+pending[0].ID+"/decision", map[string]any{"decision": "approved"}, nil); err != nil {
		t.Fatal(err)
	}
	if claimed, err := second.server.RunWorkerOnce(ctx, "child-approval-replacement"); err != nil || !claimed {
		t.Fatalf("replacement worker claimed=%t err=%v", claimed, err)
	}
	parentDone, err := second.queue.GetRun(ctx, parentRun.RunID)
	if err != nil || parentDone.Status != string(core.RunCompleted) || effects.Load() != 1 {
		t.Fatalf("replacement parent status=%q effects=%d err=%v", parentDone.Status, effects.Load(), err)
	}
	rows, err = recoveredLinks.List(ctx, subagent.DelegationLinkFilter{ParentSessionID: parentSession, ParentRunID: parentRun.RunID, TenantID: "acceptance-tenant", Limit: 2})
	if err != nil || len(rows) != 1 || rows[0] != link {
		t.Fatalf("replacement link=%#v want=%#v err=%v", rows, link, err)
	}
	parent, err := second.sessions.Load(ctx, parentSession)
	if err != nil {
		t.Fatal(err)
	}
	if status, found := parent.RunStatus(parentRun.RunID); !found || status != core.RunCompleted {
		t.Fatalf("replacement parent session status=%q found=%t", status, found)
	}
	child, err = second.sessions.Load(ctx, link.ChildSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if status, found := child.RunStatus(link.ChildRunID); !found || status != core.RunCompleted {
		t.Fatalf("replacement child session status=%q found=%t", status, found)
	}
	approved, err := second.approvals.ListApprovals(ctx, storage.ApprovalFilter{TenantID: "acceptance-tenant", Status: core.ApprovalApproved})
	if err != nil || len(approved) != 1 || approved[0].ID != pending[0].ID {
		t.Fatalf("approved records=%d first=%#v err=%v", len(approved), approved, err)
	}
	if effects.Load() != 1 {
		t.Fatalf("child approval effect calls=%d want 1", effects.Load())
	}
	if err := acceptanceRequest(client, http.MethodPost, second.http.URL+"/v1/admin/approvals/"+pending[0].ID+"/decision", map[string]any{"decision": "approved"}, nil); err != nil {
		t.Fatal(err)
	}
	if claimed, err := second.server.RunWorkerOnce(ctx, "child-approval-duplicate"); err != nil || claimed || effects.Load() != 1 {
		t.Fatalf("duplicate decision claimed=%t effects=%d err=%v", claimed, effects.Load(), err)
	}
	t.Logf("child approval replacement parent_session=%s parent_run=%s child_session=%s child_run=%s approval_id=%s effects=%d", parentSession, parentRun.RunID, link.ChildSessionID, link.ChildRunID, pending[0].ID, effects.Load())
}

func TestPostgresChildApprovalRefusesUnsafeReplacementTarget(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *serialAuditedAPI, subagent.Link, storage.RunRecord)
	}{
		{
			name: "cancelled_parent",
			mutate: func(t *testing.T, api *serialAuditedAPI, _ subagent.Link, parent storage.RunRecord) {
				t.Helper()
				if _, err := api.db.ExecContext(context.Background(), `UPDATE run_control SET cancel_requested=1 WHERE run_id=$1`, parent.RunID); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "link_identity_mismatch",
			mutate: func(t *testing.T, api *serialAuditedAPI, link subagent.Link, _ storage.RunRecord) {
				t.Helper()
				if _, err := api.db.ExecContext(context.Background(), `UPDATE delegation_links SET tenant_id='other-tenant' WHERE child_session_id=$1`, link.ChildSessionID); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "nested_link",
			mutate: func(t *testing.T, api *serialAuditedAPI, link subagent.Link, _ storage.RunRecord) {
				t.Helper()
				if _, err := api.db.ExecContext(context.Background(), `UPDATE delegation_links SET depth=2 WHERE child_session_id=$1`, link.ChildSessionID); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "missing_parent_queue",
			mutate: func(t *testing.T, api *serialAuditedAPI, _ subagent.Link, parent storage.RunRecord) {
				t.Helper()
				if _, err := api.db.ExecContext(context.Background(), `DELETE FROM run_queue WHERE run_id=$1`, parent.RunID); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("HARNESS_LLM_MODEL", "serial-offline-fixture")
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			open := testdb.Postgres(t)
			var effects atomic.Int32
			configure := func() (*serialAuditedAPI, *storage.SQLDelegationLinkStore) {
				api := newSerialAuditedAPI(t, open(), serialChildApprovalModel{}, &effects)
				return api, configureSerialChildApproval(t, api, &effects)
			}
			first, links := configure()
			client := &http.Client{Timeout: 20 * time.Second}
			parentSession := serialSession(t, first, "serial.parent_approval")
			var parentRun storage.RunRecord
			if err := acceptanceRequest(client, http.MethodPost, first.http.URL+"/v1/sessions/"+parentSession+"/runs/async", map[string]any{"message": "Delegate the required child approval."}, &parentRun); err != nil {
				t.Fatal(err)
			}
			if claimed, err := first.server.RunWorkerOnce(ctx, "child-approval-first"); err != nil || !claimed {
				t.Fatalf("first worker claimed=%t err=%v", claimed, err)
			}
			pending, err := first.approvals.ListApprovals(ctx, storage.ApprovalFilter{TenantID: "acceptance-tenant", Status: core.ApprovalPending})
			if err != nil || len(pending) != 1 || effects.Load() != 0 {
				t.Fatalf("pending approvals=%d effects=%d err=%v", len(pending), effects.Load(), err)
			}
			rows, err := links.List(ctx, subagent.DelegationLinkFilter{ParentSessionID: parentSession, ParentRunID: parentRun.RunID, TenantID: "acceptance-tenant", Limit: 2})
			if err != nil || len(rows) != 1 {
				t.Fatalf("delegation links=%d err=%v", len(rows), err)
			}
			test.mutate(t, first, rows[0], parentRun)
			first.close(t)

			second, _ := configure()
			status, err := serialChildApprovalDecisionStatus(client, second.http.URL, pending[0].ID)
			if err != nil {
				t.Fatalf("unsafe child approval decision transport failed: %v", err)
			}
			if status != http.StatusConflict {
				t.Fatalf("unsafe child approval decision status=%d want=%d", status, http.StatusConflict)
			}
			stillPending, err := second.approvals.ListApprovals(ctx, storage.ApprovalFilter{TenantID: "acceptance-tenant", Status: core.ApprovalPending})
			if err != nil || len(stillPending) != 1 || stillPending[0].ID != pending[0].ID || effects.Load() != 0 {
				t.Fatalf("pending approvals=%#v effects=%d err=%v", stillPending, effects.Load(), err)
			}
			parent, err := second.queue.GetRun(ctx, parentRun.RunID)
			if err != nil || parent.Status != storage.RunStatusWaitingApproval {
				t.Fatalf("unsafe target changed parent status=%q err=%v", parent.Status, err)
			}
		})
	}
}
