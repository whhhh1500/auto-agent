package server

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/whhhh1500/auto-agent/internal/testdb"
	"github.com/whhhh1500/auto-agent/pkg/core"
)

const serialLibraryID = "harness.tool.library"

// Distinct read-only business catalogs, without padding tool descriptions to
// force a token win. Both arms expose precisely the same underlying snapshot.
var serialDisclosureCatalog = []struct{ id, description string }{
	{"warehouse.stock_lookup", "Retrieve the stock verification marker for an inventory item. Supply the item code as record_id. Returns the warehouse's current verification marker, not an estimate or a calculation."},
	{"billing.invoice_lookup", "Retrieve the issued invoice and payment due date for an invoice record_id. This catalog contains billing documents and totals; use it to inspect a particular invoice."},
	{"support.ticket_lookup", "Retrieve customer support ticket status, assigned queue and last response for ticket record_id. This catalog concerns support cases and their resolution history."},
	{"shipping.parcel_lookup", "Retrieve the carrier tracking checkpoint and estimated delivery date for parcel record_id. Use this for a parcel already handed to a carrier."},
	{"finance.refund_lookup", "Retrieve refund settlement status and transaction reference for refund record_id. This catalog tracks reimbursements to the original payment method."},
	{"crm.customer_lookup", "Retrieve a customer account's contact details and service tier using customer record_id. This catalog holds the customer profile and account preferences."},
	{"hr.employee_lookup", "Retrieve employee department, job title and office assignment for employee record_id. Use this catalog to inspect organizational directory information."},
	{"calendar.event_lookup", "Retrieve the scheduled start time, duration and meeting room for calendar event record_id. This catalog contains scheduled appointments and meeting details."},
	{"procurement.order_lookup", "Retrieve supplier purchase order approval and expected arrival date for order record_id. This catalog tracks procurement orders before receiving goods."},
	{"contracts.agreement_lookup", "Retrieve agreement effective dates and renewal notice period for agreement record_id. This catalog provides contract administration information."},
	{"assets.device_lookup", "Retrieve assigned owner, warranty expiry and hardware model for device record_id. This catalog tracks company equipment and service warranties."},
	{"security.incident_lookup", "Retrieve incident severity, containment state and responder assignment for incident record_id. This catalog tracks security incident response cases."},
	{"quality.inspection_lookup", "Retrieve inspection outcome, inspector and test date for inspection record_id. This catalog contains completed quality inspection reports."},
	{"production.batch_lookup", "Retrieve manufacturing batch start date and production-line status for batch record_id. This catalog tracks manufacturing operations."},
	{"facilities.maintenance_lookup", "Retrieve maintenance schedule, technician assignment and completion status for work-order record_id. This catalog concerns facility repairs."},
	{"travel.booking_lookup", "Retrieve itinerary, reservation status and departure time for booking record_id. This catalog contains business travel reservations."},
	{"training.course_lookup", "Retrieve course schedule, enrollment count and instructor for course record_id. This catalog concerns internal training classes."},
	{"legal.case_lookup", "Retrieve legal case docket, filing date and assigned counsel for case record_id. This catalog contains legal matter administration records."},
	{"marketing.campaign_lookup", "Retrieve campaign launch date, channel and budget allocation for campaign record_id. This catalog concerns campaign planning and delivery."},
	{"analytics.report_lookup", "Retrieve report refresh time, owner and reporting period for report record_id. This catalog describes analytical report publication metadata."},
	{"subscriptions.plan_lookup", "Retrieve subscription renewal date, active plan and billing cadence for subscription record_id. This catalog concerns recurring service subscriptions."},
	{"research.study_lookup", "Retrieve study phase, project owner and planned completion date for study record_id. This catalog tracks research projects."},
	{"compliance.audit_lookup", "Retrieve audit schedule, reviewer assignment and finding status for audit record_id. This catalog concerns compliance review activities."},
	{"content.article_lookup", "Retrieve editorial review stage, publication date and author for article record_id. This catalog tracks content publication workflows."},
}

type serialDisclosureTool struct {
	index   int
	markers map[string]string
	effects *atomic.Int32
}

func (tool serialDisclosureTool) Manifest() core.CapabilityManifest {
	entry := serialDisclosureCatalog[tool.index]
	schema := map[string]any{"type": "object", "properties": map[string]any{
		"record_id": map[string]any{"type": "string", "description": "The exact record identifier from the user request.", "minLength": 1, "maxLength": 64},
	}, "required": []any{"record_id"}, "additionalProperties": false}
	return core.CapabilityManifest{ID: entry.id, Version: "1", Name: entry.id, Description: entry.description, Kind: core.KindConnector,
		Contract: "harness.tool/v1", InputSchema: schema, Idempotent: true, Tool: &core.ToolExposure{Description: entry.description, Parameters: schema}}
}

func (tool serialDisclosureTool) Execute(_ context.Context, request core.CapabilityRequest) (core.CapabilityResult, error) {
	if err := core.RequireAcceptedInvocation(request); err != nil {
		return core.CapabilityResult{}, err
	}
	if tool.index != 0 {
		return core.CapabilityResult{}, fmt.Errorf("wrong business catalog selected")
	}
	record, _ := request.Args["record_id"].(string)
	marker, ok := tool.markers[record]
	if !ok {
		return core.CapabilityResult{}, fmt.Errorf("unknown inventory fixture record")
	}
	tool.effects.Add(1)
	return core.CapabilityResult{OK: true, Content: marker}, nil
}

type serialDisclosureArm struct {
	Disclosed    bool                     `json:"disclosed"`
	RunIDs       []string                 `json:"run_ids"`
	InputTokens  int64                    `json:"input_tokens"`
	OutputTokens int64                    `json:"output_tokens"`
	ElapsedMS    int64                    `json:"elapsed_ms"`
	ToolCalls    int                      `json:"tool_calls"`
	Requests     []serialModelObservation `json:"requests"`
}

func serialToolDisclosureSmall(t *testing.T, model *serialAcceptanceModel) {
	serialToolDisclosureComparison(t, model, 4)
}
func serialToolDisclosureLarge(t *testing.T, model *serialAcceptanceModel) {
	serialToolDisclosureComparison(t, model, 24)
}

func serialToolDisclosureComparison(t *testing.T, model *serialAcceptanceModel, size int) {
	records := []string{"ORION-42", "LYRA-17"}
	markers := map[string]string{records[0]: serialMarker(t), records[1]: serialMarker(t)}
	var arms []serialDisclosureArm
	for _, disclosed := range []bool{false, true} {
		open := testdb.Postgres(t)
		var effects atomic.Int32
		configure := func() *serialAuditedAPI {
			api := newSerialAuditedAPI(t, open(), model, &effects)
			ids := make([]string, size)
			for i := range ids {
				tool := serialDisclosureTool{index: i, markers: markers, effects: &effects}
				ids[i] = tool.Manifest().ID
				serialRegister(t, api, tool)
			}
			serialProfile(t, api, "serial.disclosure", "Retrieve the user's requested value with exactly one business lookup and return only its exact result. Never invent or calculate a verification marker. If the needed tool is already declared, call it directly. If only a tool library is available, search for the relevant tool, describe its qualified id, then call it. Do not use the library again once the needed tool is declared.", ids...)
			api.server.runtime.DiscloseTools = disclosed
			return api
		}
		api := configure()
		session := serialSession(t, api, "serial.disclosure")
		start := len(model.observations)
		started := time.Now()
		arm := serialDisclosureArm{Disclosed: disclosed}
		for turn, record := range records {
			if turn > 0 {
				history := serialAuditHistory(t, api, session)
				api.http.Close()
				if err := api.db.Close(); err != nil {
					t.Fatal(err)
				}
				api = configure()
				if !reflect.DeepEqual(history, serialAuditHistory(t, api, session)) {
					t.Fatal("discovery history changed after service/pool replacement")
				}
			}
			before := model.calls
			evidence := serialRun(t, api, session, "Retrieve the stock verification marker for inventory item "+record+". Reply only with that marker.", markers[record])
			serialWantTool(t, evidence, serialDisclosureCatalog[0].id, 1)
			if effects.Load() != int32(turn+1) || strings.TrimSpace(evidence.answer) != markers[record] {
				t.Fatal("wrong business result or duplicate business execution")
			}
			for _, result := range evidence.results {
				if !result.OK {
					t.Fatal("disclosure path contains a failed tool result")
				}
			}
			expectedCalls := 2
			if disclosed && turn == 0 {
				expectedCalls = 4 // search, describe, execute, answer
				serialWantTool(t, evidence, serialLibraryID, 2)
			} else {
				serialWantTool(t, evidence, serialLibraryID, 0)
			}
			if model.calls-before != expectedCalls {
				t.Fatalf("unexpected discovery protocol model_calls=%d want=%d", model.calls-before, expectedCalls)
			}
			arm.RunIDs = append(arm.RunIDs, evidence.runID)
			arm.ToolCalls += len(evidence.calls)
		}
		arm.ElapsedMS = time.Since(started).Milliseconds()
		arm.Requests = append([]serialModelObservation(nil), model.observations[start:]...)
		for _, request := range arm.Requests {
			arm.InputTokens += request.InputTokens
			arm.OutputTokens += request.OutputTokens
		}
		arms = append(arms, arm)
		t.Logf("disclosure catalog=%d disclosed=%t model_calls=%d tool_calls=%d input_tokens=%d output_tokens=%d elapsed_ms=%d correct_answers=2 business_effects=2", size, disclosed, len(arm.Requests), arm.ToolCalls, arm.InputTokens, arm.OutputTokens, arm.ElapsedMS)
		serialWriteAudit(t, fmt.Sprintf("disclosure_%d_comparison", size), map[string]any{"catalog_size": size, "turns_per_arm": 2, "arms": arms})
	}
}

func TestPostgresSerialToolDisclosure(t *testing.T) {
	t.Setenv("HARNESS_LLM_MODEL", "serial-offline-fixture")
	for _, size := range []int{4, 24} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			model := &serialAcceptanceModel{inner: serialDisclosureFixtureModel{}, test: t}
			serialToolDisclosureComparison(t, model, size)
		})
	}
}

type serialDisclosureFixtureModel struct{}

func (serialDisclosureFixtureModel) Provider() string { return "openai-compatible" }
func (serialDisclosureFixtureModel) Stream(_ context.Context, options core.GenerateOptions, emit func(core.StreamChunk)) error {
	last := options.Messages[len(options.Messages)-1]
	if last.Role == core.RoleTool && strings.HasPrefix(last.Content, "proof_") {
		emit(core.StreamChunk{Kind: core.StreamKindAssistant, Text: last.Content})
		emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishStop})
		return nil
	}
	var call core.ToolCall
	if len(options.Tools) == 1 && options.Tools[0].Name == serialLibraryID {
		call = core.ToolCall{ID: "library-search", Name: serialLibraryID, Args: map[string]any{"action": "search", "query": "stock verification marker"}}
		if last.Role == core.RoleTool {
			var result struct {
				Tools []struct{ ID string }
			}
			if err := json.Unmarshal([]byte(last.Content), &result); err != nil || len(result.Tools) == 0 || result.Tools[0].ID != serialDisclosureCatalog[0].id {
				return fmt.Errorf("search result missing target, or described tool was not declared on next request")
			}
			call = core.ToolCall{ID: "library-describe", Name: serialLibraryID, Args: map[string]any{"action": "describe", "id": result.Tools[0].ID}}
		}
	} else {
		record := ""
		for i := len(options.Messages) - 1; i >= 0; i-- {
			if options.Messages[i].Role == core.RoleUser {
				for _, candidate := range []string{"ORION-42", "LYRA-17"} {
					if strings.Contains(options.Messages[i].Content, candidate) {
						record = candidate
					}
				}
				break
			}
		}
		call = core.ToolCall{ID: "lookup-" + record, Name: serialDisclosureCatalog[0].id, Args: map[string]any{"record_id": record}}
	}
	declared := false
	for _, schema := range options.Tools {
		declared = declared || schema.Name == call.Name
	}
	if !declared {
		return fmt.Errorf("fixture refuses to call a tool absent from the model declarations")
	}
	emit(core.StreamChunk{Kind: core.StreamKindAssistant, ToolCall: &call})
	emit(core.StreamChunk{Kind: core.StreamKindFinish, FinishKind: core.FinishToolCalls})
	return nil
}
