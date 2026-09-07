package console

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestConsoleEmbedsUnifiedEvidenceCursorControls(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("console status=%d body=%s", response.Code, response.Body.String())
	}
	body := consoleBundle(t, response.Body.String())
	for _, required := range []string{
		"统一 Evidence", "nextEvidence()", "previousEvidence()", "next_cursor", "/v1/admin/evidence", "/v1/admin/evidence/stats",
		"viewEvidence(e)", "record.detail_path", "对象详情",
		"evidenceDetailKind==='evaluation'", "得分 / Cases", "Candidate Revision", "原始 JSON",
		"created_after", "created_before", "evidenceQuery.status", "datetime-local", "loadEvidenceStats()", "evidenceStats",
		"Total", "Terminal", "Completion Rate", "Candidate", "Live", "Unassigned", "By Status",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("console missing %q", required)
		}
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("console app cache-control=%q", response.Header().Get("Cache-Control"))
	}
}

func TestConsoleEmbedsRuntimeSpecificCapabilityBindingControls(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("console status=%d body=%s", response.Code, response.Body.String())
	}
	body := consoleBundle(t, response.Body.String())
	for _, required := range []string{
		"<label>Runtime</label>", `x-model="capNew.runtime"`, "capabilityRuntimeID()",
		`runtime.id + '@' + runtime.version`, `<option :value="runtime.id + '@' + runtime.version"`,
		`x-if="capabilityRuntimeID() === 'http'"`, `x-if="capabilityRuntimeID() === 'runner'"`, `x-if="capabilityRuntimeID() === 'subagent'"`,
		"Private Runner 会接管执行", "const runtimeID = this.capabilityRuntimeID()",
		"? { runtime: this.capNew.runtime }", "return { runtime: this.capNew.runtime, entrypoint: this.capNew.url, method: this.capNew.method, headers }",
		"childProfile", "subagent.max_depth", "subagent.max_children", "subagent.max_tool_calls", "runtimeID === 'subagent'", "entrypoint: this.capNew.childProfile",
		"manifest.tool = manifest.tool || {}", "if (!manifest.tool.parameters)", "execution,",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("console missing %q", required)
		}
	}
}

func TestConsoleEmbedsDelegationCatalogWithoutSensitivePayloadFields(t *testing.T) {
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("console status=%d body=%s", response.Code, response.Body.String())
	}
	body := consoleBundle(t, response.Body.String())
	start := strings.Index(body, "<!-- Delegation relationships")
	end := strings.Index(body, "<!-- Runner Tasks -->")
	if start < 0 || end <= start {
		t.Fatal("delegation view is missing or outside main content")
	}
	view := body[start:end]
	for _, required := range []string{
		"Delegations / 委派关系", "delegationQuery", "parent_session_id", "parent_run_id", "child_session_id", "child_run_id", "child_status", "openDelegationSession",
	} {
		if !strings.Contains(view, required) {
			t.Fatalf("delegation console missing %q", required)
		}
	}
	if !strings.Contains(body, "/v1/admin/delegations?") {
		t.Fatal("delegation console does not call its metadata catalog")
	}
	for _, forbidden := range []string{"d.prompt", "d.args", "d.events", "d.result", "JSON.stringify(d)"} {
		if strings.Contains(view, forbidden) {
			t.Fatalf("delegation console must not expose sensitive field %q", forbidden)
		}
	}
	assertInlineJavaScriptSyntax(t, body)
}

func TestConsoleEmbedsProfileCapabilityAndApprovalWorkflows(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("console status=%d body=%s", response.Code, response.Body.String())
	}
	body := consoleBundle(t, response.Body.String())
	for _, required := range []string{
		"Profile Layer 管理", "profileEdit", "loadProfileEditor()", "copyProfileEditor()", "saveProfileEditor()",
		"/v1/admin/profiles/", "profile_id", "name", "description", "provider", "model",
		"add_capabilities", "remove_capabilities", "put_fragments", "Layer JSON",
		"manifestJSON", "manifestError", "serverError", "JSON.parse(this.capNew.manifestJSON", "tool.parameters",
		"Approval Inbox / 审批", "approvalFilter", "loadApprovals", "pending", "processed", "expired",
		"/v1/admin/approvals?", "/v1/admin/approvals/", "decision", "decideApproval", "approvalRunMatch",
		"runnerTaskActions?.retry?.allowed", "runnerRetry", "confirm: true", "reason", "/retry", "original_id", "retry_id",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("console missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"approvalDetail.request", "approvalDetail.args", "approvalDetail.tool_args",
		"JSON.stringify(approvalDetail", "JSON.stringify(runnerTaskDetail",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("console must not render sensitive payload %q", forbidden)
		}
	}
	for _, required := range []string{
		"const original = data.original || originalSummary", "const retry = data.retry || {}", "created: data.created === true",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("console retry response handling missing %q", required)
		}
	}
	assertInlineJavaScriptSyntax(t, body)
}

func TestConsoleEmbedsRunnerTasksMetadataControls(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("console status=%d body=%s", response.Code, response.Body.String())
	}
	body := consoleBundle(t, response.Body.String())
	for _, required := range []string{
		"Runner Tasks / 运行器任务", "runner-tasks", "runnerTasks", "runnerTaskQuery", "runnerTaskPage", "runnerTaskDetail",
		"Tenant", "Subject", "Scope", "Capability", "Worker", "State", "Limit", "上一页", "下一页",
		"this.api('GET', '/v1/admin/runners/tasks?' + params)",
		"this.api('GET', '/v1/admin/runners/tasks/' + encodeURIComponent(id))",
		"this.api('POST', '/v1/admin/runners/tasks/' + encodeURIComponent(task.id) + '/cancel')",
		"this.api('POST', '/v1/admin/runners/recover')",
		"loadRunnerTasks", "resetRunnerTasks", "paginateRunnerTasks", "cancelRunnerTask", "recoverRunnerTasks",
		"this.runnerTaskDetail = data.task || null",
		"ID", "capability", "tenant/subject", "scope", "state", "cancel_requested", "worker", "attempt/max", "generation", "updated_at", "has_result", "has_trace_context",
		"identity.role==='admin'", "Recover expired", "Cancel", "没有匹配的运行器任务", "任务详情",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("console missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"task.args", "task.result", "traceparent", "tracestate", "idempotency_key",
	} {
		if strings.Contains(strings.ToLower(body), forbidden) {
			t.Fatalf("console must not expose sensitive runner task UI %q", forbidden)
		}
	}
	assertInlineJavaScriptSyntax(t, body)
}

func TestConsoleEmbedsAdminOnlyRagProjectionMaintenance(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("console status=%d body=%s", response.Code, response.Body.String())
	}
	body := consoleBundle(t, response.Body.String())
	for _, required := range []string{
		"RAG Projection / RAG 投影", "rag-projection", "adminOnly: true", "visibleNav",
		"view==='rag-projection' && identity.role==='admin'", "identity.role !== 'admin'",
		"canonical_documents", "token_rows", "tag_rows", "token_documents", "tag_documents", "orphan_token_rows", "orphan_tag_rows",
		"Canonical Documents", "Token Rows", "Tag Rows", "Token Documents", "Tag Documents", "Orphan Token Rows", "Orphan Tag Rows",
		"this.api('GET', '/v1/admin/rag/projection')", "this.api('POST', '/v1/admin/rag/projection/rebuild')",
		"loadRagProjection", "rebuildRagProjection", "ragProjectionRebuilding", "Rebuild Projection", `class="danger"`,
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("console missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"ragProjection.content", "ragProjection.tags", "ragProjection.tokens", "ragProjection.document_id",
		"projection.document_id", "projection.content", "projection.tags_json",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("console must not expose RAG projection values %q", forbidden)
		}
	}
	mainStart := strings.Index(body, "<main>")
	mainEnd := strings.Index(body, "</main>")
	projectionView := strings.Index(body, `<!-- RAG Projection -->`)
	if mainStart < 0 || mainEnd < 0 || projectionView <= mainStart || projectionView >= mainEnd {
		t.Fatalf("RAG projection view must be rendered inside main content: main=%d..%d view=%d", mainStart, mainEnd, projectionView)
	}
	assertInlineJavaScriptSyntax(t, body)
}

func TestConsoleEmbedsAdminOnlyMemoryProjectionMaintenance(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("console status=%d body=%s", response.Code, response.Body.String())
	}
	body := consoleBundle(t, response.Body.String())
	for _, required := range []string{
		"Memory Projection / Memory 投影", "memory-projection", "adminOnly: true", "visibleNav",
		"view==='memory-projection' && identity.role==='admin'",
		"canonical_entries", "tag_rows", "tagged_keys", "orphan_tag_rows", "key_search_missing", "content_search_missing", "search_projected_entries",
		"Canonical Entries", "Tag Rows", "Tagged Keys", "Orphan Tag Rows", "Key Search Missing", "Content Search Missing", "Search Projected Entries",
		"coverage", "key_search 与 content_search 两个派生字段均非空", "并不证明完整内容一致性",
		"this.api('GET', '/v1/admin/memory/projection')", "this.api('POST', '/v1/admin/memory/projection/rebuild')",
		"loadMemoryProjection", "rebuildMemoryProjection", "memoryProjectionRebuilding", "Rebuild Projection", `class="danger"`,
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("console missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"memoryProjection.key", "memoryProjection.content", "memoryProjection.tag", "memoryProjection.entry_id",
		"projection.entry_id", "projection.key", "projection.content", "projection.tag",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("console must not expose Memory projection values %q", forbidden)
		}
	}
	mainStart := strings.Index(body, "<main>")
	mainEnd := strings.Index(body, "</main>")
	projectionView := strings.Index(body, `<!-- Memory Projection -->`)
	if mainStart < 0 || mainEnd < 0 || projectionView <= mainStart || projectionView >= mainEnd {
		t.Fatalf("Memory projection view must be rendered inside main content: main=%d..%d view=%d", mainStart, mainEnd, projectionView)
	}
	assertInlineJavaScriptSyntax(t, body)
}

func assertInlineJavaScriptSyntax(t *testing.T, page string) {
	t.Helper()
	tagRE := regexp.MustCompile(`(?s)<script(?:\s[^>]*)?>(.*?)</script>`)
	inline := 0
	for _, match := range tagRE.FindAllStringSubmatch(page, -1) {
		if strings.Contains(match[0][:strings.Index(match[0], ">")], " src=") {
			continue
		}
		inline++
	}
	if inline != 0 {
		t.Fatalf("console must not contain inline executable scripts: %d", inline)
	}
}

func consoleBundle(t *testing.T, page string) string {
	t.Helper()
	var builder strings.Builder
	builder.WriteString(page)
	for _, name := range []string{
		"api.js", "lib.js", "auth.js", "accounts.js", "audit.js", "observability.js", "policies.js", "credentials.js",
		"resources.js", "sessions.js", "playground.js", "bindings.js", "storage-settings.js", "model-settings.js", "notification-targets.js", "sandbox.js",
		"shell.js", "capabilities.js", "profiles.js", "approvals.js", "evaluations.js", "delegations.js", "runner-tasks.js", "projections.js", "app.js",
	} {
		data, err := staticFS.ReadFile("static/js/" + name)
		if err != nil {
			t.Fatalf("read embedded console asset %s: %v", name, err)
		}
		_, _ = builder.Write(data)
	}
	return builder.String()
}

func TestConsoleServesLocalJavaScriptModules(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d body=%s", pageResponse.Code, pageResponse.Body.String())
	}
	page := pageResponse.Body.String()
	for _, src := range []string{"/console/assets/alpine.min.js", "/console/js/lib.js", "/console/js/api.js", "/console/js/auth.js", "/console/js/accounts.js", "/console/js/audit.js", "/console/js/observability.js", "/console/js/policies.js", "/console/js/credentials.js", "/console/js/resources.js", "/console/js/sessions.js", "/console/js/playground.js", "/console/js/bindings.js", "/console/js/storage-settings.js", "/console/js/model-settings.js", "/console/js/notification-targets.js", "/console/js/sandbox.js", "/console/js/shell.js", "/console/js/capabilities.js", "/console/js/profiles.js", "/console/js/approvals.js", "/console/js/evaluations.js", "/console/js/delegations.js", "/console/js/runner-tasks.js", "/console/js/projections.js", "/console/js/app.js"} {
		if !strings.Contains(page, `src="`+src+`?v=`) {
			t.Fatalf("console missing local script %q", src)
		}
		if strings.HasPrefix(src, "http://") || strings.HasPrefix(src, "https://") || strings.HasPrefix(src, "//") {
			t.Fatalf("console script must not be remote: %q", src)
		}
		path := strings.TrimPrefix(src, "/console")
		response := httptest.NewRecorder()
		Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("local script %s status=%d", src, response.Code)
		}
		if !strings.Contains(response.Header().Get("Content-Type"), "javascript") {
			t.Fatalf("local script %s content-type=%q", src, response.Header().Get("Content-Type"))
		}
		dir := t.TempDir()
		file := filepath.Join(dir, filepath.Base(path))
		if err := os.WriteFile(file, response.Body.Bytes(), 0o600); err != nil {
			t.Fatalf("write %s for syntax check: %v", src, err)
		}
		if output, err := exec.Command("node", "--check", file).CombinedOutput(); err != nil {
			t.Fatalf("local script %s syntax: %v\n%s", src, err, output)
		}
	}
	scriptSrcRE := regexp.MustCompile(`(?i)<script[^>]+src="([^"]+)"`)
	matches := scriptSrcRE.FindAllStringSubmatch(page, -1)
	for _, match := range matches {
		src := strings.ToLower(match[1])
		if strings.HasPrefix(src, "http://") || strings.HasPrefix(src, "https://") || strings.HasPrefix(src, "//") {
			t.Fatalf("console contains remote script URL %q", match[1])
		}
	}
	expectedOrder := []string{
		"/console/assets/alpine.min.js", "/console/js/api.js", "/console/js/lib.js", "/console/js/auth.js",
		"/console/js/accounts.js", "/console/js/audit.js", "/console/js/observability.js", "/console/js/policies.js",
		"/console/js/credentials.js", "/console/js/resources.js", "/console/js/sessions.js", "/console/js/playground.js",
		"/console/js/bindings.js", "/console/js/storage-settings.js", "/console/js/model-settings.js", "/console/js/notification-targets.js", "/console/js/sandbox.js", "/console/js/shell.js",
		"/console/js/capabilities.js", "/console/js/profiles.js", "/console/js/approvals.js", "/console/js/evaluations.js",
		"/console/js/delegations.js", "/console/js/runner-tasks.js", "/console/js/projections.js", "/console/js/app.js",
	}
	if len(matches) != len(expectedOrder) {
		t.Fatalf("console script count=%d, want %d", len(matches), len(expectedOrder))
	}
	for i, expected := range expectedOrder {
		parsed, err := url.Parse(matches[i][1])
		if err != nil {
			t.Fatalf("parse console script URL %q: %v", matches[i][1], err)
		}
		if parsed.Path != expected || len(parsed.Query()) != 1 || len(parsed.Query()["v"]) != 1 || len(parsed.Query().Get("v")) != 64 {
			t.Fatalf("console script order[%d]=%q, want versioned %q", i, matches[i][1], expected)
		}
	}
}

func TestConsoleAppComposesAllDomainsWithoutRuntimeCollisions(t *testing.T) {
	assets := []string{
		"api.js", "lib.js", "auth.js", "accounts.js", "audit.js", "observability.js", "policies.js", "credentials.js",
		"resources.js", "sessions.js", "playground.js", "bindings.js", "storage-settings.js", "model-settings.js", "notification-targets.js", "sandbox.js",
		"shell.js", "capabilities.js", "profiles.js", "approvals.js", "evaluations.js", "delegations.js", "runner-tasks.js", "projections.js", "app.js",
	}
	dir := t.TempDir()
	paths := make([]string, 0, len(assets))
	for _, name := range assets {
		data, err := staticFS.ReadFile("static/js/" + name)
		if err != nil {
			t.Fatalf("read embedded asset %s: %v", name, err)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("write asset %s: %v", name, err)
		}
		paths = append(paths, path)
	}

	var script strings.Builder
	script.WriteString(`
global.window = {
  localStorage: { getItem() { return null }, setItem() {}, removeItem() {} },
  addEventListener() {},
  Alpine: { data(name, fn) { global.registered = { name, fn } } },
}
global.localStorage = window.localStorage
global.document = { addEventListener() {} }
global.location = { hash: '' }
global.Alpine = window.Alpine
global.fetch = async () => ({ ok: true, status: 200, json: async () => ({}) })
`)
	for _, path := range paths {
		quoted, err := jsonString(path)
		if err != nil {
			t.Fatalf("quote asset path %s: %v", path, err)
		}
		fmt.Fprintf(&script, "require(%s)\n", quoted)
	}
	script.WriteString(`
if (!global.registered || global.registered.name !== 'console') throw new Error('console was not registered')
const model = window.HarnessConsoleApp.compose()
for (const name of ['api', 'parseScope', 'load', 'createCapability', 'loadProfiles', 'loadApprovals', 'loadEvidence', 'loadDelegations', 'loadRunnerTasks', 'loadRagProjection', 'loadMemoryProjection', 'loadNotificationTargets', 'saveNotificationTarget', 'deleteNotificationTarget', 'loadSandboxProviders', 'sandboxAssurance', 'boot', 'go']) {
  if (typeof model[name] !== 'function') throw new Error('missing composed function: ' + name)
}
if (!Array.isArray(model.visibleNav)) throw new Error('visibleNav must be an array')

const originalLayer = { profile_id: 'demo', metadata: { custom_key: 'keep', 'harness.executor.id': 'vendor-x', 'harness.executor.version': '9' }, extension: { nested: true } }
model.profileEdit.id = 'demo'
model.profileEdit.layerJSON = JSON.stringify(originalLayer)
model.profileFieldsFromLayer(originalLayer)
if (model.profileEdit.executionMode !== 'custom') throw new Error('unknown executor must be custom')
model.profileApplyExecutionMode()
let preserved = JSON.parse(model.profileEdit.layerJSON)
if (preserved.metadata.custom_key !== 'keep' || preserved.metadata['harness.executor.id'] !== 'vendor-x' || !preserved.extension.nested) throw new Error('custom metadata was not preserved')
model.profileEdit.executionMode = 'sequential'
model.profileApplyExecutionMode()
preserved = JSON.parse(model.profileEdit.layerJSON)
if (preserved.metadata.custom_key !== 'keep' || 'harness.executor.id' in preserved.metadata || 'harness.executor.version' in preserved.metadata) throw new Error('sequential metadata mapping is incorrect')
model.profileEdit.executionMode = 'graph'
model.profileApplyExecutionMode()
preserved = JSON.parse(model.profileEdit.layerJSON)
if (preserved.metadata.custom_key !== 'keep' || preserved.metadata['harness.executor.id'] !== 'graph-core-turn' || preserved.metadata['harness.executor.version'] !== '1') throw new Error('graph metadata mapping is incorrect')
`)
	scriptPath := filepath.Join(dir, "compose-check.js")
	if err := os.WriteFile(scriptPath, []byte(script.String()), 0o600); err != nil {
		t.Fatalf("write compose check: %v", err)
	}
	if output, err := exec.Command("node", scriptPath).CombinedOutput(); err != nil {
		t.Fatalf("runtime composition failed: %v\n%s", err, output)
	}
}

func jsonString(value string) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func TestConsoleActivationPayloadDoesNotContainUIError(t *testing.T) {
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/js/auth.js", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("auth.js status=%d", response.Code)
	}
	body := response.Body.String()
	if !strings.Contains(body, "const payload = { password: this.activation.password, confirm: this.activation.confirm }") {
		t.Fatal("activation must construct a password/confirm-only payload")
	}
	if strings.Contains(body, "this.api('POST', '/v1/auth/activate', this.activation)") {
		t.Fatal("activation must not send the UI activation object")
	}
}

func TestConsoleLoginUsesUnauthenticatedAPIClient(t *testing.T) {
	apiResponse := httptest.NewRecorder()
	Handler().ServeHTTP(apiResponse, httptest.NewRequest(http.MethodGet, "/js/api.js", nil))
	if apiResponse.Code != http.StatusOK {
		t.Fatalf("api.js status=%d", apiResponse.Code)
	}
	if !strings.Contains(apiResponse.Body.String(), "requestUnauthenticatedJSON") {
		t.Fatal("api.js must expose an unauthenticated JSON request entry point")
	}

	authResponse := httptest.NewRecorder()
	Handler().ServeHTTP(authResponse, httptest.NewRequest(http.MethodGet, "/js/auth.js", nil))
	if authResponse.Code != http.StatusOK {
		t.Fatalf("auth.js status=%d", authResponse.Code)
	}
	body := authResponse.Body.String()
	if !strings.Contains(body, "this.requestUnauthenticatedJSON") {
		t.Fatal("login must use the shared unauthenticated JSON client")
	}
	if strings.Contains(body, "fetch('/v1/auth/login'") {
		t.Fatal("login must not call fetch directly")
	}
	if !strings.Contains(body, "ensureIdentityTenant") || !strings.Contains(body, "tenant === '-')") {
		t.Fatal("platform administrators must recover the system tenant for existing sessions")
	}
}

func TestConsoleProfilesEditorDefaultsAndLoadsGeneral(t *testing.T) {
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/js/profiles.js", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("profiles.js status=%d", response.Code)
	}
	body := response.Body.String()
	for _, required := range []string{
		"id: 'general'", "scopeText: 'global:global'", "this.profiles.includes(this.profileEdit.id)",
		"await this.loadProfileEditor()",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("profiles editor missing %q", required)
		}
	}
}

func TestConsoleAccountsModuleIsComposedByApp(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d", pageResponse.Code)
	}
	page := pageResponse.Body.String()
	if !strings.Contains(page, "<script src=\"/console/js/accounts.js?v=") {
		t.Fatal("console must load the local accounts domain module")
	}

	accountsResponse := httptest.NewRecorder()
	Handler().ServeHTTP(accountsResponse, httptest.NewRequest(http.MethodGet, "/js/accounts.js", nil))
	if accountsResponse.Code != http.StatusOK {
		t.Fatalf("accounts.js status=%d", accountsResponse.Code)
	}
	accounts := accountsResponse.Body.String()
	for _, required := range []string{
		"window.HarnessConsoleAccounts",
		"state()",
		"accounts: []",
		"tenants: []",
		"loadAccounts()",
		"createAccount()",
		"toggleAccount(account)",
		"resetPassword(account)",
		"createTenant()",
		"/v1/admin/accounts",
		"/v1/admin/tenants",
	} {
		if !strings.Contains(accounts, required) {
			t.Fatalf("accounts module missing %q", required)
		}
	}

	appResponse := httptest.NewRecorder()
	Handler().ServeHTTP(appResponse, httptest.NewRequest(http.MethodGet, "/js/app.js", nil))
	if appResponse.Code != http.StatusOK {
		t.Fatalf("app.js status=%d", appResponse.Code)
	}
	app := appResponse.Body.String()
	for _, required := range []string{"window.HarnessConsoleAccounts"} {
		if !strings.Contains(app, required) {
			t.Fatalf("app composition missing %q", required)
		}
	}
}

func TestConsoleAuditModuleIsComposedByApp(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d", pageResponse.Code)
	}
	if !strings.Contains(pageResponse.Body.String(), "<script src=\"/console/js/audit.js?v=") {
		t.Fatal("console must load the local audit domain module")
	}

	auditResponse := httptest.NewRecorder()
	Handler().ServeHTTP(auditResponse, httptest.NewRequest(http.MethodGet, "/js/audit.js", nil))
	if auditResponse.Code != http.StatusOK {
		t.Fatalf("audit.js status=%d", auditResponse.Code)
	}
	audit := auditResponse.Body.String()
	for _, required := range []string{
		"window.HarnessConsoleAudit",
		"state()",
		"auditEvents: []",
		"au: { actor: '', action: '', limit: 50, offset: 0, total: 0 }",
		"loadAudit()",
		"/v1/admin/audit?",
	} {
		if !strings.Contains(audit, required) {
			t.Fatalf("audit module missing %q", required)
		}
	}

	appResponse := httptest.NewRecorder()
	Handler().ServeHTTP(appResponse, httptest.NewRequest(http.MethodGet, "/js/app.js", nil))
	if appResponse.Code != http.StatusOK {
		t.Fatalf("app.js status=%d", appResponse.Code)
	}
	app := appResponse.Body.String()
	for _, required := range []string{"window.HarnessConsoleAudit"} {
		if !strings.Contains(app, required) {
			t.Fatalf("app composition missing %q", required)
		}
	}
}

func TestConsoleObservabilityModuleIsComposedByApp(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d", pageResponse.Code)
	}
	page := pageResponse.Body.String()
	if !strings.Contains(page, "<script src=\"/console/js/observability.js?v=") {
		t.Fatal("console must load the local observability domain module")
	}

	observabilityResponse := httptest.NewRecorder()
	Handler().ServeHTTP(observabilityResponse, httptest.NewRequest(http.MethodGet, "/js/observability.js", nil))
	if observabilityResponse.Code != http.StatusOK {
		t.Fatalf("observability.js status=%d", observabilityResponse.Code)
	}
	observability := observabilityResponse.Body.String()
	for _, required := range []string{
		"window.HarnessConsoleObservability",
		"state()",
		"obs:",
		"bt:",
		"loadObs()",
		"loadHits()",
		"createObsRule()",
		"toggleObsRule(rule)",
		"deleteObsRule(rule)",
		"runBacktest()",
		"/v1/admin/obs/rules",
		"/v1/admin/obs/hits?",
		"/v1/admin/backtests",
	} {
		if !strings.Contains(observability, required) {
			t.Fatalf("observability module missing %q", required)
		}
	}

	appResponse := httptest.NewRecorder()
	Handler().ServeHTTP(appResponse, httptest.NewRequest(http.MethodGet, "/js/app.js", nil))
	if appResponse.Code != http.StatusOK {
		t.Fatalf("app.js status=%d", appResponse.Code)
	}
	app := appResponse.Body.String()
	for _, required := range []string{"window.HarnessConsoleObservability"} {
		if !strings.Contains(app, required) {
			t.Fatalf("app composition missing %q", required)
		}
	}
	if strings.Contains(page, "async loadObs()") || strings.Contains(page, "async runBacktest()") {
		t.Fatal("observability methods must be removed from inline domain script")
	}
}

func TestConsolePoliciesModuleIsComposedByApp(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d", pageResponse.Code)
	}
	page := pageResponse.Body.String()
	if !strings.Contains(page, "<script src=\"/console/js/policies.js?v=") {
		t.Fatal("console must load the local policies domain module")
	}

	policiesResponse := httptest.NewRecorder()
	Handler().ServeHTTP(policiesResponse, httptest.NewRequest(http.MethodGet, "/js/policies.js", nil))
	if policiesResponse.Code != http.StatusOK {
		t.Fatalf("policies.js status=%d", policiesResponse.Code)
	}
	policies := policiesResponse.Body.String()
	for _, required := range []string{
		"window.HarnessConsolePolicies",
		"state()",
		"policies: []",
		"pol:",
		"loadPolicies()",
		"bindPolicy()",
		"/v1/admin/policies",
		"this.parseScope",
		"this.csv",
	} {
		if !strings.Contains(policies, required) {
			t.Fatalf("policies module missing %q", required)
		}
	}

	appResponse := httptest.NewRecorder()
	Handler().ServeHTTP(appResponse, httptest.NewRequest(http.MethodGet, "/js/app.js", nil))
	if appResponse.Code != http.StatusOK {
		t.Fatalf("app.js status=%d", appResponse.Code)
	}
	app := appResponse.Body.String()
	for _, required := range []string{"window.HarnessConsolePolicies"} {
		if !strings.Contains(app, required) {
			t.Fatalf("app composition missing %q", required)
		}
	}
	if strings.Contains(page, "async bindPolicy()") || strings.Contains(page, "pol: {") {
		t.Fatal("policies state and methods must be removed from inline domain script")
	}
}

func TestConsoleCredentialsModuleIsComposedWithoutSecretStorage(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d", pageResponse.Code)
	}
	page := pageResponse.Body.String()
	if !strings.Contains(page, "<script src=\"/console/js/credentials.js?v=") {
		t.Fatal("console must load the local credentials domain module")
	}

	credentialsResponse := httptest.NewRecorder()
	Handler().ServeHTTP(credentialsResponse, httptest.NewRequest(http.MethodGet, "/js/credentials.js", nil))
	if credentialsResponse.Code != http.StatusOK {
		t.Fatalf("credentials.js status=%d", credentialsResponse.Code)
	}
	credentials := credentialsResponse.Body.String()
	for _, required := range []string{
		"window.HarnessConsoleCredentials",
		"state()",
		"credentials: []",
		"cred:",
		"loadCredentials()",
		"bindCredential()",
		"/v1/admin/credentials",
		"value: this.cred.value",
		"this.cred.value = ''",
	} {
		if !strings.Contains(credentials, required) {
			t.Fatalf("credentials module missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"localStorage",
		"console.log",
		"JSON.stringify(this.cred)",
		"JSON.stringify(cred)",
	} {
		if strings.Contains(credentials, forbidden) {
			t.Fatalf("credentials module must not persist or log secret via %q", forbidden)
		}
	}

	appResponse := httptest.NewRecorder()
	Handler().ServeHTTP(appResponse, httptest.NewRequest(http.MethodGet, "/js/app.js", nil))
	if appResponse.Code != http.StatusOK {
		t.Fatalf("app.js status=%d", appResponse.Code)
	}
	app := appResponse.Body.String()
	for _, required := range []string{"window.HarnessConsoleCredentials"} {
		if !strings.Contains(app, required) {
			t.Fatalf("app composition missing %q", required)
		}
	}
	if strings.Contains(page, "async bindCredential()") || strings.Contains(page, "cred: {") {
		t.Fatal("credentials state and methods must be removed from inline domain script")
	}
}

func TestConsoleResourcesModuleIsComposedWithLocalFileHandling(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d", pageResponse.Code)
	}
	page := pageResponse.Body.String()
	if !strings.Contains(page, "<script src=\"/console/js/resources.js?v=") {
		t.Fatal("console must load the local resources domain module")
	}

	resourcesResponse := httptest.NewRecorder()
	Handler().ServeHTTP(resourcesResponse, httptest.NewRequest(http.MethodGet, "/js/resources.js", nil))
	if resourcesResponse.Code != http.StatusOK {
		t.Fatalf("resources.js status=%d", resourcesResponse.Code)
	}
	resources := resourcesResponse.Body.String()
	for _, required := range []string{
		"window.HarnessConsoleResources",
		"state()",
		"resources: []",
		"res:",
		"loadResources()",
		"uploadResource(event)",
		"downloadResource(key)",
		"deleteResource(key)",
		"/v1/resources/",
		"encodeURIComponent(this.res.prefix)",
		"body: file",
		"replace(/^\\/+/, '')",
		"this.res.prefix = key.includes('/')",
	} {
		if !strings.Contains(resources, required) {
			t.Fatalf("resources module missing %q", required)
		}
	}
	for _, forbidden := range []string{"localStorage", "console.log", "credential", "secret"} {
		if strings.Contains(strings.ToLower(resources), forbidden) {
			t.Fatalf("resources module must not persist or log credentials via %q", forbidden)
		}
	}

	appResponse := httptest.NewRecorder()
	Handler().ServeHTTP(appResponse, httptest.NewRequest(http.MethodGet, "/js/app.js", nil))
	if appResponse.Code != http.StatusOK {
		t.Fatalf("app.js status=%d", appResponse.Code)
	}
	app := appResponse.Body.String()
	for _, required := range []string{"window.HarnessConsoleResources"} {
		if !strings.Contains(app, required) {
			t.Fatalf("app composition missing %q", required)
		}
	}
	if strings.Contains(page, "async loadResources()") || strings.Contains(page, "async uploadResource(") ||
		strings.Contains(page, "async downloadResource(") || strings.Contains(page, "async deleteResource(") ||
		strings.Contains(page, "res: {") {
		t.Fatal("resources state and methods must be removed from inline domain script")
	}
}

func TestConsoleSessionsModuleIsComposedByApp(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d", pageResponse.Code)
	}
	page := pageResponse.Body.String()
	if !strings.Contains(page, "<script src=\"/console/js/sessions.js?v=") {
		t.Fatal("console must load the local sessions domain module")
	}

	sessionsResponse := httptest.NewRecorder()
	Handler().ServeHTTP(sessionsResponse, httptest.NewRequest(http.MethodGet, "/js/sessions.js", nil))
	if sessionsResponse.Code != http.StatusOK {
		t.Fatalf("sessions.js status=%d", sessionsResponse.Code)
	}
	sessions := sessionsResponse.Body.String()
	for _, required := range []string{
		"window.HarnessConsoleSessions",
		"state()",
		"sessions: []",
		"sessionEvents: []",
		"sess:",
		"loadSessions()",
		"viewSession(id)",
		"/v1/sessions?",
		"/events",
		"this.sess.profile",
		"this.sess.prefix",
		"this.sess.sort",
		"this.sess.limit",
		"this.sess.offset",
	} {
		if !strings.Contains(sessions, required) {
			t.Fatalf("sessions module missing %q", required)
		}
	}

	appResponse := httptest.NewRecorder()
	Handler().ServeHTTP(appResponse, httptest.NewRequest(http.MethodGet, "/js/app.js", nil))
	if appResponse.Code != http.StatusOK {
		t.Fatalf("app.js status=%d", appResponse.Code)
	}
	app := appResponse.Body.String()
	for _, required := range []string{"window.HarnessConsoleSessions"} {
		if !strings.Contains(app, required) {
			t.Fatalf("app composition missing %q", required)
		}
	}
	if strings.Contains(page, "async loadSessions()") || strings.Contains(page, "async viewSession(") ||
		strings.Contains(page, "sess: {") || strings.Contains(page, "sessionEvents: []") {
		t.Fatal("sessions state and methods must be removed from inline domain script")
	}
}

func TestConsolePlaygroundModuleIsComposedWithSSEEvents(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d", pageResponse.Code)
	}
	page := pageResponse.Body.String()
	if !strings.Contains(page, "<script src=\"/console/js/playground.js?v=") {
		t.Fatal("console must load the local playground domain module")
	}

	playgroundResponse := httptest.NewRecorder()
	Handler().ServeHTTP(playgroundResponse, httptest.NewRequest(http.MethodGet, "/js/playground.js", nil))
	if playgroundResponse.Code != http.StatusOK {
		t.Fatalf("playground.js status=%d", playgroundResponse.Code)
	}
	playground := playgroundResponse.Body.String()
	for _, required := range []string{
		"window.HarnessConsolePlayground",
		"state()",
		"pg:",
		"pgNewSession()",
		"pgSend()",
		"/v1/sessions/",
		"/runs",
		"this.headers()",
		"body: JSON.stringify({ message: text })",
		"getReader()",
		"TextDecoder()",
		"assistant/chunk",
		"tool/call",
		"tool/result",
		"assistant/message",
		"run/end",
		"this.pg.pendingText",
		"this.pg.messages",
		"运行失败 HTTP",
	} {
		if !strings.Contains(playground, required) {
			t.Fatalf("playground module missing %q", required)
		}
	}

	appResponse := httptest.NewRecorder()
	Handler().ServeHTTP(appResponse, httptest.NewRequest(http.MethodGet, "/js/app.js", nil))
	if appResponse.Code != http.StatusOK {
		t.Fatalf("app.js status=%d", appResponse.Code)
	}
	app := appResponse.Body.String()
	for _, required := range []string{"window.HarnessConsolePlayground"} {
		if !strings.Contains(app, required) {
			t.Fatalf("app composition missing %q", required)
		}
	}
	if strings.Contains(page, "async pgNewSession()") || strings.Contains(page, "async pgSend()") ||
		strings.Contains(page, "pg: {") {
		t.Fatal("playground state and methods must be removed from inline domain script")
	}
}

func TestConsolePlaygroundPrefersGeneralProfileAndUsesChineseLabels(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d", pageResponse.Code)
	}
	page := consoleBundle(t, pageResponse.Body.String())
	for _, required := range []string{
		"label: '开始使用'",
		"label: '对话'",
		`visibleNav.filter(item => !item.advancedOnly)`,
		`visibleNav.filter(item => item.advancedOnly)`,
		`advancedOnly: true`,
		`advancedOpen: localStorage.getItem('hc_console_advanced_nav') === 'true'`,
		"toggleAdvancedNav()",
		`aria-controls="advanced-navigation"`,
		"存储高级设置",
		"更多连接选项",
		"运行状态详情",
		"<span>智能体</span>",
		"profiles.length === 1 && profiles[0] === 'general'",
		"profiles.length !== 1 || profiles[0] !== 'general'",
		"继续已有会话（可选）",
		"开始新对话，或继续已有会话。",
		"<label>智能体配置</label>",
		`x-text="p === 'general' ? '通用智能体' : p"`,
		"新建会话",
		"this.profiles.includes('general') ? 'general' : this.profiles[0]",
	} {
		if !strings.Contains(page, required) {
			t.Fatalf("console playground missing %q", required)
		}
	}
	modelIndex := strings.Index(page, "<h2 style=\"margin-top:8px\">模型连接</h2>")
	storageIndex := strings.Index(page, "<h2 style=\"margin-top:8px\">存储状态</h2>")
	if modelIndex < 0 || storageIndex < 0 || modelIndex > storageIndex {
		t.Fatal("model connection must appear before storage status on the overview")
	}

	playgroundResponse := httptest.NewRecorder()
	Handler().ServeHTTP(playgroundResponse, httptest.NewRequest(http.MethodGet, "/js/playground.js", nil))
	if playgroundResponse.Code != http.StatusOK {
		t.Fatalf("playground.js status=%d", playgroundResponse.Code)
	}
	if !strings.Contains(playgroundResponse.Body.String(), "{ profile_id: this.pg.profile }") {
		t.Fatal("new playground sessions must send the selected profile explicitly")
	}
}

func TestConsoleBindingsModuleIsComposedByApp(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d", pageResponse.Code)
	}
	page := pageResponse.Body.String()
	if !strings.Contains(page, "<script src=\"/console/js/bindings.js?v=") {
		t.Fatal("console must load the local bindings domain module")
	}

	bindingsResponse := httptest.NewRecorder()
	Handler().ServeHTTP(bindingsResponse, httptest.NewRequest(http.MethodGet, "/js/bindings.js", nil))
	if bindingsResponse.Code != http.StatusOK {
		t.Fatalf("bindings.js status=%d", bindingsResponse.Code)
	}
	bindings := bindingsResponse.Body.String()
	for _, required := range []string{
		"window.HarnessConsoleBindings",
		"state()",
		"bindings: []",
		"loadBindings()",
		"unbind(id)",
		"/v1/admin/bindings",
		"this.notify('绑定已解除')",
		"this.load('bindings')",
	} {
		if !strings.Contains(bindings, required) {
			t.Fatalf("bindings module missing %q", required)
		}
	}

	appResponse := httptest.NewRecorder()
	Handler().ServeHTTP(appResponse, httptest.NewRequest(http.MethodGet, "/js/app.js", nil))
	if appResponse.Code != http.StatusOK {
		t.Fatalf("app.js status=%d", appResponse.Code)
	}
	app := appResponse.Body.String()
	for _, required := range []string{"window.HarnessConsoleBindings"} {
		if !strings.Contains(app, required) {
			t.Fatalf("app composition missing %q", required)
		}
	}
	if strings.Contains(page, "async unbind(") || strings.Contains(page, "async loadBindings()") ||
		strings.Contains(page, "bindings: []") {
		t.Fatal("bindings state and methods must be removed from inline domain script")
	}
}

func TestConsoleStorageSettingsModuleIsComposedWithoutSecretPersistence(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d", pageResponse.Code)
	}
	page := pageResponse.Body.String()
	if !strings.Contains(page, "<script src=\"/console/js/storage-settings.js?v=") {
		t.Fatal("console must load the local storage settings domain module")
	}

	settingsResponse := httptest.NewRecorder()
	Handler().ServeHTTP(settingsResponse, httptest.NewRequest(http.MethodGet, "/js/storage-settings.js", nil))
	if settingsResponse.Code != http.StatusOK {
		t.Fatalf("storage-settings.js status=%d", settingsResponse.Code)
	}
	settings := settingsResponse.Body.String()
	for _, required := range []string{
		"window.HarnessConsoleStorageSettings",
		"state()",
		"storage:",
		"loadStorageSettings()",
		"storageBody(",
		"testStorage(which)",
		"saveStorage(which)",
		"/v1/admin/storage/",
		"/v1/admin/storage/test",
		"this.storage[typeField]",
		"this.storage[endpointField]",
		"body.type === 's3'",
		"has_secret",
		"secret_preview",
		"clear_secret",
		"desired_revision",
		"expected_revision",
		"active_revision",
		"restart_pending",
		"disable_conditional_writes",
		"config_status",
		"config_source",
		"config.config_status",
		"config.config_source",
		"sessPath",
		"resPath",
		"resHasSecret",
		"sessHasSecret",
		"this.storage.resSecret = ''",
		"this.storage.sessSecret = ''",
	} {
		if !strings.Contains(settings, required) {
			t.Fatalf("storage settings module missing %q", required)
		}
	}
	for _, forbidden := range []string{"localStorage", "console.log", "JSON.stringify", "data.config_status", "data.config_source"} {
		if strings.Contains(settings, forbidden) {
			t.Fatalf("storage settings module must not persist or log secrets via %q", forbidden)
		}
	}

	appResponse := httptest.NewRecorder()
	Handler().ServeHTTP(appResponse, httptest.NewRequest(http.MethodGet, "/js/app.js", nil))
	if appResponse.Code != http.StatusOK {
		t.Fatalf("app.js status=%d", appResponse.Code)
	}
	app := appResponse.Body.String()
	for _, required := range []string{"window.HarnessConsoleStorageSettings"} {
		if !strings.Contains(app, required) {
			t.Fatalf("app composition missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"async loadStorageSettings()",
		"storageBody(",
		"async testStorage(",
		"async saveStorage(",
		"storage: { resType:",
	} {
		if strings.Contains(page, forbidden) {
			t.Fatalf("storage settings must be removed from inline domain script: %q", forbidden)
		}
	}
}

func TestConsoleModelSettingsUseDedicatedPresenceAwareSecretFlow(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d", pageResponse.Code)
	}
	page := pageResponse.Body.String()
	if !strings.Contains(page, "<script src=\"/console/js/model-settings.js?v=") {
		t.Fatal("console must load the local model settings domain module")
	}

	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/js/model-settings.js", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("model-settings.js status=%d", response.Code)
	}
	module := response.Body.String()
	for _, required := range []string{
		"window.HarnessConsoleModelSettings", "loadModelSettings()", "modelSettingsBody()", "saveModelSettings()",
		"/v1/admin/model-settings/llm", "api_key_preview", "clear_api_key", "this.modelSettings.apiKey = ''",
	} {
		if !strings.Contains(module, required) {
			t.Fatalf("model settings module missing %q", required)
		}
	}
	for _, forbidden := range []string{"/v1/admin/settings/llm", "localStorage", "console.log", "JSON.stringify"} {
		if strings.Contains(module, forbidden) {
			t.Fatalf("model settings module must not use %q", forbidden)
		}
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "model-settings.js"), response.Body.Bytes(), 0o600); err != nil {
		t.Fatalf("write model settings module: %v", err)
	}
	const regression = `
global.window = {}
require('./model-settings.js')
const domain = window.HarnessConsoleModelSettings
let reads = 0
const model = {
  ...domain.state(),
  ...domain.methods,
  api: async (method, path) => {
    if (method === 'GET' && path === '/v1/admin/model-settings/llm') {
      reads += 1
      if (reads === 1) return { found: true, base_url: 'https://example.test/v1', model: 'model-a', allowed_models: ['model-a'], has_api_key: true, api_key_preview: 'pre••••••••tail' }
      if (reads === 2) return { found: false, base_url: '', model: '', allowed_models: [], has_api_key: false }
      throw new Error('injected reload failure')
    }
    if (method === 'PUT' && path === '/v1/admin/model-settings/llm') return { status: 'saved' }
    throw new Error('unexpected request')
  },
  notify: () => {},
}
model.modelSettings.apiKey = 'stale-api-key'
model.loadModelSettings().then(async () => {
  if (model.modelSettings.apiKey !== '') throw new Error('reload retained API key input')
  if (!model.modelSettings.hasAPIKey || model.modelSettings.apiKeyPreview !== 'pre••••••••tail') throw new Error('preview state was not loaded')
  const preserve = model.modelSettingsBody()
  if ('api_key' in preserve || 'api_key_preview' in preserve) throw new Error('preview was submitted as an API key')
  model.modelSettings.clearAPIKey = true
  const clear = model.modelSettingsBody()
  if (clear.clear_api_key !== true || 'api_key' in clear) throw new Error('clear intent was not explicit')
  model.modelSettings.clearAPIKey = false
  model.modelSettings.apiKey = 'replacement-api-key'
  const replace = model.modelSettingsBody()
  if (replace.api_key !== 'replacement-api-key' || replace.api_key_preview !== undefined) throw new Error('replacement was not presence-aware')
  model.modelSettings.baseURL = 'stale-base'
  model.modelSettings.model = 'stale-model'
  model.modelSettings.allowedModels = 'stale-model'
  model.modelSettings.apiKey = 'stale-key'
  await model.loadModelSettings()
  if (model.modelSettings.baseURL !== '' || model.modelSettings.model !== '' || model.modelSettings.allowedModels !== '' || model.modelSettings.apiKey !== '') {
    throw new Error('missing reload retained model settings state')
  }
  model.modelSettings.baseURL = 'stale-base'
  model.modelSettings.model = 'stale-model'
  model.modelSettings.allowedModels = 'stale-model'
  model.modelSettings.apiKey = 'stale-key'
  await model.loadModelSettings()
  if (model.modelSettings.baseURL !== '' || model.modelSettings.model !== '' || model.modelSettings.allowedModels !== '' || model.modelSettings.apiKey !== '') {
    throw new Error('failed reload retained model settings state')
  }
}).catch((error) => {
  process.stderr.write(error.stack + '\n')
  process.exit(1)
})
`
	if err := os.WriteFile(filepath.Join(dir, "model-settings-regression.js"), []byte(regression), 0o600); err != nil {
		t.Fatalf("write model settings regression: %v", err)
	}
	command := exec.Command("node", "model-settings-regression.js")
	command.Dir = dir
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("model settings must preserve explicit secret intent: %v\n%s", err, output)
	}
}

func TestConsoleNotificationTargetsUseWriteOnlyConfigurationFlow(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d", pageResponse.Code)
	}
	page := pageResponse.Body.String()
	if !strings.Contains(page, "<script src=\"/console/js/notification-targets.js?v=") {
		t.Fatal("console must load the local notification targets domain module")
	}
	for _, forbidden := range []string{"has_config", "config_preview", "target.config"} {
		if strings.Contains(page, forbidden) {
			t.Fatalf("notification target view must not display private configuration field %q", forbidden)
		}
	}

	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/js/notification-targets.js", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("notification-targets.js status=%d", response.Code)
	}
	module := response.Body.String()
	for _, required := range []string{
		"window.HarnessConsoleNotificationTargets", "notificationTargetEnsureTenant()", "loadNotificationTargets()", "saveNotificationTarget()", "deleteNotificationTarget(record)",
		"/v1/admin/notification-targets", "notificationTargetConfigRequired()", "target.configText = ''",
	} {
		if !strings.Contains(module, required) {
			t.Fatalf("notification targets module missing %q", required)
		}
	}
	for _, forbidden := range []string{"has_config", "config_preview", "localStorage", "console.log"} {
		if strings.Contains(module, forbidden) {
			t.Fatalf("notification targets module must not use %q", forbidden)
		}
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "notification-targets.js"), response.Body.Bytes(), 0o600); err != nil {
		t.Fatalf("write notification targets module: %v", err)
	}
	const regression = `
global.window = { confirm: () => true }
require('./notification-targets.js')
const domain = window.HarnessConsoleNotificationTargets
const sent = []
const model = {
  ...domain.state(), ...domain.methods,
  identity: { role: 'admin', tenant: 'tenant-a' },
  api: async (method, path, body) => {
    sent.push({ method, path, body })
    if (method === 'GET') return { targets: [{ target_ref: 'ops', channel_id: 'webhook', channel_version: '1', label: 'Operations', formats: ['markdown'], enabled: true, revision: 'r1' }] }
    if (method === 'POST' || method === 'PUT' || method === 'DELETE') return { status: 'ok' }
    throw new Error('unexpected request')
  },
  notify: () => {},
}
;(async () => {
  await model.loadNotificationTargets()
  if (model.notificationTargets.tenantID !== 'tenant-a') throw new Error('admin tenant was not prefilled')
  const firstLoad = sent.find(request => request.method === 'GET')
  if (!firstLoad || firstLoad.path !== '/v1/admin/notification-targets?tenant_id=tenant-a') throw new Error('admin tenant was not used for initial load')
  model.notificationTargets.configText = '{"secret":"stale"}'
  await model.loadNotificationTargets()
  if (model.notificationTargets.configText !== '') throw new Error('list retained write-only configuration')
  const listed = model.notificationTargets.targets[0]
  if ('config' in listed || 'has_config' in listed || 'config_preview' in listed) throw new Error('list model invents private configuration')
  model.editNotificationTarget(listed)
  if (model.notificationTargets.configText !== '') throw new Error('edit hydrated a configuration')
  const preserve = model.notificationTargetBody(true)
  if ('config' in preserve || preserve.expected_revision !== 'r1') throw new Error('same-channel update did not preserve configuration by omission')
  model.notificationTargets.channelVersion = '2'
  let switched = false
  try { model.notificationTargetBody(true) } catch (error) { switched = /完整配置/.test(error.message) }
  if (!switched) throw new Error('channel change accepted an omitted configuration')
  model.notificationTargets.configText = '{"url":"https://example.test/hook","secret":"new-secret"}'
  const replaced = model.notificationTargetBody(true)
  if (!replaced.config || replaced.config.secret !== 'new-secret') throw new Error('new write-only configuration was not sent')
  model.resetNotificationTargetForm()
  model.notificationTargets.targetRef = 'new-target'
  model.notificationTargets.configText = '{"url":"https://example.test/hook","secret":"create-secret"}'
  await model.saveNotificationTarget()
  const create = sent.find(request => request.method === 'POST')
  if (!create || !create.body.config || create.body.config.secret !== 'create-secret') throw new Error('create omitted required configuration')
  if (model.notificationTargets.configText !== '') throw new Error('save retained configuration input')
})().catch((error) => { process.stderr.write(error.stack + '\n'); process.exit(1) })
`
	if err := os.WriteFile(filepath.Join(dir, "notification-targets-regression.js"), []byte(regression), 0o600); err != nil {
		t.Fatalf("write notification targets regression: %v", err)
	}
	command := exec.Command("node", "notification-targets-regression.js")
	command.Dir = dir
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("notification targets must retain a write-only configuration flow: %v\n%s", err, output)
	}
}

func TestConsoleSandboxUsesDedicatedReadOnlyDiscoveryModule(t *testing.T) {
	pageResponse := httptest.NewRecorder()
	Handler().ServeHTTP(pageResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if pageResponse.Code != http.StatusOK {
		t.Fatalf("console status=%d", pageResponse.Code)
	}
	page := pageResponse.Body.String()
	if !strings.Contains(page, "<script src=\"/console/js/sandbox.js?v=") || !strings.Contains(page, "view==='sandbox'") {
		t.Fatal("console must load and render the sandbox discovery domain")
	}
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/js/sandbox.js", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("sandbox.js status=%d", response.Code)
	}
	module := response.Body.String()
	for _, required := range []string{"window.HarnessConsoleSandbox", "loadSandboxProviders()", "/v1/admin/sandbox/providers", "sandboxAssurance("} {
		if !strings.Contains(module, required) {
			t.Fatalf("sandbox module missing %q", required)
		}
	}
	for _, forbidden := range []string{"fetch(", "localStorage", "console.log", "Start(", "Run("} {
		if strings.Contains(module, forbidden) {
			t.Fatalf("sandbox discovery module must not contain %q", forbidden)
		}
	}
}

func TestConsoleStorageSettingsReloadClearsMaskedAndStaleSecrets(t *testing.T) {
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/js/storage-settings.js", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("storage-settings.js status=%d", response.Code)
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "storage-settings.js"), response.Body.Bytes(), 0o600); err != nil {
		t.Fatalf("write storage settings module: %v", err)
	}
	const regression = `
global.window = {}
require('./storage-settings.js')
const domain = window.HarnessConsoleStorageSettings
const model = {
  ...domain.state(),
  ...domain.methods,
  api: async (_method, path) => {
    if (path.endsWith('/resources')) {
      return { found: true, config: { type: 's3', endpoint: 'https://s3.example', bucket: 'resources', access_key: 'resource-access', has_secret: true, secret_preview: 'pre••••••••tail' } }
    }
    throw new Error('injected sessions reload failure')
  },
}
model.storage.resSecret = 'stale-resource-secret'
model.storage.sessSecret = 'stale-session-secret'
model.loadStorageSettings().then(() => {
  if (model.storage.resSecret !== '' || model.storage.sessSecret !== '') {
    throw new Error('storage reload retained a secret in the view model')
  }
  if (!model.storage.resHasSecret || model.storage.resSecretPreview !== 'pre••••••••tail') {
    throw new Error('storage preview state was not loaded')
  }
  const preserve = model.storageBody('resType', 'resEndpoint', 'resBucket', 'resAccess', 'resSecret', 'resClearSecret')
  if ('secret_key' in preserve || 'secret_preview' in preserve) {
    throw new Error('storage preview was submitted as a secret')
  }
  model.storage.resClearSecret = true
  const clear = model.storageBody('resType', 'resEndpoint', 'resBucket', 'resAccess', 'resSecret', 'resClearSecret')
  if (clear.secret_key !== undefined || clear.clear_secret !== true) {
    throw new Error('storage clear intent was not explicit')
  }
}).catch((error) => {
  process.stderr.write(error.stack + '\n')
  process.exit(1)
})
`
	if err := os.WriteFile(filepath.Join(dir, "storage-settings-regression.js"), []byte(regression), 0o600); err != nil {
		t.Fatalf("write storage settings regression: %v", err)
	}
	command := exec.Command("node", "storage-settings-regression.js")
	command.Dir = dir
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("storage settings reload must clear masked/stale secrets: %v\n%s", err, output)
	}
}

func TestConsoleStorageSettingsUsesRevisionCASAndReloadsOnConflict(t *testing.T) {
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/js/storage-settings.js", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("storage-settings.js status=%d", response.Code)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "storage-settings.js"), response.Body.Bytes(), 0o600); err != nil {
		t.Fatalf("write storage settings module: %v", err)
	}
	const regression = `
global.window = {}
require('./storage-settings.js')
const domain = window.HarnessConsoleStorageSettings
let gets = 0
let putBody = null
const model = {
  ...domain.state(),
  ...domain.methods,
  notify: () => {},
  load: () => {},
  api: async (method, path, body) => {
    if (method === 'GET' && path.endsWith('/resources')) {
      gets++
      return { found: true, config: { type: 's3', desired_revision: 'rev-new', has_secret: true, secret_preview: 'new••••••••tail' } }
    }
    if (method === 'PUT' && path.endsWith('/resources')) {
      putBody = body
      const error = new Error('revision conflict')
      error.status = 409
      throw error
    }
    throw new Error('unexpected storage request')
  },
}
model.storage.resType = 's3'
model.storage.resDesiredRevision = 'rev-old'
model.storage.resSecret = 'replacement-secret'
model.saveStorage('resources').then(() => {
  if (!putBody || putBody.expected_revision !== 'rev-old') throw new Error('expected revision was not sent')
  if (gets !== 1 || model.storage.resDesiredRevision !== 'rev-new') throw new Error('conflict did not reload latest revision')
  if (model.storage.resSecret !== '') throw new Error('conflict reload retained typed secret')
}).catch((error) => {
  process.stderr.write(error.stack + '\\n')
  process.exit(1)
})
`
	if err := os.WriteFile(filepath.Join(dir, "storage-settings-cas-regression.js"), []byte(regression), 0o600); err != nil {
		t.Fatalf("write storage CAS regression: %v", err)
	}
	command := exec.Command("node", "storage-settings-cas-regression.js")
	command.Dir = dir
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("storage settings CAS conflict must reload: %v\n%s", err, output)
	}
}
