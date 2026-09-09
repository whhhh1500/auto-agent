package programmatic

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	Version = "ptc-ir/v1"

	// LanguageGuide is concise model-facing syntax for the program.execute
	// source argument. Hosts may include it verbatim in a tool description.
	LanguageGuide = `program.execute source is one JSON object:
{"version":"ptc-ir/v1","body":[STATEMENT,...]}
Statements:
{"op":"assign","name":"x","value":EXPR}
{"op":"if","cond":EXPR,"then":[STATEMENT,...],"else":[STATEMENT,...]}
{"op":"for","var":"row","in":EXPR,"body":[STATEMENT,...]}
{"op":"call","assign":"rows","tool":"catalog.search","args":EXPR}
{"op":"append","target":"items","value":EXPR}
{"op":"return","value":EXPR}; {"op":"break"} is valid only in for.
Tool is a literal exposed tool name. args must evaluate to an object.
Expressions are JSON objects: {"op":"literal","value":null|BOOL|NUMBER|STRING},
{"op":"var","name":"x"}, {"op":"get","object":EXPR,"key":"field"},
{"op":"index","object":EXPR,"index":EXPR}, {"op":"len","value":EXPR},
{"op":"cmp","kind":"eq|ne|lt|le|gt|ge","left":EXPR,"right":EXPR},
{"op":"bool","kind":"not","value":EXPR},
{"op":"bool","kind":"and|or","left":EXPR,"right":EXPR},
{"op":"int","kind":"add|sub|mul|div|mod","left":EXPR,"right":EXPR},
{"op":"list","items":[EXPR,...]}, {"op":"map","entries":{"key":EXPR,...}}.
EXPR is an expression object with an op. A bare JSON array or object without
an expression op is invalid. literal.value is scalar only; construct arrays
with list/items and objects with map/entries, whose members are EXPR.
Use integer operations only on exact integers. Comparisons also support finite
JSON decimals with float64 precision. No dynamic tool names, functions, catch,
imports, I/O, time, random, recursion, or parallel work.
Strict runtime: only input is predefined; assign an empty list before append.
get requires an object and an existing key; index and for require a list; data
in a tool result envelope may be null. Each form accepts only the fields shown.
A program_invalid diagnostic is fixed and data-free; it does not mean earlier
tool effects were rolled back or that retrying is safe.`

	DefaultMaxSteps          uint64 = 10_000
	DefaultMaxLoopIterations uint64 = 1_000
	DefaultMaxToolCalls      uint64 = 64
	DefaultMaxContainerItems        = 1_000
	DefaultMaxStringBytes           = 16 << 10
	DefaultMaxArenaBytes            = 4 << 20

	HardMaxSourceBytes            = 64 << 10
	HardMaxJSONDepth              = 128
	HardMaxNodes                  = 512
	HardMaxDepth                  = 32
	HardMaxSteps           uint64 = 10_000
	HardMaxLoopIterations  uint64 = 1_000
	HardMaxToolCalls       uint64 = 1_024
	HardMaxContainerItems         = 1_000
	HardMaxStringBytes            = 16 << 10
	HardMaxArenaBytes             = 16 << 20
	HardMaxToolResultBytes        = 256 << 10
)

var (
	ErrInvalidProgram = errors.New("programmatic: invalid program")
	ErrProgramLimit   = errors.New("programmatic: resource limit exceeded")
)

// Diagnostic returns a fixed, data-free category for an error created by this
// interpreter. It never derives a category from an error string. A host which
// transports a previously returned VM error must preserve that error's origin
// separately; its type alone cannot establish where it came from.
func Diagnostic(err error) (string, bool) {
	var classified *classifiedError
	if !errors.As(err, &classified) {
		return "", false
	}
	return classified.code, true
}

type classifiedError struct {
	cause error
	code  string
}

func (e *classifiedError) Error() string { return e.cause.Error() }
func (e *classifiedError) Unwrap() error { return e.cause }

const (
	compileSourceInvalid     = "compile_source_invalid"
	compileJSONInvalid       = "compile_json_invalid"
	compileLimitExceeded     = "compile_limit_exceeded"
	compileTopLevelInvalid   = "compile_top_level_invalid"
	compileVersionInvalid    = "compile_version_invalid"
	compileBodyInvalid       = "compile_body_invalid"
	compileStatementInvalid  = "compile_statement_invalid"
	compileStatementOp       = "compile_statement_op_invalid"
	compileExpressionInvalid = "compile_expression_invalid"
	compileExpressionOp      = "compile_expression_op_invalid"
)

func compileInvalid(code string) error {
	return &classifiedError{cause: ErrInvalidProgram, code: code}
}
func runtimeInvalid(code string) error {
	return &classifiedError{cause: ErrInvalidProgram, code: code}
}

// compileStageError preserves an internal compiler phase while Compile
// converts the final result back to ErrInvalidProgram. It never escapes as a
// public error or includes source data.
type compileStageError struct {
	cause error
	code  string
}

func (e *compileStageError) Error() string { return e.cause.Error() }
func (e *compileStageError) Unwrap() error { return e.cause }

func compileFailure(code string) error {
	return &compileStageError{cause: ErrInvalidProgram, code: code}
}

func compileStage(err error, fallback string) error {
	if err == nil || errors.Is(err, ErrProgramLimit) {
		return err
	}
	var existing *compileStageError
	if errors.As(err, &existing) {
		return err
	}
	return &compileStageError{cause: err, code: fallback}
}

func compileDiagnostic(err error, fallback string) string {
	if errors.Is(err, ErrProgramLimit) {
		return compileLimitExceeded
	}
	var stage *compileStageError
	if errors.As(err, &stage) && stage.code != "" {
		return stage.code
	}
	return fallback
}

// callerError preserves an arbitrary host error through Run. The outer Run
// boundary unwraps it before returning, so a caller cannot gain a VM
// diagnostic merely by returning an error that happens to match a sentinel.
type callerError struct{ cause error }

func (e *callerError) Error() string { return e.cause.Error() }
func (e *callerError) Unwrap() error { return e.cause }

// Call is the complete host-visible identity input for one program tool call.
type Call struct {
	Tool    string
	Args    map[string]any
	Ordinal uint64
}

// Caller executes a single host-authorized tool call. Any returned error is
// propagated unchanged, including approval-pending and cancellation errors.
type Caller func(context.Context, Call) (any, error)

// Limits bounds one Run. Zero values use strict defaults. Values above the
// hard limits are rejected rather than silently clamped.
type Limits struct {
	MaxSteps          uint64
	MaxLoopIterations uint64
	MaxToolCalls      uint64
	MaxContainerItems int
	MaxStringBytes    int
	MaxArenaBytes     int
}

// Result is the detached result of one Run.
type Result struct {
	Value      any
	Steps      uint64
	ToolCalls  int
	ArenaBytes int
}

// Program is an immutable, validated IR and is safe to share by concurrent
// runs.
type Program struct {
	body   []statement
	tools  []string
	digest string
}

// Compile parses and validates a single JSON IR document. Source must be
// UTF-8, no larger than HardMaxSourceBytes, and contain exactly version/body.
func Compile(source []byte) (*Program, error) {
	if len(source) == 0 || len(source) > HardMaxSourceBytes || !utf8.Valid(source) {
		return nil, compileInvalid(compileSourceInvalid)
	}
	if err := validateJSONStructure(source); err != nil {
		return nil, compileInvalid(compileDiagnostic(err, compileJSONInvalid))
	}
	decoder := json.NewDecoder(bytes.NewReader(source))
	decoder.UseNumber()
	var root any
	if err := decoder.Decode(&root); err != nil || decoder.More() {
		return nil, compileInvalid(compileJSONInvalid)
	}
	if err := ensureDecoderEOF(decoder); err != nil {
		return nil, compileInvalid(compileJSONInvalid)
	}
	object, ok := root.(map[string]any)
	if !ok || len(object) != 2 {
		return nil, compileInvalid(compileTopLevelInvalid)
	}
	if stringValue(object["version"]) != Version {
		return nil, compileInvalid(compileVersionInvalid)
	}
	bodyRaw, ok := object["body"].([]any)
	if !ok || len(bodyRaw) == 0 {
		return nil, compileInvalid(compileBodyInvalid)
	}
	compiler := compiler{}
	body, err := compiler.statements(bodyRaw, 1, false)
	if err != nil || compiler.nodes > HardMaxNodes {
		return nil, compileInvalid(compileDiagnostic(err, compileLimitExceeded))
	}
	canonical, err := json.Marshal(root)
	if err != nil {
		return nil, compileInvalid(compileJSONInvalid)
	}
	sum := sha256.Sum256(canonical)
	tools := make([]string, 0, len(compiler.tools))
	for tool := range compiler.tools {
		tools = append(tools, tool)
	}
	sort.Strings(tools)
	return &Program{body: body, tools: tools, digest: hex.EncodeToString(sum[:])}, nil
}

// Tools returns sorted, unique compile-time literal tool names.
func (p *Program) Tools() []string {
	if p == nil {
		return nil
	}
	return append([]string(nil), p.tools...)
}

// Digest is SHA-256 over canonical JSON after syntax validation.
func (p *Program) Digest() string {
	if p == nil {
		return ""
	}
	return p.digest
}

// Run executes a Program against detached JSON-like input. It has no ambient
// host access; the only side-effect boundary is Caller.
func (p *Program) Run(ctx context.Context, input map[string]any, caller Caller, limits Limits) (Result, error) {
	if p == nil || caller == nil {
		return Result{}, runtimeInvalid("runtime_invalid")
	}
	if ctx == nil {
		return Result{}, runtimeInvalid("runtime_invalid")
	}
	resolved, err := resolveLimits(limits)
	if err != nil {
		return Result{}, err
	}
	run := runtime{ctx: ctx, caller: caller, limits: resolved, vars: make(map[string]any)}
	cloned, err := run.cloneValue(input, 0)
	if err != nil {
		if errors.Is(err, ErrInvalidProgram) {
			return Result{}, runtimeInvalid("runtime_invalid")
		}
		return Result{}, err
	}
	run.vars["input"] = cloned
	state, err := run.statements(p.body, 0)
	if err != nil {
		var fromCaller *callerError
		if errors.As(err, &fromCaller) {
			return Result{}, fromCaller.cause
		}
		if errors.Is(err, ErrProgramLimit) || !errors.Is(err, ErrInvalidProgram) {
			return Result{}, err
		}
		if _, ok := Diagnostic(err); ok {
			return Result{}, err
		}
		return Result{}, runtimeInvalid("runtime_invalid")
	}
	value := state.value
	if state.signal != signalReturn {
		value = nil
	}
	// Expressions may intentionally reuse a container through variables. Return
	// a detached bounded copy so a small internal DAG cannot expand without
	// limit when an embedding host later serializes Result.Value.
	// A returned tool envelope may contain bounded raw content larger than a
	// source-language string. It remains subject to the 256 KiB result cap and
	// arena cap, but does not inherit the 16 KiB source-string limit.
	value, err = run.cloneValueWithStringLimit(value, 0, HardMaxToolResultBytes)
	if err != nil {
		if errors.Is(err, ErrInvalidProgram) {
			return Result{}, runtimeInvalid("runtime_invalid")
		}
		return Result{}, err
	}
	return Result{Value: value, Steps: run.steps, ToolCalls: int(run.toolCalls), ArenaBytes: run.arena}, nil
}

type resolvedLimits struct {
	maxSteps, maxLoops, maxTools  uint64
	maxItems, maxString, maxArena int
}

func resolveLimits(input Limits) (resolvedLimits, error) {
	if input.MaxSteps == 0 {
		input.MaxSteps = DefaultMaxSteps
	}
	if input.MaxLoopIterations == 0 {
		input.MaxLoopIterations = DefaultMaxLoopIterations
	}
	if input.MaxToolCalls == 0 {
		input.MaxToolCalls = DefaultMaxToolCalls
	}
	if input.MaxContainerItems == 0 {
		input.MaxContainerItems = DefaultMaxContainerItems
	}
	if input.MaxStringBytes == 0 {
		input.MaxStringBytes = DefaultMaxStringBytes
	}
	if input.MaxArenaBytes == 0 {
		input.MaxArenaBytes = DefaultMaxArenaBytes
	}
	if input.MaxSteps > HardMaxSteps || input.MaxLoopIterations > HardMaxLoopIterations || input.MaxToolCalls > HardMaxToolCalls ||
		input.MaxContainerItems < 1 || input.MaxContainerItems > HardMaxContainerItems || input.MaxStringBytes < 1 || input.MaxStringBytes > HardMaxStringBytes || input.MaxArenaBytes < 1 || input.MaxArenaBytes > HardMaxArenaBytes {
		return resolvedLimits{}, ErrProgramLimit
	}
	return resolvedLimits{input.MaxSteps, input.MaxLoopIterations, input.MaxToolCalls, input.MaxContainerItems, input.MaxStringBytes, input.MaxArenaBytes}, nil
}

type statementKind uint8

const (
	stmtAssign statementKind = iota
	stmtIf
	stmtFor
	stmtCall
	stmtAppend
	stmtReturn
	stmtBreak
)

type statement struct {
	kind                    statementKind
	name, tool              string
	value, cond, iter, args *expression
	body, otherwise         []statement
}
type expressionKind uint8

const (
	exprLiteral expressionKind = iota
	exprVar
	exprGet
	exprIndex
	exprLen
	exprCompare
	exprBool
	exprInt
	exprList
	exprMap
)

type expression struct {
	kind                              expressionKind
	name, op                          string
	literal                           any
	left, right, object, index, value *expression
	args                              []*expression
	entries                           []mapEntry
}
type mapEntry struct {
	key   string
	value *expression
}

type compiler struct {
	nodes int
	calls uint64
	tools map[string]struct{}
}

func (c *compiler) node(depth int) error {
	c.nodes++
	if depth > HardMaxDepth || c.nodes > HardMaxNodes {
		return ErrProgramLimit
	}
	return nil
}
func (c *compiler) statements(raw []any, depth int, loop bool) ([]statement, error) {
	if len(raw) > HardMaxNodes {
		return nil, compileFailure(compileLimitExceeded)
	}
	out := make([]statement, 0, len(raw))
	for _, item := range raw {
		st, err := c.statement(item, depth, loop)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}
func (c *compiler) statement(raw any, depth int, loop bool) (result statement, err error) {
	defer func() { err = compileStage(err, compileStatementInvalid) }()
	if err := c.node(depth); err != nil {
		return statement{}, err
	}
	o, ok := raw.(map[string]any)
	if !ok {
		return statement{}, ErrInvalidProgram
	}
	op := stringValue(o["op"])
	switch op {
	case "assign":
		if !exactKeys(o, "op", "name", "value") || !validName(stringValue(o["name"])) {
			return statement{}, ErrInvalidProgram
		}
		value, err := c.expression(o["value"], depth+1)
		return statement{kind: stmtAssign, name: stringValue(o["name"]), value: value}, err
	case "if":
		if !allowedKeys(o, "op", "cond", "then", "else") {
			return statement{}, ErrInvalidProgram
		}
		cond, err := c.expression(o["cond"], depth+1)
		if err != nil {
			return statement{}, err
		}
		then, ok := o["then"].([]any)
		if !ok || len(then) == 0 {
			return statement{}, ErrInvalidProgram
		}
		body, err := c.statements(then, depth+1, loop)
		if err != nil {
			return statement{}, err
		}
		var otherwise []statement
		if rawElse, exists := o["else"]; exists {
			items, ok := rawElse.([]any)
			if !ok {
				return statement{}, ErrInvalidProgram
			}
			otherwise, err = c.statements(items, depth+1, loop)
			if err != nil {
				return statement{}, err
			}
		}
		return statement{kind: stmtIf, cond: cond, body: body, otherwise: otherwise}, nil
	case "for":
		if !exactKeys(o, "op", "var", "in", "body") || !validName(stringValue(o["var"])) {
			return statement{}, ErrInvalidProgram
		}
		iter, err := c.expression(o["in"], depth+1)
		if err != nil {
			return statement{}, err
		}
		rawBody, ok := o["body"].([]any)
		if !ok || len(rawBody) == 0 {
			return statement{}, ErrInvalidProgram
		}
		body, err := c.statements(rawBody, depth+1, true)
		return statement{kind: stmtFor, name: stringValue(o["var"]), iter: iter, body: body}, err
	case "call":
		if !allowedKeys(o, "op", "assign", "tool", "args") || !validTool(stringValue(o["tool"])) {
			return statement{}, ErrInvalidProgram
		}
		if assign, exists := o["assign"]; exists && !validName(stringValue(assign)) {
			return statement{}, ErrInvalidProgram
		}
		args, err := c.expression(o["args"], depth+1)
		if err != nil {
			return statement{}, err
		}
		c.calls++
		if c.calls > HardMaxToolCalls {
			return statement{}, ErrProgramLimit
		}
		if c.tools == nil {
			c.tools = map[string]struct{}{}
		}
		tool := stringValue(o["tool"])
		c.tools[tool] = struct{}{}
		return statement{kind: stmtCall, name: stringValue(o["assign"]), tool: tool, args: args}, nil
	case "append":
		if !exactKeys(o, "op", "target", "value") || !validName(stringValue(o["target"])) {
			return statement{}, ErrInvalidProgram
		}
		value, err := c.expression(o["value"], depth+1)
		return statement{kind: stmtAppend, name: stringValue(o["target"]), value: value}, err
	case "return":
		if !exactKeys(o, "op", "value") {
			return statement{}, ErrInvalidProgram
		}
		value, err := c.expression(o["value"], depth+1)
		return statement{kind: stmtReturn, value: value}, err
	case "break":
		if !exactKeys(o, "op") || !loop {
			return statement{}, ErrInvalidProgram
		}
		return statement{kind: stmtBreak}, nil
	default:
		return statement{}, compileFailure(compileStatementOp)
	}
}
func (c *compiler) expression(raw any, depth int) (result *expression, err error) {
	defer func() { err = compileStage(err, compileExpressionInvalid) }()
	if err := c.node(depth); err != nil {
		return nil, err
	}
	o, ok := raw.(map[string]any)
	if !ok {
		return nil, ErrInvalidProgram
	}
	op := stringValue(o["op"])
	compileBinary := func(kind expressionKind) (*expression, error) {
		if !exactKeys(o, "op", "kind", "left", "right") {
			return nil, ErrInvalidProgram
		}
		operator := stringValue(o["kind"])
		if (kind == exprCompare && !validCompare(operator)) || (kind == exprInt && !validInt(operator)) {
			return nil, ErrInvalidProgram
		}
		left, e := c.expression(o["left"], depth+1)
		if e != nil {
			return nil, e
		}
		right, e := c.expression(o["right"], depth+1)
		return &expression{kind: kind, op: operator, left: left, right: right}, e
	}
	switch op {
	case "literal":
		if !exactKeys(o, "op", "value") {
			return nil, ErrInvalidProgram
		}
		v, e := compileLiteral(o["value"])
		return &expression{kind: exprLiteral, literal: v}, e
	case "var":
		if !exactKeys(o, "op", "name") || !validName(stringValue(o["name"])) {
			return nil, ErrInvalidProgram
		}
		return &expression{kind: exprVar, name: stringValue(o["name"])}, nil
	case "get":
		if !exactKeys(o, "op", "object", "key") || !validMapKey(stringValue(o["key"])) {
			return nil, ErrInvalidProgram
		}
		v, e := c.expression(o["object"], depth+1)
		return &expression{kind: exprGet, object: v, name: stringValue(o["key"])}, e
	case "index":
		if !exactKeys(o, "op", "object", "index") {
			return nil, ErrInvalidProgram
		}
		v, e := c.expression(o["object"], depth+1)
		if e != nil {
			return nil, e
		}
		i, e := c.expression(o["index"], depth+1)
		return &expression{kind: exprIndex, object: v, index: i}, e
	case "len":
		if !exactKeys(o, "op", "value") {
			return nil, ErrInvalidProgram
		}
		v, e := c.expression(o["value"], depth+1)
		return &expression{kind: exprLen, value: v}, e
	case "cmp":
		return compileBinary(exprCompare)
	case "int":
		return compileBinary(exprInt)
	case "bool":
		kind := stringValue(o["kind"])
		if kind == "not" {
			if !exactKeys(o, "op", "kind", "value") {
				return nil, ErrInvalidProgram
			}
			v, e := c.expression(o["value"], depth+1)
			return &expression{kind: exprBool, op: kind, value: v}, e
		}
		if (kind != "and" && kind != "or") || !exactKeys(o, "op", "kind", "left", "right") {
			return nil, ErrInvalidProgram
		}
		return compileBinary(exprBool)
	case "list":
		if !exactKeys(o, "op", "items") {
			return nil, ErrInvalidProgram
		}
		items, ok := o["items"].([]any)
		if !ok || len(items) > HardMaxContainerItems {
			return nil, ErrInvalidProgram
		}
		out := make([]*expression, 0, len(items))
		for _, item := range items {
			v, e := c.expression(item, depth+1)
			if e != nil {
				return nil, e
			}
			out = append(out, v)
		}
		return &expression{kind: exprList, args: out}, nil
	case "map":
		if !exactKeys(o, "op", "entries") {
			return nil, ErrInvalidProgram
		}
		entries, ok := o["entries"].(map[string]any)
		if !ok || len(entries) > HardMaxContainerItems {
			return nil, ErrInvalidProgram
		}
		keys := make([]string, 0, len(entries))
		for key := range entries {
			if !validMapKey(key) {
				return nil, ErrInvalidProgram
			}
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out := make([]mapEntry, 0, len(keys))
		for _, key := range keys {
			v, e := c.expression(entries[key], depth+1)
			if e != nil {
				return nil, e
			}
			out = append(out, mapEntry{key, v})
		}
		return &expression{kind: exprMap, entries: out}, nil
	default:
		return nil, compileFailure(compileExpressionOp)
	}
}

func compileLiteral(v any) (any, error) {
	switch value := v.(type) {
	case nil, bool, string:
		if s, ok := value.(string); ok && len(s) > HardMaxStringBytes {
			return nil, ErrInvalidProgram
		}
		return value, nil
	case json.Number:
		return numberValue(value)
	default:
		return nil, ErrInvalidProgram
	}
}
func numberValue(number json.Number) (any, error) {
	s := number.String()
	if !strings.ContainsAny(s, ".eE") {
		if integer, err := strconv.ParseInt(s, 10, 64); err == nil {
			return integer, nil
		}
	}
	floating, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(floating) || math.IsInf(floating, 0) {
		return nil, ErrInvalidProgram
	}
	return floating, nil
}
func stringValue(v any) string { s, _ := v.(string); return s }
func exactKeys(o map[string]any, keys ...string) bool {
	if len(o) != len(keys) {
		return false
	}
	return allowedKeys(o, keys...)
}
func allowedKeys(o map[string]any, keys ...string) bool {
	allowed := make(map[string]bool, len(keys))
	for _, key := range keys {
		allowed[key] = true
	}
	for key := range o {
		if !allowed[key] {
			return false
		}
	}
	return true
}
func validName(value string) bool { return validToken(value, 64) }
func validTool(value string) bool { return validToken(value, 192) }
func validMapKey(value string) bool {
	return value != "" && len(value) <= 128 && utf8.ValidString(value) && !strings.ContainsAny(value, "\r\n\x00")
}
func validToken(value string, max int) bool {
	if value == "" || len(value) > max || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r == '\r' || r == '\n' || r == '\x00' || r == ' ' || r == '\t' {
			return false
		}
	}
	return true
}
func validCompare(value string) bool {
	switch value {
	case "eq", "ne", "lt", "le", "gt", "ge":
		return true
	default:
		return false
	}
}
func validInt(value string) bool {
	switch value {
	case "add", "sub", "mul", "div", "mod":
		return true
	default:
		return false
	}
}
func ensureDecoderEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	}
	return ErrInvalidProgram
}

// validateJSONStructure rejects duplicate object keys before decoding into Go
// maps, where encoding/json otherwise keeps only the final duplicate. It also
// bounds raw JSON nesting before IR compilation begins.
func validateJSONStructure(source []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(source))
	decoder.UseNumber()
	if err := validateJSONValue(decoder, 0); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ErrInvalidProgram
	}
	return nil
}

func validateJSONValue(decoder *json.Decoder, depth int) error {
	if depth > HardMaxJSONDepth {
		return ErrProgramLimit
	}
	token, err := decoder.Token()
	if err != nil {
		return ErrInvalidProgram
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		switch token.(type) {
		case nil, bool, string, json.Number:
			return nil
		default:
			return ErrInvalidProgram
		}
	}
	switch delimiter {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			key, ok := keyToken.(string)
			if err != nil || !ok {
				return ErrInvalidProgram
			}
			if _, duplicate := seen[key]; duplicate {
				return ErrInvalidProgram
			}
			seen[key] = struct{}{}
			if err := validateJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return ErrInvalidProgram
		}
		return nil
	case '[':
		for decoder.More() {
			if err := validateJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return ErrInvalidProgram
		}
		return nil
	default:
		return ErrInvalidProgram
	}
}

type signal uint8

const (
	signalNone signal = iota
	signalBreak
	signalReturn
)

type runState struct {
	signal signal
	value  any
}
type runtime struct {
	ctx                     context.Context
	caller                  Caller
	limits                  resolvedLimits
	vars                    map[string]any
	steps, toolCalls, loops uint64
	arena                   int
}

func (r *runtime) statements(body []statement, loopDepth int) (runState, error) {
	for _, statement := range body {
		if err := r.step(); err != nil {
			return runState{}, err
		}
		state, err := r.statement(statement, loopDepth)
		if err != nil || state.signal != signalNone {
			return state, err
		}
	}
	return runState{}, nil
}
func (r *runtime) statement(statement statement, loopDepth int) (runState, error) {
	switch statement.kind {
	case stmtAssign:
		value, err := r.expression(statement.value)
		if err != nil {
			return runState{}, err
		}
		r.vars[statement.name] = value
	case stmtIf:
		condition, err := r.expression(statement.cond)
		if err != nil {
			return runState{}, err
		}
		if truthy(condition) {
			return r.statements(statement.body, loopDepth)
		}
		return r.statements(statement.otherwise, loopDepth)
	case stmtFor:
		iterable, err := r.expression(statement.iter)
		if err != nil {
			return runState{}, err
		}
		values, ok := iterable.([]any)
		if !ok {
			return runState{}, runtimeInvalid("runtime_for_not_list")
		}
		if len(values) > r.limits.maxItems {
			return runState{}, ErrProgramLimit
		}
		for _, value := range values {
			r.loops++
			if r.loops > r.limits.maxLoops {
				return runState{}, ErrProgramLimit
			}
			r.vars[statement.name] = value
			state, err := r.statements(statement.body, loopDepth+1)
			if err != nil {
				return runState{}, err
			}
			if state.signal == signalReturn {
				return state, nil
			}
			if state.signal == signalBreak {
				break
			}
		}
	case stmtCall:
		if r.toolCalls >= r.limits.maxTools {
			return runState{}, ErrProgramLimit
		}
		value, err := r.expression(statement.args)
		if err != nil {
			return runState{}, err
		}
		args, ok := value.(map[string]any)
		if !ok {
			return runState{}, runtimeInvalid("runtime_call_args_not_object")
		}
		detached, err := r.cloneValue(args, 0)
		if err != nil {
			return runState{}, err
		}
		callArgs, ok := detached.(map[string]any)
		if !ok {
			return runState{}, runtimeInvalid("runtime_call_args_not_object")
		}
		r.toolCalls++
		result, err := r.caller(r.ctx, Call{Tool: statement.tool, Args: callArgs, Ordinal: r.toolCalls})
		if err != nil {
			return runState{}, &callerError{cause: err}
		}
		result, err = r.cloneCallerResult(result)
		if err != nil {
			return runState{}, err
		}
		if statement.name != "" {
			r.vars[statement.name] = result
		}
	case stmtAppend:
		value, err := r.expression(statement.value)
		if err != nil {
			return runState{}, err
		}
		current, ok := r.vars[statement.name].([]any)
		if !ok {
			return runState{}, runtimeInvalid("runtime_append_target_not_list")
		}
		if len(current) >= r.limits.maxItems {
			return runState{}, ErrProgramLimit
		}
		if err := r.charge(8); err != nil {
			return runState{}, err
		}
		r.vars[statement.name] = append(current, value)
	case stmtReturn:
		value, err := r.expression(statement.value)
		if err != nil {
			return runState{}, err
		}
		return runState{signal: signalReturn, value: value}, nil
	case stmtBreak:
		if loopDepth == 0 {
			return runState{}, ErrInvalidProgram
		}
		return runState{signal: signalBreak}, nil
	default:
		return runState{}, ErrInvalidProgram
	}
	return runState{}, nil
}

func (r *runtime) expression(expression *expression) (any, error) {
	if err := r.step(); err != nil {
		return nil, err
	}
	switch expression.kind {
	case exprLiteral:
		return r.cloneValue(expression.literal, 0)
	case exprVar:
		value, ok := r.vars[expression.name]
		if !ok {
			return nil, runtimeInvalid("runtime_variable_missing")
		}
		return value, nil
	case exprGet:
		object, err := r.expression(expression.object)
		if err != nil {
			return nil, err
		}
		values, ok := object.(map[string]any)
		if !ok {
			return nil, runtimeInvalid("runtime_get_not_object")
		}
		value, ok := values[expression.name]
		if !ok {
			return nil, runtimeInvalid("runtime_get_missing_key")
		}
		return value, nil
	case exprIndex:
		object, err := r.expression(expression.object)
		if err != nil {
			return nil, err
		}
		index, err := r.expression(expression.index)
		if err != nil {
			return nil, err
		}
		integer, ok := index.(int64)
		if !ok || integer < 0 {
			return nil, runtimeInvalid("runtime_index_invalid")
		}
		values, ok := object.([]any)
		if !ok || integer >= int64(len(values)) {
			return nil, runtimeInvalid("runtime_index_invalid")
		}
		return values[integer], nil
	case exprLen:
		value, err := r.expression(expression.value)
		if err != nil {
			return nil, err
		}
		switch typed := value.(type) {
		case string:
			return int64(len(typed)), nil
		case []any:
			return int64(len(typed)), nil
		case map[string]any:
			return int64(len(typed)), nil
		default:
			return nil, runtimeInvalid("runtime_length_unsupported")
		}
	case exprCompare:
		left, err := r.expression(expression.left)
		if err != nil {
			return nil, err
		}
		right, err := r.expression(expression.right)
		if err != nil {
			return nil, err
		}
		value, err := compare(expression.op, left, right)
		if errors.Is(err, ErrInvalidProgram) {
			return nil, runtimeInvalid("runtime_compare_incompatible")
		}
		return value, err
	case exprBool:
		if expression.op == "not" {
			value, err := r.expression(expression.value)
			if err != nil {
				return nil, err
			}
			return !truthy(value), nil
		}
		left, err := r.expression(expression.left)
		if err != nil {
			return nil, err
		}
		if expression.op == "and" {
			if !truthy(left) {
				return false, nil
			}
			right, err := r.expression(expression.right)
			return truthy(right), err
		}
		if truthy(left) {
			return true, nil
		}
		right, err := r.expression(expression.right)
		return truthy(right), err
	case exprInt:
		left, err := r.expression(expression.left)
		if err != nil {
			return nil, err
		}
		right, err := r.expression(expression.right)
		if err != nil {
			return nil, err
		}
		value, err := intOperation(expression.op, left, right)
		if errors.Is(err, ErrInvalidProgram) {
			return nil, runtimeInvalid("runtime_integer_invalid")
		}
		return value, err
	case exprList:
		if len(expression.args) > r.limits.maxItems {
			return nil, ErrProgramLimit
		}
		if err := r.charge(16 + len(expression.args)*8); err != nil {
			return nil, err
		}
		out := make([]any, 0, len(expression.args))
		for _, item := range expression.args {
			value, err := r.expression(item)
			if err != nil {
				return nil, err
			}
			out = append(out, value)
		}
		return out, nil
	case exprMap:
		if len(expression.entries) > r.limits.maxItems {
			return nil, ErrProgramLimit
		}
		if err := r.charge(32 + len(expression.entries)*16); err != nil {
			return nil, err
		}
		out := make(map[string]any, len(expression.entries))
		for _, entry := range expression.entries {
			value, err := r.expression(entry.value)
			if err != nil {
				return nil, err
			}
			out[entry.key] = value
		}
		return out, nil
	default:
		return nil, ErrInvalidProgram
	}
}
func (r *runtime) step() error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	r.steps++
	if r.steps > r.limits.maxSteps {
		return ErrProgramLimit
	}
	return nil
}
func (r *runtime) charge(bytes int) error {
	if bytes < 0 || bytes > r.limits.maxArena-r.arena {
		return ErrProgramLimit
	}
	r.arena += bytes
	return nil
}
func (r *runtime) cloneValue(value any, depth int) (any, error) {
	return r.cloneValueWithStringLimit(value, depth, r.limits.maxString)
}
func (r *runtime) cloneCallerResult(value any) (any, error) {
	before := r.arena
	cloned, err := r.cloneValueWithStringLimit(value, 0, HardMaxToolResultBytes)
	if err != nil {
		return nil, err
	}
	if r.arena-before > HardMaxToolResultBytes {
		return nil, ErrProgramLimit
	}
	return cloned, nil
}
func (r *runtime) cloneValueWithStringLimit(value any, depth int, maxString int) (any, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if depth > HardMaxDepth {
		return nil, ErrProgramLimit
	}
	switch typed := value.(type) {
	case nil:
		if err := r.charge(1); err != nil {
			return nil, err
		}
		return nil, nil
	case bool:
		if err := r.charge(1); err != nil {
			return nil, err
		}
		return typed, nil
	case int64:
		if err := r.charge(8); err != nil {
			return nil, err
		}
		return typed, nil
	case int:
		return r.cloneValueWithStringLimit(int64(typed), depth, maxString)
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return nil, ErrInvalidProgram
		}
		if err := r.charge(8); err != nil {
			return nil, err
		}
		return typed, nil
	case json.Number:
		value, err := numberValue(typed)
		if err != nil {
			return nil, err
		}
		return r.cloneValueWithStringLimit(value, depth, maxString)
	case string:
		if len(typed) > maxString {
			return nil, ErrProgramLimit
		}
		if err := r.charge(len(typed) + 16); err != nil {
			return nil, err
		}
		return typed, nil
	case []any:
		if len(typed) > r.limits.maxItems {
			return nil, ErrProgramLimit
		}
		if err := r.charge(16 + len(typed)*8); err != nil {
			return nil, err
		}
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			value, err := r.cloneValueWithStringLimit(item, depth+1, maxString)
			if err != nil {
				return nil, err
			}
			out = append(out, value)
		}
		return out, nil
	case map[string]any:
		if len(typed) > r.limits.maxItems {
			return nil, ErrProgramLimit
		}
		if err := r.charge(32 + len(typed)*16); err != nil {
			return nil, err
		}
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			if !validMapKey(key) {
				return nil, ErrInvalidProgram
			}
			if err := r.charge(len(key)); err != nil {
				return nil, err
			}
			value, err := r.cloneValueWithStringLimit(item, depth+1, maxString)
			if err != nil {
				return nil, err
			}
			out[key] = value
		}
		return out, nil
	default:
		return nil, ErrInvalidProgram
	}
}
func truthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case int64:
		return typed != 0
	case float64:
		return typed != 0
	case string:
		return typed != ""
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	default:
		return false
	}
}
func compare(operator string, left, right any) (bool, error) {
	if result, handled, err := compareNumbers(operator, left, right); handled || err != nil {
		return result, err
	}
	if operator == "eq" {
		return equalValue(left, right), nil
	}
	if operator == "ne" {
		return !equalValue(left, right), nil
	}
	if leftString, ok := left.(string); ok {
		if rightString, ok := right.(string); ok {
			switch operator {
			case "lt":
				return leftString < rightString, nil
			case "le":
				return leftString <= rightString, nil
			case "gt":
				return leftString > rightString, nil
			case "ge":
				return leftString >= rightString, nil
			}
		}
	}
	return false, ErrInvalidProgram
}

const maxExactlyRepresentableFloatInt int64 = 1 << 53

// compareNumbers never converts two integers to float64. A mixed comparison is
// accepted only when the integer is exactly representable as float64; otherwise
// a model cannot use a rounded decimal to route a tool call.
func compareNumbers(operator string, left, right any) (result bool, handled bool, err error) {
	leftInt, leftIsInt := left.(int64)
	leftFloat, leftIsFloat := left.(float64)
	rightInt, rightIsInt := right.(int64)
	rightFloat, rightIsFloat := right.(float64)

	switch {
	case leftIsInt && rightIsInt:
		return compareInt64(operator, leftInt, rightInt), true, nil
	case leftIsFloat && rightIsFloat:
		return compareFloat64(operator, leftFloat, rightFloat), true, nil
	case leftIsInt && rightIsFloat:
		converted, ok := intAsExactFloat(leftInt)
		if !ok {
			return false, true, ErrInvalidProgram
		}
		return compareFloat64(operator, converted, rightFloat), true, nil
	case leftIsFloat && rightIsInt:
		converted, ok := intAsExactFloat(rightInt)
		if !ok {
			return false, true, ErrInvalidProgram
		}
		return compareFloat64(operator, leftFloat, converted), true, nil
	default:
		return false, false, nil
	}
}

func intAsExactFloat(value int64) (float64, bool) {
	if value < -maxExactlyRepresentableFloatInt || value > maxExactlyRepresentableFloatInt {
		return 0, false
	}
	return float64(value), true
}

func compareInt64(operator string, left, right int64) bool {
	switch operator {
	case "eq":
		return left == right
	case "ne":
		return left != right
	case "lt":
		return left < right
	case "le":
		return left <= right
	case "gt":
		return left > right
	case "ge":
		return left >= right
	default:
		return false
	}
}

func compareFloat64(operator string, left, right float64) bool {
	switch operator {
	case "eq":
		return left == right
	case "ne":
		return left != right
	case "lt":
		return left < right
	case "le":
		return left <= right
	case "gt":
		return left > right
	case "ge":
		return left >= right
	default:
		return false
	}
}
func equalValue(left, right any) bool {
	if result, handled, err := compareNumbers("eq", left, right); handled && err == nil {
		return result
	}
	switch a := left.(type) {
	case nil:
		return right == nil
	case bool:
		b, ok := right.(bool)
		return ok && a == b
	case string:
		b, ok := right.(string)
		return ok && a == b
	default:
		return false
	}
}
func intOperation(operator string, left, right any) (int64, error) {
	a, ok := left.(int64)
	if !ok {
		return 0, ErrInvalidProgram
	}
	b, ok := right.(int64)
	if !ok {
		return 0, ErrInvalidProgram
	}
	switch operator {
	case "add":
		if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
			return 0, ErrInvalidProgram
		}
		return a + b, nil
	case "sub":
		if (b < 0 && a > math.MaxInt64+b) || (b > 0 && a < math.MinInt64+b) {
			return 0, ErrInvalidProgram
		}
		return a - b, nil
	case "mul":
		if a != 0 && ((a == -1 && b == math.MinInt64) || (b == -1 && a == math.MinInt64) || a*b/a != b) {
			return 0, ErrInvalidProgram
		}
		return a * b, nil
	case "div":
		if b == 0 || (a == math.MinInt64 && b == -1) {
			return 0, ErrInvalidProgram
		}
		return a / b, nil
	case "mod":
		if b == 0 {
			return 0, ErrInvalidProgram
		}
		return a % b, nil
	default:
		return 0, ErrInvalidProgram
	}
}
