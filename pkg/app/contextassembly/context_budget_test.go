package contextassembly

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/cc-auto-agent/harness-core/pkg/core"
)

func TestAssembleRequiredAndGrouped(t *testing.T) {
	r := Request{Budget: Budget{TotalBytes: 180}, Estimator: ByteEstimator{}, RequiredSystem: "sys", RequiredProfile: "profile", Items: []Item{
		{Layer: LayerRecent, SourceID: "a", Revision: "1", GroupID: "pair", Message: core.ChatMessage{Role: core.RoleAssistant, Content: strings.Repeat("x", 80)}},
		{Layer: LayerToolResults, SourceID: "b", Revision: "1", GroupID: "pair", Message: core.ChatMessage{Role: core.RoleTool, Content: "result", ToolCallID: "id"}},
	}}
	got, err := Assemble(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if got.System != "sys" || got.Profile != "profile" || got.Evidence.Selected != 0 || got.Evidence.Dropped != 2 {
		t.Fatalf("%#v", got)
	}
}

func TestBudgetTokenZeroDisablesGateAndExplicitLayerZeroDenies(t *testing.T) {
	r := Request{Budget: Budget{TotalBytes: 1000, TotalTokens: 0, LayerBytes: map[Layer]int64{LayerRecent: 0}}, Estimator: CompositeBudgetEstimator{Tokenizer: tokenFunc(func(core.ChatMessage) (int64, error) { return 999, nil })}, Items: []Item{{Layer: LayerRecent, SourceID: "r", Revision: "1", Message: core.ChatMessage{Role: core.RoleUser, Content: "x"}}}}
	got, err := Assemble(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if got.Evidence.Dropped != 1 {
		t.Fatalf("explicit deny not applied: %#v", got)
	}
	delete(r.Budget.LayerBytes, LayerRecent)
	got, err = Assemble(context.Background(), r)
	if err != nil || got.Evidence.Selected != 1 {
		t.Fatalf("token zero should disable: %#v %v", got, err)
	}
}

func TestMetadataValidationAndRequiredOverflow(t *testing.T) {
	r := Request{Budget: Budget{TotalBytes: 1}, Estimator: ByteEstimator{}, RequiredSystem: "too long"}
	if _, err := Assemble(context.Background(), r); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatal(err)
	}
	r = Request{Budget: Budget{TotalBytes: 100}, Estimator: ByteEstimator{}, Items: []Item{{Layer: LayerRecent, SourceID: "bad\n", Revision: "1", Message: core.ChatMessage{Role: core.RoleUser}}}}
	if _, err := Assemble(context.Background(), r); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestByteEstimatorNeverUnderestimatesJSON(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	samples := []string{"<&>", "\u2028\u2029", "中文🙂"}
	for i := 0; i < 32; i++ {
		content := samples[i%len(samples)] + strings.Repeat("x", rng.Intn(40))
		m := core.ChatMessage{Role: core.RoleAssistant, Content: content, ToolCalls: []core.ToolCall{{ID: "a", Name: "one", Args: map[string]any{"nested": []any{content, true}}}, {ID: "b", Name: "two", Args: map[string]any{"n": float64(i)}}}}
		raw, e := json.Marshal(m)
		if e != nil {
			t.Fatal(e)
		}
		c, e := ByteEstimator{}.Estimate(m)
		if e != nil || c.Bytes < int64(len(raw)) {
			t.Fatalf("cost=%v raw=%d err=%v", c, len(raw), e)
		}
	}
}

func TestInvalidArgsFailClosed(t *testing.T) {
	bad := []any{map[string]any{"x": math.NaN()}, map[string]any{}}
	bad[1].(map[string]any)["self"] = bad[1]
	for _, v := range bad {
		r := Request{Budget: Budget{TotalBytes: 1000}, Estimator: lowEstimator{}, Items: []Item{{Layer: LayerRecent, SourceID: "x", Revision: "1", Message: core.ChatMessage{Role: core.RoleTool, Content: "ok", ToolCall: &core.ToolCall{ID: "c", Name: "n", Args: map[string]any{"v": v}}}}}}
		func() {
			defer func() {
				if recover() != nil {
					t.Fatal("panic")
				}
			}()
			if _, e := Assemble(context.Background(), r); !errors.Is(e, ErrInvalid) {
				t.Fatalf("err=%v", e)
			}
		}()
	}
}

func TestAssemblyBoundaryAndStableProtocolOrder(t *testing.T) {
	items := []Item{
		{Layer: LayerToolResults, SourceID: "tool", Revision: "1", Message: core.ChatMessage{Role: core.RoleTool, Content: "tool", ToolCallID: "id"}},
		{Layer: LayerSummary, SourceID: "summary", Revision: "1", Message: core.ChatMessage{Role: core.RoleUser, Content: "summary"}},
		{Layer: LayerRecent, SourceID: "recent", Revision: "1", Message: core.ChatMessage{Role: core.RoleUser, Content: "recent"}},
	}
	e := ByteEstimator{}
	total := int64(0)
	for _, item := range items {
		c, _ := e.Estimate(item.Message)
		total += c.Bytes
	}
	got, err := Assemble(context.Background(), Request{Budget: Budget{TotalBytes: total}, Estimator: e, Items: items})
	if err != nil || len(got.Messages) != 3 {
		t.Fatalf("result=%#v err=%v", got, err)
	}
	if got.Messages[0].Content != "tool" || got.Messages[1].Content != "summary" || got.Messages[2].Content != "recent" {
		t.Fatalf("protocol order changed: %#v", got.Messages)
	}
	if got.Evidence.Used.Bytes != total || len(got.Evidence.SelectedItems) != 3 {
		t.Fatalf("evidence=%#v", got.Evidence)
	}
}

func TestGroupCrossLayerCapAndMutationIsolation(t *testing.T) {
	args := map[string]any{"nested": map[string]any{"values": []any{"before"}}}
	items := []Item{
		{Layer: LayerRecent, SourceID: "call", Revision: "1", GroupID: "pair", Message: core.ChatMessage{Role: core.RoleAssistant, Content: "call", ToolCall: &core.ToolCall{ID: "id", Name: "tool", Args: args}}},
		{Layer: LayerToolResults, SourceID: "result", Revision: "1", GroupID: "pair", Message: core.ChatMessage{Role: core.RoleTool, Content: strings.Repeat("r", 20), ToolCallID: "id"}},
	}
	got, err := Assemble(context.Background(), Request{Budget: Budget{TotalBytes: MaxBytes, LayerBytes: map[Layer]int64{LayerToolResults: 1}}, Estimator: ByteEstimator{}, Items: items})
	if err != nil || got.Evidence.Selected != 0 || got.Evidence.Dropped != 2 {
		t.Fatalf("cross-layer group bypassed cap: %#v %v", got, err)
	}
	got, err = Assemble(context.Background(), Request{Budget: Budget{TotalBytes: MaxBytes}, Estimator: ByteEstimator{}, Items: items})
	if err != nil || len(got.Messages) != 2 {
		t.Fatalf("group result=%#v err=%v", got, err)
	}
	got.Messages[0].ToolCall.Args["nested"].(map[string]any)["values"].([]any)[0] = "mutated"
	if args["nested"].(map[string]any)["values"].([]any)[0] != "before" {
		t.Fatal("output mutation leaked into input")
	}
}

func TestPromptUTF8AndNilContext(t *testing.T) {
	var nilContext context.Context
	if _, err := Assemble(nilContext, Request{Budget: Budget{TotalBytes: 100}, Estimator: ByteEstimator{}, RequiredSystem: "line 1\nline 2"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil ctx err=%v", err)
	}
	if _, err := Assemble(context.Background(), Request{Budget: Budget{TotalBytes: 100}, Estimator: ByteEstimator{}, RequiredSystem: "line 1\nline 2"}); err != nil {
		t.Fatalf("multiline prompt rejected: %v", err)
	}
	if _, err := Assemble(context.Background(), Request{Budget: Budget{TotalBytes: 100}, Estimator: ByteEstimator{}, RequiredSystem: string([]byte{0xff})}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid utf8 err=%v", err)
	}
}

type panicEstimator struct{}

func (panicEstimator) Estimate(core.ChatMessage) (Cost, error)      { panic("secret-token") }
func (panicEstimator) EstimateFragment(Layer, string) (Cost, error) { panic("secret-token") }

func TestEstimatorPanicAndRoleContractsFailClosed(t *testing.T) {
	r := Request{Budget: Budget{TotalBytes: 100}, Estimator: panicEstimator{}, Items: []Item{{Layer: LayerRecent, SourceID: "x", Revision: "1", Message: core.ChatMessage{Role: core.RoleUser, Content: "x"}}}}
	func() {
		defer func() {
			if v := recover(); v != nil {
				t.Fatalf("panic leaked: %v", v)
			}
		}()
		if _, err := Assemble(context.Background(), r); !errors.Is(err, ErrInvalid) {
			t.Fatalf("panic error=%v", err)
		}
	}()
	bad := []core.ChatMessage{{Role: core.RoleUser, ToolCallID: "x"}, {Role: core.RoleAssistant, ToolCallID: "x"}, {Role: core.RoleTool}, {Role: core.RoleUser, ToolCalls: []core.ToolCall{{ID: "x", Name: "n"}}}}
	for _, m := range bad {
		rr := Request{Budget: Budget{TotalBytes: 100}, Estimator: ByteEstimator{}, Items: []Item{{Layer: LayerRecent, SourceID: "x", Revision: "1", Message: m}}}
		if _, err := Assemble(context.Background(), rr); !errors.Is(err, ErrInvalid) {
			t.Fatalf("message=%#v err=%v", m, err)
		}
	}
	for _, item := range []Item{
		{Layer: LayerRecent, SourceID: " x", Revision: "1", Message: core.ChatMessage{Role: core.RoleUser, Content: "ok"}},
		{Layer: LayerRecent, SourceID: "x", Revision: "1", GroupID: "g\u200b", Message: core.ChatMessage{Role: core.RoleUser, Content: "ok"}},
		{Layer: LayerRecent, SourceID: "x", Revision: "1", Message: core.ChatMessage{Role: core.RoleAssistant, ToolCall: &core.ToolCall{ID: " id", Name: "tool"}}},
		{Layer: LayerRecent, SourceID: "x", Revision: "1", Message: core.ChatMessage{Role: core.RoleAssistant, ToolCall: &core.ToolCall{ID: "id", Name: " tool"}}},
	} {
		rr := Request{Budget: Budget{TotalBytes: 100}, Estimator: ByteEstimator{}, Items: []Item{item}}
		if _, err := Assemble(context.Background(), rr); !errors.Is(err, ErrInvalid) {
			t.Fatalf("identifier item=%#v err=%v", item, err)
		}
	}
}

type lowEstimator struct{}

func (lowEstimator) Estimate(core.ChatMessage) (Cost, error)      { return Cost{Bytes: 1}, nil }
func (lowEstimator) EstimateFragment(Layer, string) (Cost, error) { return Cost{Bytes: 1}, nil }

type tokenFunc func(core.ChatMessage) (int64, error)

func (f tokenFunc) EstimateTokens(m core.ChatMessage) (int64, error)        { return f(m) }
func (f tokenFunc) EstimateFragmentTokens(_ Layer, _ string) (int64, error) { return 1, nil }
func (f tokenFunc) EstimateFragment(_ Layer, text string) (Cost, error) {
	return Cost{Bytes: int64(len(text)), Tokens: 0}, nil
}

func BenchmarkAssemblePayloadSizes(b *testing.B) {
	for _, size := range []int{16 << 10, 1 << 20, 15 << 20} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			item := Item{Layer: LayerRecent, SourceID: "bench", Revision: "1", Message: core.ChatMessage{Role: core.RoleUser, Content: strings.Repeat("x", size)}}
			req := Request{Budget: Budget{TotalBytes: MaxBytes}, Estimator: ByteEstimator{}, Items: []Item{item}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Assemble(context.Background(), req); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
