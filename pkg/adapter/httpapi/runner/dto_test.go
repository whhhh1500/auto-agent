package runner

import (
	"strings"
	"testing"
	"time"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	runtime "github.com/whhhh1500/auto-agent/pkg/extensions/runner"
)

func TestClaimRequestCanonicalizesAndMapsCommand(t *testing.T) {
	request := ClaimRequest{Capabilities: []string{" runner.z ", "runner.a", "runner.z"}}
	command, err := request.ToCommand("worker-1", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"runner.a", "runner.z"}
	if len(command.Capabilities) != len(want) || command.Capabilities[0] != want[0] || command.Capabilities[1] != want[1] {
		t.Fatalf("capabilities=%v want=%v", command.Capabilities, want)
	}
	if command.WorkerID != "worker-1" || command.LeaseTTL != 15*time.Second {
		t.Fatalf("command=%+v", command)
	}
}

func TestClaimRequestPreservesSelectorLimits(t *testing.T) {
	for _, test := range []struct {
		name   string
		values []string
	}{
		{name: "wildcard", values: []string{"*"}},
		{name: "not namespaced", values: []string{"runner"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := (ClaimRequest{Capabilities: test.values}).Validate(); err == nil {
				t.Fatal("invalid selector was accepted")
			}
		})
	}
	overLimit := make([]string, runtime.MaxClaimCapabilities+1)
	for index := range overLimit {
		overLimit[index] = "runner.render"
	}
	if err := (ClaimRequest{Capabilities: overLimit}).Validate(); err == nil {
		t.Fatal("over-limit selector was accepted")
	}
}

func TestGenerationAndCompletionValidation(t *testing.T) {
	if err := (GenerationRequest{}).Validate(); err == nil {
		t.Fatal("zero generation was accepted")
	}
	request := CompletionRequest{Generation: 2, Content: "done", OK: true, Metadata: map[string]any{"source": "worker"}}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	result := request.ToResult()
	if result.Content != "done" || !result.OK || result.Metadata["source"] != "worker" {
		t.Fatalf("result=%+v", result)
	}
	if err := (CompletionRequest{Generation: 1, Content: strings.Repeat("x", core.HardMaxCapabilityOutputBytes+1)}).Validate(); err == nil {
		t.Fatal("oversized completion was accepted")
	}
}
