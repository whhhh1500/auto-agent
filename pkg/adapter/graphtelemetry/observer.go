package graphtelemetry

import (
	"context"

	core "github.com/whhhh1500/auto-agent/pkg/core"
	execgraph "github.com/whhhh1500/auto-agent/pkg/execution/graph"
)

// Observer bridges bounded Graph events into core telemetry. Identity fields
// and payloads are intentionally ignored to keep metric cardinality bounded.
type Observer struct{ Telemetry core.Telemetry }

func (o Observer) Observe(ctx context.Context, event execgraph.Event) (err error) {
	defer func() {
		if recover() != nil {
			err = nil
		}
	}()
	if o.Telemetry == nil {
		return nil
	}
	attrs := core.TelemetryAttributes{"graph.event": boundedEvent(string(event.Kind)), "graph.node_kind": boundedNode(event.NodeKind), "graph.outcome": string(execgraph.NormalizeOutcomeCode(event.OutcomeCode))}
	core.AddTelemetryCounter(o.Telemetry, ctx, "harness.graph.events", 1, attrs)
	if event.Duration > 0 {
		core.RecordTelemetryHistogram(o.Telemetry, ctx, "harness.graph.node.duration", event.Duration.Seconds(), "s", attrs)
	}
	return nil
}

func boundedEvent(value string) string {
	switch execgraph.EventKind(value) {
	case execgraph.EventStarted, execgraph.EventNodeStart, execgraph.EventNodeEnd, execgraph.EventRetry, execgraph.EventSuspended, execgraph.EventCompleted, execgraph.EventFailed, execgraph.EventCancelled, execgraph.EventUnknown, execgraph.EventResumed, execgraph.EventApprovalResolved, execgraph.EventTimeout, execgraph.EventConflict, execgraph.EventEdgeAmbiguous, execgraph.EventNodeError:
		return value
	}
	return "unknown"
}
func boundedNode(value string) string {
	if value == "core-turn" {
		return value
	}
	return "unknown"
}

var _ execgraph.Observer = Observer{}
