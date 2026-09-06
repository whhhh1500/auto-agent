// Package graph defines the bounded, stdlib-only contract for a graph-shaped
// agent workflow. It deliberately has no executor, checkpoint implementation,
// service locator, transport, telemetry, or sandbox implementation. G1 adds
// only bounded checkpoint/transition value contracts and a Store port here;
// execution and adapters remain separate. If several conditional edges are
// true, G1 must fail closed; G0 intentionally supplies no map/order-based tie
// breaker.
package graph
