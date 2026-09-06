// Package modelruntime is the executable composition seam between durable
// model settings and the protocol-neutral model execution contract. It builds
// one immutable, exact modelcontrol plan per normalized settings revision and
// exposes that plan through the legacy core.ModelResolver interface.
//
// It owns neither settings persistence nor HTTP wire protocols. Providers and
// protocols are registered independently in a bounded PluginRegistry, so an
// integration adds a new vendor by supplying exact builders instead of editing
// a central switch. Registry instances are explicitly constructed; there is no
// global registration, refresh goroutine, or implicit fallback.
package modelruntime
