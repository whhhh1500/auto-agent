// Package modelexecution defines the protocol-neutral M2-B execution seam.
//
// It owns only bounded request/event values, opaque credential resolution, and
// an exact immutable provider/protocol binding registry. Providers own endpoint,
// authentication, and transport; protocols own wire paths, bodies, and parsers.
// Neither contract imports HTTP, a vendor SDK, core, storage, or server code.
// A core bridge is deliberately an adapter, not part of this package.
package modelexecution
