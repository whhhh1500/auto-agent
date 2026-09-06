// Package logging provides the server's background logging with size-based
// rotation: JSON records go to stdout (container friendly) and to a rotating
// file on disk. Rotation renames the active file to a timestamped backup,
// starts a fresh one, and prunes the oldest backups beyond the configured
// count — no external dependency, no cron, no signal handling.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// Options configures the rotating file log.
type Options struct {
	// Dir holds the log files; created if missing. Default ./data/logs.
	Dir string
	// Name is the base file name. Default harness.log; backups become
	// harness-<timestamp>.log.
	Name string
	// MaxBytes is the active-file size that triggers rotation. Default 64 MiB.
	MaxBytes int64
	// MaxBackups is how many rotated files are kept. Default 14.
	MaxBackups int
}

// RotatingWriter is an io.Writer that rotates its file at MaxBytes.
type RotatingWriter struct {
	opts Options

	mu   sync.Mutex
	file *os.File
	size int64
}

// NewRotatingWriter opens (and creates) the active log file.
func NewRotatingWriter(opts Options) (*RotatingWriter, error) {
	if opts.Dir == "" {
		opts.Dir = filepath.Join("data", "logs")
	}
	if opts.Name == "" {
		opts.Name = "harness.log"
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = 64 << 20
	}
	if opts.MaxBackups <= 0 {
		opts.MaxBackups = 14
	}
	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(opts.Dir, 0o700); err != nil {
			return nil, fmt.Errorf("secure log dir: %w", err)
		}
	}
	w := &RotatingWriter{opts: opts}
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *RotatingWriter) activePath() string {
	return filepath.Join(w.opts.Dir, w.opts.Name)
}

func (w *RotatingWriter) open() error {
	file, err := os.OpenFile(w.activePath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	if runtime.GOOS != "windows" {
		if err := file.Chmod(0o600); err != nil {
			_ = file.Close()
			return fmt.Errorf("secure log file: %w", err)
		}
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}
	w.file = file
	w.size = info.Size()
	return nil
}

// Write appends the bytes, rotating first when the size budget would overflow.
func (w *RotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if int64(len(p))+w.size > w.opts.MaxBytes {
		if err := w.rotateLocked(); err != nil {
			// Rotation failure must not lose the record: fall through and
			// keep writing to the still-open file.
			fmt.Fprintf(os.Stderr, "log rotation failed: %v\n", err)
		}
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, err
}

// Rotate forces a rotation (useful for day-based policies driven externally).
func (w *RotatingWriter) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateLocked()
}

func (w *RotatingWriter) rotateLocked() error {
	if w.file != nil {
		w.file.Close()
	}
	backup := filepath.Join(w.opts.Dir, fmt.Sprintf("%s-%s.log",
		strings.TrimSuffix(w.opts.Name, filepath.Ext(w.opts.Name)),
		time.Now().UTC().Format("20060102-150405.000")))
	if err := os.Rename(w.activePath(), backup); err != nil && !os.IsNotExist(err) {
		// Reopen the active file before returning so logging continues.
		w.open()
		return fmt.Errorf("rotate log: %w", err)
	}
	if err := w.open(); err != nil {
		return err
	}
	w.pruneLocked()
	return nil
}

// pruneLocked deletes the oldest backups beyond MaxBackups.
func (w *RotatingWriter) pruneLocked() {
	entries, err := os.ReadDir(w.opts.Dir)
	if err != nil {
		return
	}
	prefix := strings.TrimSuffix(w.opts.Name, filepath.Ext(w.opts.Name)) + "-"
	var backups []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".log") {
			continue
		}
		backups = append(backups, name)
	}
	// Names embed UTC timestamps, so lexical order is chronological.
	sort.Strings(backups)
	for len(backups) > w.opts.MaxBackups {
		oldest := backups[0]
		backups = backups[1:]
		if err := os.Remove(filepath.Join(w.opts.Dir, oldest)); err != nil {
			return
		}
	}
}

// Close flushes and closes the active file.
func (w *RotatingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Sync()
	if closeErr := w.file.Close(); err == nil {
		err = closeErr
	}
	w.file = nil
	return err
}

// New builds the server logger: JSON records to stdout and to the rotating
// file. It never panics and degrades to a discard logger when the disk is
// unusable, because logging must not take the service down.
func New(opts Options) (*slog.Logger, io.Closer, error) {
	writer, err := NewRotatingWriter(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "file logging disabled: %v\n", err)
		return slog.New(discardHandler{}), nil, nil
	}
	logger := slog.New(newTraceHandler(slog.NewJSONHandler(io.MultiWriter(os.Stdout, writer), &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
	return logger, writer, nil
}

// traceHandler is a logging adapter that adds the valid OTel SpanContext from
// a log call's context to the JSON record. It deliberately keeps OTel out of
// the core packages while making correlation available at the logging edge.
//
// User-supplied trace_id, span_id, and trace_flags attributes always win. A
// key supplied in a bound attribute or anywhere in a record's attribute tree
// suppresses generation of that one automatic field. This avoids overwriting
// or emitting duplicate correlation keys, including under logger groups.
type traceHandler struct {
	next     slog.Handler
	groups   []string
	bound    []boundAttrs
	explicit traceFields
}

type boundAttrs struct {
	groups []string
	attrs  []slog.Attr
}

type traceFields struct {
	traceID    bool
	spanID     bool
	traceFlags bool
}

func newTraceHandler(next slog.Handler) slog.Handler {
	return &traceHandler{next: next}
}

func (h *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *traceHandler) Handle(ctx context.Context, record slog.Record) error {
	explicit := h.explicit
	record.Attrs(func(attr slog.Attr) bool {
		explicit.addAttr(attr)
		return true
	})

	attrs := h.traceAttrs(ctx, explicit)
	return h.next.Handle(ctx, h.withGroups(record, attrs))
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	explicit := h.explicit
	for _, attr := range attrs {
		explicit.addAttr(attr)
	}

	bound := make([]boundAttrs, len(h.bound)+1)
	copy(bound, h.bound)
	bound[len(h.bound)] = boundAttrs{
		groups: cloneStrings(h.groups),
		attrs:  append([]slog.Attr(nil), attrs...),
	}

	return &traceHandler{
		next:     h.next,
		groups:   cloneStrings(h.groups),
		bound:    bound,
		explicit: explicit,
	}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	explicit := h.explicit
	// A user group is emitted as a JSON key too. Treat a trace-field group
	// name as explicit so automatic root fields can never duplicate it.
	explicit.addKey(name)

	groups := make([]string, len(h.groups)+1)
	copy(groups, h.groups)
	groups[len(h.groups)] = name
	return &traceHandler{
		next:     h.next,
		groups:   groups,
		bound:    h.bound,
		explicit: explicit,
	}
}

func (h *traceHandler) traceAttrs(ctx context.Context, explicit traceFields) []slog.Attr {
	if ctx == nil {
		return nil
	}
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return nil
	}

	attrs := make([]slog.Attr, 0, 3)
	if !explicit.traceID {
		attrs = append(attrs, slog.String("trace_id", spanContext.TraceID().String()))
	}
	if !explicit.spanID {
		attrs = append(attrs, slog.String("span_id", spanContext.SpanID().String()))
	}
	if !explicit.traceFlags {
		// TraceFlags is an eight-bit W3C trace-context field. Formatting the
		// byte explicitly keeps its JSON form fixed-width and lowercase: "01"
		// for sampled spans and "00" for unsampled spans.
		attrs = append(attrs, slog.String("trace_flags", fmt.Sprintf("%02x", byte(spanContext.TraceFlags()))))
	}
	return attrs
}

func (h *traceHandler) withGroups(record slog.Record, traceAttrs []slog.Attr) slog.Record {
	root := groupNode{}
	for _, bound := range h.bound {
		root.add(bound.groups, bound.attrs)
	}

	recordAttrs := make([]slog.Attr, 0, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		recordAttrs = append(recordAttrs, attr)
		return true
	})
	root.add(h.groups, recordAttrs)

	result := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	result.AddAttrs(root.attrs()...)
	result.AddAttrs(traceAttrs...)
	return result
}

// groupNode preserves the order in which attributes and groups are bound to a
// logger while coalescing common group paths. This matches slog's WithGroup
// behavior without allowing generated trace fields to inherit a user group.
type groupNode struct {
	items  []groupItem
	groups map[string]*groupNode
}

type groupItem struct {
	attr  slog.Attr
	name  string
	group *groupNode
}

func (node *groupNode) add(groups []string, attrs []slog.Attr) {
	if len(attrs) == 0 {
		return
	}
	for _, name := range groups {
		node = node.child(name)
	}
	for _, attr := range attrs {
		node.items = append(node.items, groupItem{attr: attr})
	}
}

func (node *groupNode) child(name string) *groupNode {
	if node.groups == nil {
		node.groups = make(map[string]*groupNode)
	}
	if child := node.groups[name]; child != nil {
		return child
	}
	child := &groupNode{}
	node.groups[name] = child
	node.items = append(node.items, groupItem{name: name, group: child})
	return child
}

func (node *groupNode) attrs() []slog.Attr {
	attrs := make([]slog.Attr, 0, len(node.items))
	for _, item := range node.items {
		if item.group == nil {
			attrs = append(attrs, item.attr)
			continue
		}
		attrs = append(attrs, slog.Attr{
			Key:   item.name,
			Value: slog.GroupValue(item.group.attrs()...),
		})
	}
	return attrs
}

func (fields *traceFields) addAttr(attr slog.Attr) {
	fields.addKey(attr.Key)
	if attr.Value.Kind() != slog.KindGroup {
		return
	}
	for _, groupAttr := range attr.Value.Group() {
		fields.addAttr(groupAttr)
	}
}

func (fields *traceFields) addKey(key string) {
	switch key {
	case "trace_id":
		fields.traceID = true
	case "span_id":
		fields.spanID = true
	case "trace_flags":
		fields.traceFlags = true
	}
}

func cloneStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (d discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return d }
func (d discardHandler) WithGroup(string) slog.Handler           { return d }
