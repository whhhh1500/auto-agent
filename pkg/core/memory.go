package core

import "time"

// MemoryEntry remains in core as a shared persistence wire type used by the
// storage package. Memory behavior and store contracts live in
// pkg/extensions/memory.
type MemoryEntry struct {
	ID        string    `json:"id"`
	Key       string    `json:"key"`
	Content   string    `json:"content"`
	Tags      []string  `json:"tags,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}
