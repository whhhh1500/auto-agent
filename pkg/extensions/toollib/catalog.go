package toollib

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"sync"
)

const (
	MaxSearchTopK   = 32
	DefaultTopK     = 8
	MaxSummaryRunes = 240
)

// Record is one tool as a catalog document. It is derived from a capability
// manifest or MCP tools/list entry, never mixed with user RAG corpora.
type Record struct {
	ID          string
	Library     string
	Name        string
	Description string
	Schema      map[string]any
	RemoteName  string
	Revision    string
	Triggers    []string
	NotFor      []string
}

// ApplyStats reports a catalog reconciliation against a full listing.
type ApplyStats struct {
	Added     int
	Updated   int
	Unchanged int
	Removed   int
}

// Catalog is an in-process tool knowledge base. Apply replaces by id, deletes
// ids missing from the listing, and skips records whose revision is unchanged.
// Searcher ranks catalog records for a query. Keyword SearchRecords is the
// default; a vector or hybrid implementation can be installed with SetSearcher.
type Searcher interface {
	Search(docs []Record, query string, topK int) []Hit
}

type Catalog struct {
	mu       sync.RWMutex
	byID     map[string]Record
	index    *searchIndex
	searcher Searcher
}

func NewCatalog() *Catalog {
	return &Catalog{byID: map[string]Record{}}
}

func (c *Catalog) SetSearcher(searcher Searcher) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.searcher = searcher
	c.mu.Unlock()
}

func ContentRevision(name, description string, schema map[string]any, triggers, notFor []string) string {
	payload, _ := json.Marshal(struct {
		Name        string         `json:"n"`
		Description string         `json:"d"`
		Schema      map[string]any `json:"s"`
		Triggers    []string       `json:"t"`
		NotFor      []string       `json:"x"`
	}{Name: name, Description: description, Schema: schema, Triggers: triggers, NotFor: notFor})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func QualifyID(library, name string) string {
	name = strings.TrimSpace(name)
	if library == "" {
		return name
	}
	return library + "/" + name
}

// Apply reconciles the catalog with a complete listing from the source of
// truth. An empty listing is refused so a failed remote list cannot wipe the
// last good catalog.
func (c *Catalog) Apply(listed []Record) (ApplyStats, error) {
	if c == nil {
		return ApplyStats{}, errNilCatalog
	}
	incoming := map[string]Record{}
	for _, rec := range listed {
		rec = normalizeRecord(rec)
		if rec.ID == "" {
			continue
		}
		incoming[rec.ID] = rec
	}
	if len(incoming) == 0 {
		return ApplyStats{}, errEmptyListing
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byID == nil {
		c.byID = map[string]Record{}
	}
	stats := ApplyStats{}
	for id, rec := range incoming {
		existing, ok := c.byID[id]
		if !ok {
			c.byID[id] = rec
			stats.Added++
			continue
		}
		if existing.Revision == rec.Revision && rec.Revision != "" {
			stats.Unchanged++
			continue
		}
		c.byID[id] = rec
		stats.Updated++
	}
	for id := range c.byID {
		if _, ok := incoming[id]; !ok {
			delete(c.byID, id)
			stats.Removed++
		}
	}
	c.index = buildSearchIndex(c.byID)
	return stats, nil
}

func (c *Catalog) Reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.byID = map[string]Record{}
	c.index = nil
	c.mu.Unlock()
}

func (c *Catalog) Get(id string) (Record, bool) {
	if c == nil {
		return Record{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	rec, ok := c.byID[id]
	return rec, ok
}

func (c *Catalog) ByName(name string) []Record {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := []Record{}
	for _, rec := range c.byID {
		if rec.Name == name {
			out = append(out, rec)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (c *Catalog) Snapshot() []Record {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Record, 0, len(c.byID))
	for _, rec := range c.byID {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (c *Catalog) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.byID)
}

var errNilCatalog = catalogError("tool catalog is nil")
var errEmptyListing = catalogError("tool listing is empty; keeping previous catalog")

type catalogError string

func (e catalogError) Error() string { return string(e) }

func normalizeRecord(rec Record) Record {
	rec.Name = strings.TrimSpace(rec.Name)
	rec.Library = strings.TrimSpace(rec.Library)
	if rec.RemoteName == "" {
		rec.RemoteName = rec.Name
	}
	if rec.ID == "" {
		rec.ID = QualifyID(rec.Library, rec.Name)
	}
	rec.Triggers = append([]string(nil), rec.Triggers...)
	rec.NotFor = append([]string(nil), rec.NotFor...)
	if rec.Revision == "" {
		rec.Revision = ContentRevision(rec.Name, rec.Description, rec.Schema, rec.Triggers, rec.NotFor)
	}
	return rec
}
