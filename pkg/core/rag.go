package core

// RagDocument, RagQuery, and RagChunk remain in core as shared persistence
// wire types used by the storage package. Retrieval behavior and index
// contracts live in pkg/extensions/rag.
type RagDocument struct {
	ID      string   `json:"id"`
	Source  string   `json:"source,omitempty"`
	Content string   `json:"content"`
	Tags    []string `json:"tags,omitempty"`
}

type RagQuery struct {
	Query string   `json:"query"`
	TopK  int      `json:"top_k,omitempty"`
	Tags  []string `json:"tags,omitempty"`
}

type RagChunk struct {
	ID      string  `json:"id"`
	Source  string  `json:"source,omitempty"`
	Content string  `json:"content"`
	Score   float64 `json:"score"`
}
