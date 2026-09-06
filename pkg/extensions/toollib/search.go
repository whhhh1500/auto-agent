package toollib

import (
	"sort"
	"strings"
	"unicode"
)

type Hit struct {
	ID          string
	Library     string
	Name        string
	Description string
	Score       int
	Ambiguous   bool
	SameName    []string
}

func (c *Catalog) Search(query string, topK int) []Hit {
	if c == nil {
		return nil
	}
	if topK <= 0 {
		topK = DefaultTopK
	}
	if topK > MaxSearchTopK {
		topK = MaxSearchTopK
	}
	c.mu.RLock()
	searcher := c.searcher
	index := c.index
	var docs []Record
	if searcher != nil {
		docs = make([]Record, 0, len(c.byID))
		for _, rec := range c.byID {
			docs = append(docs, rec)
		}
		sort.Slice(docs, func(i, j int) bool { return docs[i].ID < docs[j].ID })
	}
	c.mu.RUnlock()
	if searcher != nil {
		return searcher.Search(docs, query, topK)
	}
	if index != nil {
		return index.search(query, topK)
	}
	return SearchRecords(docs, query, topK)
}

type indexedRecord struct {
	record     Record
	nameTokens map[string]bool
	descTokens map[string]bool
	notTokens  map[string]bool
	allTokens  map[string]bool
}

// searchIndex is immutable after build. Catalog swaps the pointer only after
// Apply/Reset has produced a complete new index, so searches never observe a
// partially updated token or sibling-name map.
type searchIndex struct {
	records []indexedRecord
	byName  map[string][]int
}

func buildSearchIndex(byID map[string]Record) *searchIndex {
	if len(byID) == 0 {
		return nil
	}
	records := make([]Record, 0, len(byID))
	for _, rec := range byID {
		records = append(records, rec)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	index := &searchIndex{records: make([]indexedRecord, len(records)), byName: make(map[string][]int, len(records))}
	for i, rec := range records {
		index.records[i] = indexedRecord{
			record:     rec,
			nameTokens: tokens(rec.Name),
			descTokens: tokens(rec.Description + " " + strings.Join(rec.Triggers, " ")),
			notTokens:  tokens(strings.Join(rec.NotFor, " ")),
		}
		index.records[i].allTokens = mergeTokens(index.records[i].nameTokens, index.records[i].descTokens)
		index.byName[rec.Name] = append(index.byName[rec.Name], i)
	}
	for _, siblings := range index.byName {
		if len(siblings) < 2 {
			continue
		}
		for _, i := range siblings {
			mine := index.records[i].allTokens
			for _, j := range siblings {
				if i == j {
					continue
				}
				for token := range tokens(index.records[j].record.Description) {
					if mine[token] || stopwords[token] {
						continue
					}
					index.records[i].notTokens[token] = true
				}
			}
		}
	}
	return index
}

func (index *searchIndex) search(query string, topK int) []Hit {
	queryTokens := queryTokens(query)
	if len(queryTokens) == 0 {
		return nil
	}
	hits := []Hit{}
	for _, indexed := range index.records {
		doc := indexed.record
		nameScore := overlap(queryTokens, indexed.nameTokens)
		descScore := overlap(queryTokens, indexed.descTokens)
		covered := overlap(queryTokens, indexed.allTokens)
		notScore := overlap(queryTokens, indexed.notTokens)
		if notScore > 0 && notScore >= descScore && notScore >= nameScore {
			continue
		}
		if covered <= 0 {
			continue
		}
		if len(queryTokens) >= 2 && descScore == 0 && covered < len(queryTokens) {
			continue
		}
		if len(queryTokens) >= 2 && covered*2 < len(queryTokens) {
			continue
		}
		same := []string{}
		for _, otherIndex := range index.byName[doc.Name] {
			other := index.records[otherIndex].record.ID
			if other != doc.ID {
				same = append(same, other)
			}
		}
		hits = append(hits, Hit{ID: doc.ID, Library: doc.Library, Name: doc.Name,
			Description: summary(doc.Description), Score: descScore*3 + nameScore,
			Ambiguous: len(same) > 0, SameName: same})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].ID < hits[j].ID
	})
	if topK > 0 && len(hits) > topK {
		hits = hits[:topK]
	}
	return hits
}

func SearchRecords(docs []Record, query string, topK int) []Hit {
	queryTokens := queryTokens(query)
	if len(queryTokens) == 0 {
		return nil
	}
	byName := map[string][]string{}
	byNameDocs := map[string][]Record{}
	for _, doc := range docs {
		byName[doc.Name] = append(byName[doc.Name], doc.ID)
		byNameDocs[doc.Name] = append(byNameDocs[doc.Name], doc)
	}
	hits := []Hit{}
	for _, doc := range docs {
		nameTokens := tokens(doc.Name)
		descTokens := tokens(doc.Description + " " + strings.Join(doc.Triggers, " "))
		notTokens := tokens(strings.Join(doc.NotFor, " "))
		if siblings := byNameDocs[doc.Name]; len(siblings) > 1 {
			mine := mergeTokens(nameTokens, descTokens)
			for _, other := range siblings {
				if other.ID == doc.ID {
					continue
				}
				for token := range tokens(other.Description) {
					if mine[token] || stopwords[token] {
						continue
					}
					notTokens[token] = true
				}
			}
		}
		docTokens := mergeTokens(nameTokens, descTokens)
		nameScore := overlap(queryTokens, nameTokens)
		descScore := overlap(queryTokens, descTokens)
		covered := overlap(queryTokens, docTokens)
		notScore := overlap(queryTokens, notTokens)
		if notScore > 0 && notScore >= descScore && notScore >= nameScore {
			continue
		}
		if covered <= 0 {
			continue
		}
		if len(queryTokens) >= 2 && descScore == 0 && covered < len(queryTokens) {
			continue
		}
		if len(queryTokens) >= 2 && covered*2 < len(queryTokens) {
			continue
		}
		same := []string{}
		for _, other := range byName[doc.Name] {
			if other != doc.ID {
				same = append(same, other)
			}
		}
		hits = append(hits, Hit{
			ID: doc.ID, Library: doc.Library, Name: doc.Name,
			Description: summary(doc.Description), Score: descScore*3 + nameScore,
			Ambiguous: len(same) > 0, SameName: same,
		})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].ID < hits[j].ID
	})
	if topK > 0 && len(hits) > topK {
		hits = hits[:topK]
	}
	return hits
}

func queryTokens(text string) map[string]bool {
	toks := tokens(text)
	filtered := map[string]bool{}
	for token := range toks {
		if stopwords[token] {
			continue
		}
		filtered[token] = true
	}
	if len(filtered) == 0 {
		return toks
	}
	return filtered
}

func tokens(text string) map[string]bool {
	out := map[string]bool{}
	normalized := strings.NewReplacer("-", " ", "_", " ", "/", " ", ".", " ").Replace(strings.ToLower(text))
	for _, field := range strings.Fields(normalized) {
		out[strings.Trim(field, ".,;:!?")] = true
	}
	delete(out, "")
	return out
}

func mergeTokens(parts ...map[string]bool) map[string]bool {
	out := map[string]bool{}
	for _, part := range parts {
		for token := range part {
			out[token] = true
		}
	}
	return out
}

func overlap(query, doc map[string]bool) int {
	score := 0
	for token := range query {
		if doc[token] {
			score++
		}
	}
	return score
}

func summary(text string) string {
	var out strings.Builder
	if len(text) < MaxSummaryRunes {
		out.Grow(len(text))
	}
	runes := 0
	for _, r := range text {
		if runes >= MaxSummaryRunes {
			break
		}
		if r == '\n' || r == '\t' {
			r = ' '
		} else if unicode.IsControl(r) {
			continue
		}
		out.WriteRune(r)
		runes++
	}
	return out.String()
}

var stopwords = map[string]bool{
	"a": true, "an": true, "the": true, "to": true, "for": true, "of": true,
	"and": true, "or": true, "in": true, "on": true, "with": true, "into": true,
}
