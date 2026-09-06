package toollib

import "sort"

// HybridSearcher reranks keyword hits with character 3-gram overlap.
// It never reintroduces records that SearchRecords already dropped (NotFor,
// coverage). Embedding models can still replace it via SetSearcher.
type HybridSearcher struct{}

func (HybridSearcher) Search(docs []Record, query string, topK int) []Hit {
	if topK <= 0 {
		topK = DefaultTopK
	}
	if topK > MaxSearchTopK {
		topK = MaxSearchTopK
	}
	candidateK := topK * 3
	if candidateK < 16 {
		candidateK = 16
	}
	if candidateK > MaxSearchTopK {
		candidateK = MaxSearchTopK
	}
	hits := SearchRecords(docs, query, candidateK)
	queryGrams := charNgrams(query, 3)
	for i := range hits {
		text := hits[i].Name + " " + hits[i].Description
		hits[i].Score = hits[i].Score*10 + ngramOverlap(queryGrams, charNgrams(text, 3))
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].ID < hits[j].ID
	})
	if len(hits) > topK {
		hits = hits[:topK]
	}
	return hits
}

func charNgrams(text string, n int) map[string]int {
	runes := []rune(normalizeNgram(text))
	out := map[string]int{}
	if n <= 0 || len(runes) < n {
		if len(runes) > 0 {
			out[string(runes)]++
		}
		return out
	}
	for i := 0; i+n <= len(runes); i++ {
		out[string(runes[i:i+n])]++
	}
	return out
}

func ngramOverlap(query, doc map[string]int) int {
	score := 0
	for gram, qn := range query {
		if dn := doc[gram]; dn > 0 {
			if qn < dn {
				score += qn
			} else {
				score += dn
			}
		}
	}
	return score
}

func normalizeNgram(text string) string {
	out := make([]rune, 0, len(text))
	lastSpace := true
	for _, r := range text {
		switch {
		case r >= 'A' && r <= 'Z':
			out = append(out, r-'A'+'a')
			lastSpace = false
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			out = append(out, r)
			lastSpace = false
		default:
			if !lastSpace {
				out = append(out, ' ')
				lastSpace = true
			}
		}
	}
	return string(out)
}
