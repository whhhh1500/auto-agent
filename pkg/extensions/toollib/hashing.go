package toollib

import (
	"hash/fnv"
	"math"
	"sort"
	"strings"
)

const hashingDim = 128

// HashingSearcher is an embedding-free vector ranker: token hashing trick
// into a fixed-width vector, cosine rerank of keyword candidates. It does
// not reintroduce NotFor/coverage drops from SearchRecords.
type HashingSearcher struct{}

func (HashingSearcher) Search(docs []Record, query string, topK int) []Hit {
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
	queryVec := hashingEmbed(query)
	byID := map[string]Record{}
	for _, doc := range docs {
		byID[doc.ID] = doc
	}
	for i := range hits {
		doc := byID[hits[i].ID]
		text := doc.Name + " " + doc.Description + " " + strings.Join(doc.Triggers, " ")
		cos := cosine(queryVec, hashingEmbed(text))
		hits[i].Score = hits[i].Score*100 + int(cos*1000)
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

func hashingEmbed(text string) []float64 {
	vec := make([]float64, hashingDim)
	for token := range queryTokens(text) {
		sum := fnv.New64a()
		_, _ = sum.Write([]byte(token))
		h := sum.Sum64()
		idx := int(h>>1) % hashingDim
		if h&1 == 1 {
			vec[idx] -= 1
		} else {
			vec[idx] += 1
		}
	}
	var norm float64
	for _, v := range vec {
		norm += v * v
	}
	norm = math.Sqrt(norm)
	if norm == 0 {
		return vec
	}
	for i := range vec {
		vec[i] /= norm
	}
	return vec
}

func cosine(a, b []float64) float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	var sum float64
	for i := 0; i < n; i++ {
		sum += a[i] * b[i]
	}
	return sum
}
