package index

import (
	"fmt"
	"sort"
	"sync"

	iq "github.com/rekki/go-query"
	analyzer "github.com/rekki/go-query-analyze"
)

// MemOnlyIndex is representation of an index stored in the memory
type MemOnlyIndex struct {
	perField map[string]*analyzer.Analyzer
	postings map[string]map[string][]int32
	forward  []Document

	// stored twice, but just for convinience
	forwardByID map[string]int32
	IDField     string
	sync.RWMutex
}

// NewMemOnlyIndex creates new in-memory index with the specified perField analyzer by default DefaultAnalyzer is used
func NewMemOnlyIndex(perField map[string]*analyzer.Analyzer) *MemOnlyIndex {
	if perField == nil {
		perField = map[string]*analyzer.Analyzer{}
	}
	m := &MemOnlyIndex{postings: map[string]map[string][]int32{}, perField: perField, forwardByID: map[string]int32{}, IDField: "_id"}
	return m
}

func (m *MemOnlyIndex) Get(id int32) Document {
	return m.forward[id]
}

func (m *MemOnlyIndex) GetByID(uuid string) Document {
	m.RLock()
	id, ok := m.forwardByID[uuid]
	m.RUnlock()

	if ok {
		return m.forward[id]
	}
	return nil
}

func (m *MemOnlyIndex) DeleteByID(uuid string) {
	m.Lock()
	defer m.Unlock()

	id, ok := m.forwardByID[uuid]
	if ok {
		m.deleteLocked(id)
	}
}

func (m *MemOnlyIndex) Delete(id int32) {
	m.Lock()
	defer m.Unlock()
	m.deleteLocked(id)
}

func (m *MemOnlyIndex) deleteLocked(id int32) {
	d := m.forward[id]

	fields := d.IndexableFields()

	for field, value := range fields {
		if field == m.IDField {
			for _, v := range value {
				delete(m.forwardByID, v)
			}
		}

		analyzer, ok := m.perField[field]
		if !ok {
			if field == m.IDField || field == "id" || field == "uuid" {
				analyzer = IDAnalyzer
			} else {
				analyzer = DefaultAnalyzer
			}
		}

		for _, v := range value {
			tokens := analyzer.AnalyzeIndex(v)
			for _, t := range tokens {
				m.deletePostings(field, t, id)
			}
		}
	}

	m.forward[id] = nil
}

// Index a bunch of documents
func (m *MemOnlyIndex) Index(docs ...Document) {
	m.Lock()
	defer m.Unlock()

	for _, d := range docs {
		fields := d.IndexableFields()
		did := len(m.forward)
		m.forward = append(m.forward, d)
		for field, value := range fields {
			if field == m.IDField {
				for _, v := range value {
					m.forwardByID[v] = int32(did)
				}
			}

			analyzer, ok := m.perField[field]
			if !ok {
				if field == m.IDField || field == "id" || field == "uuid" {
					analyzer = IDAnalyzer
				} else {
					analyzer = DefaultAnalyzer
				}
			}

			for _, v := range value {
				tokens := analyzer.AnalyzeIndex(v)
				for _, t := range tokens {
					m.addPostings(field, t, int32(did))
				}
			}
		}
	}
}

func (m *MemOnlyIndex) addPostings(k, v string, did int32) {
	pk, ok := m.postings[k]
	if !ok {
		pk = map[string][]int32{}
		m.postings[k] = pk
	}

	current, ok := pk[v]
	if !ok || len(current) == 0 {
		pk[v] = []int32{did}
	} else {
		if current[len(current)-1] != did {
			pk[v] = append(current, did)
		}
	}
}

func (m *MemOnlyIndex) deletePostings(k, v string, did int32) {
	pk, ok := m.postings[k]
	if !ok {
		return
	}

	current, ok := pk[v]
	if !ok || len(current) == 0 {
		return
	}

	// find the index where this documentID is and cut the slice
	found := sort.Search(len(current), func(i int) bool {
		return current[i] <= did
	})

	if found < len(current) && current[found] == did {
		pk[v] = append(current[:found], current[found+1:]...)
	}
}

// Terms generates array of queries from the tokenized term for this field, using the perField analyzer
func (m *MemOnlyIndex) Terms(field string, term string) []iq.Query {
	m.RLock()
	defer m.RUnlock()

	analyzer, ok := m.perField[field]
	if !ok {
		analyzer = DefaultAnalyzer
	}
	tokens := analyzer.AnalyzeSearch(term)
	queries := []iq.Query{}
	for _, t := range tokens {
		queries = append(queries, m.NewTermQuery(field, t))
	}
	return queries
}

func (m *MemOnlyIndex) NewTermQuery(field string, term string) iq.Query {
	m.RLock()
	defer m.RUnlock()

	s := fmt.Sprintf("%s:%s", field, term)
	pk, ok := m.postings[field]
	if !ok {
		return iq.Term(len(m.forward), s, []int32{})
	}
	pv, ok := pk[term]
	if !ok {
		return iq.Term(len(m.forward), s, []int32{})
	}
	// there are allocation in iq.Term(), so dont just defer unlock, otherwise it will be locked while term is created
	return iq.Term(len(m.forward), s, pv)
}

// Foreach matching document
// Example:
//  query := iq.And(
//  	iq.Or(m.Terms("name", "aMS u")...),
//  	iq.Or(m.Terms("country", "NL BG")...),
//  )
//  m.Foreach(query, func(did int32, score float32, doc index.Document) {
//  	city := doc.(*ExampleCity)
//  	log.Printf("%v matching with score %f", city, score)
//  })
func (m *MemOnlyIndex) Foreach(query iq.Query, cb func(int32, float32, Document)) {
	for query.Next() != iq.NO_MORE {
		did := query.GetDocId()
		score := query.Score()
		doc := m.forward[did]
		if doc == nil {
			// deleted
			continue
		}
		cb(did, score, doc)
	}
}

// TopN documents
// The following texample gets top5 results and also check add 100 to the score of cities that have NL in the score.
// usually the score of your search is some linear combination of f(a*text + b*popularity + c*context..)
//
// Example:
//  query := iq.And(
//  	iq.Or(m.Terms("name", "ams university")...),
//  	iq.Or(m.Terms("country", "NL BG")...),
//  )
//  top := m.TopN(5, q, func(did int32, score float32, doc Document) float32 {
//  	city := doc.(*ExampleCity)
//  	if city.Country == "NL" {
//  		score += 100
//  	}
//  	n++
//  	return score
//  })
// the SearchResult structure looks like
//  {
//    "total": 3,
//    "hits": [
//      {
//        "score": 101.09861,
//        "id": 0,
//        "doc": {
//          "Name": "Amsterdam",
//          "Country": "NL"
//        }
//      }
//      ...
//    ]
//  }
// If the callback is null, then the original score is used (1*idf at the moment)
func (m *MemOnlyIndex) TopN(limit int, query iq.Query, cb func(int32, float32, Document) float32) *SearchResult {
	out := &SearchResult{}
	scored := []Hit{}
	m.Foreach(query, func(did int32, originalScore float32, d Document) {
		out.Total++
		if limit == 0 {
			return
		}
		score := originalScore
		if cb != nil {
			score = cb(did, originalScore, d)
		}

		// just keep the list sorted
		// FIXME: use bounded priority queue
		doInsert := false
		if len(scored) < limit {
			doInsert = true
		} else if scored[len(scored)-1].Score < score {
			doInsert = true
		}

		if doInsert {
			hit := Hit{Score: score, ID: did, Document: d}
			if len(scored) < limit {
				scored = append(scored, hit)
			}
			for i := 0; i < len(scored); i++ {
				if scored[i].Score < hit.Score {
					copy(scored[i+1:], scored[i:])
					scored[i] = hit
					break
				}
			}
		}
	})

	out.Hits = scored

	return out
}

// Hit is struct result for `TopN` method
type Hit struct {
	Score    float32  `json:"score"`
	ID       int32    `json:"id"`
	Document Document `json:"doc"`
}

// SearchResult is the search result for the `TopN` method
type SearchResult struct {
	Total int   `json:"total"`
	Hits  []Hit `json:"hits"`
}
