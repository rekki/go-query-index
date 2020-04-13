# index [![GitHub Actions Status](https://github.com/rekki/go-query-index/workflows/test/badge.svg?branch=master)](https://github.com/rekki/go-query-index/actions) [![codecov](https://codecov.io/gh/rekki/go-query-index/branch/master/graph/badge.svg)](https://codecov.io/gh/rekki/go-query-index) [![GoDoc](https://godoc.org/github.com/rekki/go-query-index?status.svg)](https://godoc.org/github.com/rekki/go-query-index)

> ## Search index for go-query

Illustration of how you can use go-query to build a somewhat functional search index:

```go
package main

import (
  "log"

  iq "github.com/rekki/go-query"
  "github.com/rekki/go-query/util/analyzer"
  "github.com/rekki/go-query/util/index"
)

type ExampleCity struct {
  Name    string
  Country string
}

func (e *ExampleCity) IndexableFields() map[string][]string {
  out := map[string][]string{}

  out["name"] = []string{e.Name}
  out["name_fuzzy"] = []string{e.Name}
  out["name_soundex"] = []string{e.Name}
  out["country"] = []string{e.Country}

  return out
}

func toDocuments(in []*ExampleCity) []index.Document {
  out := make([]index.Document, len(in))
  for i, d := range in {
    out[i] = index.Document(d)
  }
  return out
}

func main() {
  m := index.NewMemOnlyIndex(map[string]*analyzer.Analyzer{
    "name":         index.AutocompleteAnalyzer,
    "name_fuzzy":   index.FuzzyAnalyzer,
    "name_soundex": index.SoundexAnalyzer,
    "country":      index.IDAnalyzer,
  })

  list := []*ExampleCity{
    &ExampleCity{Name: "Amsterdam", Country: "NL"},
    &ExampleCity{Name: "Amsterdam University", Country: "NL"},
    &ExampleCity{Name: "Amsterdam University Second", Country: "NL"},
    &ExampleCity{Name: "London", Country: "UK"},
    &ExampleCity{Name: "Sofia", Country: "BG"},
  }

  m.Index(toDocuments(list)...)

  // search for "(name:aMS OR name:u) AND (country:NL OR country:BG)"

  query := iq.Or(
    iq.And(
      iq.Or(m.Terms("name", "aMS u")...),
      iq.Or(m.Terms("country", "NL")...),
    ).SetBoost(2),
    iq.And(
      iq.Or(m.Terms("name_fuzzy", "bondom u")...),
      iq.Or(m.Terms("country", "UK")...),
    ).SetBoost(0.1),
    iq.And(
      iq.Or(m.Terms("name_soundex", "sfa")...),
      iq.Or(m.Terms("country", "BG")...),
    ).SetBoost(0.01),
  )
  log.Printf("query: %v", query.String())
  m.Foreach(query, func(did int32, score float32, doc index.Document) {
    city := doc.(*ExampleCity)
    log.Printf("%v matching with score %f", city, score)
  })
}
```

will print

```sh
2019/12/03 18:40:11 &{Amsterdam NL} matching with score 3.923317
2019/12/03 18:40:11 &{Amsterdam University NL} matching with score 6.428843
2019/12/03 18:40:11 &{Amsterdam University NL Second} matching with score 6.428843
2019/12/03 18:40:11 &{London UK} matching with score 0.537528
2019/12/03 18:40:11 &{Sofia BG} matching with score 0.035835
```
