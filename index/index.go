// Package index provides means to build the search index
package index

import (
	norm "github.com/rekki/go-query-normalize"
	tokenize "github.com/rekki/go-query-tokenize"
	"github.com/rekki/go-query/util/analyzer"
)

// Document provides an interface on the documents you want indexed
//
// Example if you want to index fields "name" and "country":
//  type ExampleCity struct {
//  	Name    string
//  	Country string
//  }
//
//  func (e *ExampleCity) IndexableFields() map[string]string {
//  	out := map[string]string{}
//
//  	out["name"] = e.Name
//  	out["country"] = e.Country
//
//  	return out
//  }
type Document interface {
	IndexableFields() map[string][]string
}

var DefaultNormalizer = []norm.Normalizer{
	norm.NewUnaccent(),
	norm.NewLowerCase(),
	norm.NewSpaceBetweenDigits(),
	norm.NewCleanup(norm.BASIC_NON_ALPHANUMERIC),
	norm.NewTrim(" "),
}

var DefaultSearchTokenizer = []tokenize.Tokenizer{
	tokenize.NewWhitespace(),
}

var DefaultIndexTokenizer = []tokenize.Tokenizer{
	tokenize.NewWhitespace(),
}

var DefaultAnalyzer = analyzer.NewAnalyzer(DefaultNormalizer, DefaultSearchTokenizer, DefaultIndexTokenizer)

var IDAnalyzer = analyzer.NewAnalyzer([]norm.Normalizer{norm.NewNoop()}, []tokenize.Tokenizer{tokenize.NewNoop()}, []tokenize.Tokenizer{tokenize.NewNoop()})

var SoundexTokenizer = []tokenize.Tokenizer{
	tokenize.NewWhitespace(),
	tokenize.NewSoundex(),
}

var FuzzyTokenizer = []tokenize.Tokenizer{
	tokenize.NewWhitespace(),
	tokenize.NewCharNgram(2),
	tokenize.NewUnique(),
	tokenize.NewSurround("$"),
}

var AutocompleteIndexTokenizer = []tokenize.Tokenizer{
	tokenize.NewWhitespace(),
	tokenize.NewLeftEdge(1),
}

var SoundexAnalyzer = analyzer.NewAnalyzer(DefaultNormalizer, SoundexTokenizer, SoundexTokenizer)

var FuzzyAnalyzer = analyzer.NewAnalyzer(DefaultNormalizer, FuzzyTokenizer, FuzzyTokenizer)

var AutocompleteAnalyzer = analyzer.NewAnalyzer(DefaultNormalizer, DefaultSearchTokenizer, AutocompleteIndexTokenizer)
