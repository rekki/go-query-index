// Package index provides means to build the search index
package index

import (
	norm "github.com/rekki/go-query-normalize"
	tokenize "github.com/rekki/go-query-tokenize"
	"github.com/rekki/go-query/util/analyzer"
)

// Document provides an interface on the documents you want indexed
//
//  Example if you want to index fields "name" and "country":
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

// --- Normalizers ---

// DefaultNormalizer is an default normalizer
var DefaultNormalizer = []norm.Normalizer{
	norm.NewUnaccent(),
	norm.NewLowerCase(),
	norm.NewSpaceBetweenDigits(),
	norm.NewCleanup(norm.BASIC_NON_ALPHANUMERIC),
	norm.NewTrim(" "),
}

// --- Tokenizers ---

// DefaultSearchTokenizer is an default search tokenizer
var DefaultSearchTokenizer = []tokenize.Tokenizer{
	tokenize.NewWhitespace(),
}

// DefaultIndexTokenizer is an default tokenizer
var DefaultIndexTokenizer = []tokenize.Tokenizer{
	tokenize.NewWhitespace(),
}

// SoundexTokenizer is an soundex tokenizer
var SoundexTokenizer = []tokenize.Tokenizer{
	tokenize.NewWhitespace(),
	tokenize.NewSoundex(),
}

// FuzzyTokenizer is an fuzzy tokenizer
var FuzzyTokenizer = []tokenize.Tokenizer{
	tokenize.NewWhitespace(),
	tokenize.NewCharNgram(2),
	tokenize.NewUnique(),
	tokenize.NewSurround("$"),
}

// AutocompleteIndexTokenizer is an autocompelete tokenizer
var AutocompleteIndexTokenizer = []tokenize.Tokenizer{
	tokenize.NewWhitespace(),
	tokenize.NewLeftEdge(1),
}

// --- Analyzers ---

// DefaultAnalyzer is an default analyzer
var DefaultAnalyzer = analyzer.NewAnalyzer(
	DefaultNormalizer,
	DefaultSearchTokenizer,
	DefaultIndexTokenizer,
)

// IDAnalyzer is an id analyzer
var IDAnalyzer = analyzer.NewAnalyzer(
	[]norm.Normalizer{norm.NewNoop()},
	[]tokenize.Tokenizer{tokenize.NewNoop()},
	[]tokenize.Tokenizer{tokenize.NewNoop()},
)

// SoundexAnalyzer provides an analyzer for soundex
// https://en.wikipedia.org/wiki/Soundex
var SoundexAnalyzer = analyzer.NewAnalyzer(
	DefaultNormalizer,
	SoundexTokenizer,
	SoundexTokenizer,
)

// FuzzyAnalyzer provides an analyzer for the fuzzy search
var FuzzyAnalyzer = analyzer.NewAnalyzer(
	DefaultNormalizer,
	FuzzyTokenizer,
	FuzzyTokenizer,
)

// AutocompleteAnalyzer is an autocomplete analyzer
var AutocompleteAnalyzer = analyzer.NewAnalyzer(
	DefaultNormalizer,
	DefaultSearchTokenizer,
	AutocompleteIndexToken,
	izer)
