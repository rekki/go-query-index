package tokenize

import (
	"testing"
)

type TestCase struct {
	in  string
	out []string
	t   []Tokenizer
}

type TestCaseT struct {
	in  string
	out []Token
	t   []Tokenizer
}

func Equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func EqualT(a, b []Token) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v.Text != b[i].Text || v.Position != b[i].Position || v.LineNo != b[i].LineNo {
			return false
		}
	}
	return true
}

func testMany(t *testing.T, cases []TestCase) {
	for _, c := range cases {
		tokenized := Tokenize(c.in, c.t...)
		if !Equal(tokenized, c.out) {
			t.Fatalf("in: <%s>, out: <%v>, expected: <%v>", c.in, tokenized, c.out)
		}
	}
}

func testManyT(t *testing.T, cases []TestCaseT) {
	for _, c := range cases {
		tokenized := TokenizeT(c.in, c.t...)
		if !EqualT(tokenized, c.out) {
			t.Fatalf("in: <%s>, out: <%v>, expected: <%v>", c.in, tokenized, c.out)
		}
	}
}

func TestUnique(t *testing.T) {
	cases := []TestCase{
		{
			in:  "hello hello world",
			out: []string{"hello", "world"},
			t:   []Tokenizer{NewWhitespace(), NewUnique()},
		},
	}
	testMany(t, cases)
}

func TestPositions(t *testing.T) {
	cases := []TestCaseT{
		{
			in:  "hello hello world a    b     c   ",
			out: []Token{{"hello", 0, 0}, {"world", 2, 0}, {"a", 3, 0}, {"b", 4, 0}, {"c", 5, 0}},
			t:   []Tokenizer{NewWhitespace(), NewUnique()},
		},
		{
			in: `

hello hello world a    b     c   

x y   

z


`,
			out: []Token{{"hello", 0, 2}, {"world", 2, 2}, {"a", 3, 2}, {"b", 4, 2}, {"c", 5, 2}, {"x", 6, 4}, {"y", 7, 4}, {"z", 8, 6}},
			t:   []Tokenizer{NewWhitespace(), NewUnique()},
		},

		{
			in: `abc
def`,
			out: []Token{{"ab", 0, 0}, {"abc", 0, 0}, {"de", 1, 1}, {"def", 1, 1}},
			t:   []Tokenizer{NewWhitespace(), NewLeftEdge(2), NewUnique()},
		},
	}
	testManyT(t, cases)
}

func TestCharNgram(t *testing.T) {
	cases := []TestCase{
		{
			in:  "rome",
			out: []string{"ro", "om", "me"},
			t:   []Tokenizer{NewCharNgram(2)},
		},
		{
			in:  "rome",
			out: []string{"$ro", "om", "me$"},
			t:   []Tokenizer{NewCharNgram(2), NewSurround("$")},
		},
		{
			in:  "rome",
			out: []string{"rom", "ome"},
			t:   []Tokenizer{NewCharNgram(3)},
		},
		{
			in:  "ro",
			out: []string{"ro"},
			t:   []Tokenizer{NewCharNgram(3)},
		},
		{
			in:  "",
			out: []string{""},
			t:   []Tokenizer{NewCharNgram(3)},
		},
		{
			in:  "rome",
			out: []string{"r", "o", "m", "e"},
			t:   []Tokenizer{NewCharNgram(1)},
		},
		{
			in:  "rome",
			out: []string{"rome"},
			t:   []Tokenizer{NewCharNgram(4)},
		},
	}
	testMany(t, cases)
}

func TestShingles(t *testing.T) {
	cases := []TestCase{
		{
			in:  "",
			out: []string{""},
			t:   []Tokenizer{NewShingles(3)},
		},
		{
			in:  "new york",
			out: []string{"new", "newyork", "york"},
			t:   []Tokenizer{NewWhitespace(), NewShingles(2)},
		},
		{
			in:  "new york",
			out: []string{"new", "york"},
			t:   []Tokenizer{NewWhitespace(), NewShingles(3)},
		},
		{
			in:  "new york",
			out: []string{"new", "york"},
			t:   []Tokenizer{NewWhitespace(), NewShingles(1)},
		},
		{
			in:  "new york city",
			out: []string{"new", "newyork", "york", "yorkcity", "city"},
			t:   []Tokenizer{NewWhitespace(), NewShingles(2)},
		},
		{
			in: "new york city",
			out: []string{
				"new", "newyorkcity", "york", "city",
			},
			t: []Tokenizer{NewWhitespace(), NewShingles(3)},
		},
		{
			in: "new york city killa",
			out: []string{
				"new", "newyorkcity", "york", "yorkcitykilla", "city", "killa",
			},
			t: []Tokenizer{NewWhitespace(), NewShingles(3)},
		},
		{
			in: "new york city killa gorilla",
			out: []string{
				"new", "newyorkcity", "york", "yorkcitykilla", "city", "citykillagorilla", "killa", "gorilla",
			},
			t: []Tokenizer{NewWhitespace(), NewShingles(3)},
		},
	}
	testMany(t, cases)
}

func TestSurround(t *testing.T) {
	cases := []TestCase{
		{
			in:  "hello abc world",
			out: []string{"$hello", "abc", "world$"},
			t:   []Tokenizer{NewWhitespace(), NewSurround("$"), NewUnique()},
		},
		{
			in:  "",
			out: []string{},
			t:   []Tokenizer{NewWhitespace(), NewSurround("$"), NewUnique()},
		},
		{
			in:  "a",
			out: []string{"$a$"},
			t:   []Tokenizer{NewWhitespace(), NewSurround("$"), NewUnique()},
		},
	}
	testMany(t, cases)
}

func TestSoundex(t *testing.T) {
	cases := []TestCase{
		{
			in:  "hello hallo abc world warld",
			out: []string{"H400", "H400", "A120", "W643", "W643"},
			t:   []Tokenizer{NewWhitespace(), NewSoundex()},
		},
		{
			in:  "",
			out: []string{},
			t:   []Tokenizer{NewWhitespace(), NewSoundex()},
		},
	}

	testMany(t, cases)
}

func TestNoop(t *testing.T) {
	cases := []TestCase{
		{
			in:  "hello hallo abc world warld",
			out: []string{"hello hallo abc world warld"},
			t:   []Tokenizer{NewNoop()},
		},
	}

	testMany(t, cases)
}

func TestEmpty(t *testing.T) {
	cases := []TestCase{
		{
			in:  "hello hallo abc world warld",
			out: []string{},
			t:   []Tokenizer{},
		},
	}

	testMany(t, cases)
}

func TestLegtEdge(t *testing.T) {
	cases := []TestCase{
		{
			in:  "hello",
			out: []string{"he", "hel", "hell", "hello"},
			t:   []Tokenizer{NewLeftEdge(2)},
		},
		{
			in:  "hello",
			out: []string{"hello"},
			t:   []Tokenizer{NewLeftEdge(20)},
		},
		{
			in:  "hello",
			out: []string{"h", "he", "hel", "hell", "hello"},
			t:   []Tokenizer{NewLeftEdge(1)},
		},
	}
	testMany(t, cases)
}

func TestWhitespace(t *testing.T) {
	cases := []TestCase{
		{
			in:  "hello",
			out: []string{"hello"},
			t:   []Tokenizer{NewWhitespace()},
		},
		{
			in:  "",
			out: []string{},
			t:   []Tokenizer{NewWhitespace()},
		},
		{
			in:  "     ",
			out: []string{},
			t:   []Tokenizer{NewWhitespace()},
		},
		{
			in:  "     a     b",
			out: []string{"a", "b"},
			t:   []Tokenizer{NewWhitespace()},
		},
		{
			in: ` a
b
c	g
d  f
`,
			out: []string{"a", "b", "c", "g", "d", "f"},
			t:   []Tokenizer{NewWhitespace()},
		},
	}
	testMany(t, cases)
}

func TestComplex(t *testing.T) {
	cases := []TestCase{
		{
			in:  "hello world hellz",
			out: []string{"h", "he", "hel", "hello", "w", "wo", "wor", "world", "hellz"},
			t: []Tokenizer{NewWhitespace(), NewLeftEdge(1), NewUnique(), NewCustom(func(c []Token) []Token {
				out := []Token{}
				for _, v := range c {
					if len(v.Text) != 4 {
						out = append(out, v)
					}
				}
				return out
			})},
		},
	}
	testMany(t, cases)
}
