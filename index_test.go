package index

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	iq "github.com/rekki/go-query"
)

// get full list from https://raw.githubusercontent.com/lutangar/cities.json/master/cities.json

type ExampleCity struct {
	ID      int32
	Name    string
	TestID  string
	Country string
	Names   []string
}

func (e *ExampleCity) DocumentID() int32 {
	return e.ID
}

func (e *ExampleCity) IndexableFields() map[string][]string {
	out := map[string][]string{}

	out["name"] = []string{e.Name}
	out["names"] = e.Names
	out["_id"] = []string{e.TestID}
	out["country"] = []string{e.Country}

	return out
}

func toDocuments(in []*ExampleCity) []Document {
	out := make([]Document, len(in))
	for i, d := range in {
		out[i] = Document(d)
	}
	return out
}

func toDocumentsID(in []*ExampleCity) []DocumentWithID {
	out := make([]DocumentWithID, len(in))
	for i, d := range in {
		out[i] = DocumentWithID(d)
	}
	return out
}

func TestUnique(t *testing.T) {
	m := NewMemOnlyIndex(nil)
	list := []*ExampleCity{
		{Names: []string{"Amsterdam", "Amsterdam"}, Country: "NL"},
		{Names: []string{"Sofia", "Sofia"}, Country: "NL"},
	}

	m.Index(toDocuments(list)...)
	n := 0
	q := iq.Or(m.Terms("names", "sofia")...)

	m.Foreach(q, func(did int32, score float32, doc Document) {
		n++
	})
	if n != 1 {
		t.Fatalf("expected 2 got %d", n)
	}
}

func TestMerge(t *testing.T) {
	a := NewMemOnlyIndex(nil)
	b := NewMemOnlyIndex(nil)
	listA := []*ExampleCity{
		{ID: 0, Names: []string{"Amsterdam", "Amsterdam"}, Country: "NL"},
		{ID: 2, Names: []string{"Sofia", "Sofia"}, Country: "BG"},
	}
	listB := []*ExampleCity{
		{Names: []string{"Reykjavik", "Reykjavik"}, Country: "IS"},
		{Names: []string{"Amsterdam", "Amsterdam"}, Country: "NL"},
	}

	a.Index(toDocuments(listA)...)
	b.Index(toDocuments(listB)...)
	a.MergeInto(b)

	n := 0
	q := iq.Or(a.Terms("names", "Amsterdam")...)

	a.Foreach(q, func(did int32, score float32, doc Document) {
		n++

	})
	if n != 2 {
		t.Fatalf("expected 2 got %d", n)
	}
}

func TestDelete(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	for k := 0; k < 100; k++ {
		m := NewMemOnlyIndex(nil)
		list := []*ExampleCity{}

		max := 1000
		min := 100
		end := min + rand.Intn(max)
		for j := 0; j < end; j++ {
			list = append(list, &ExampleCity{Names: []string{fmt.Sprintf("%d", j), "everything"}})
		}

		m.Index(toDocuments(list)...)

		expect := func(term string, expected int) {
			q := iq.And(m.Terms("names", term)...)
			n := 0

			m.Foreach(q, func(did int32, score float32, doc Document) {
				n++
			})

			if n != expected {
				t.Fatalf("%s expected %d got %d", term, expected, n)
			}
		}

		deleted := map[int32]bool{}
		for i := 0; i < 100; i++ {
			did := int32(rand.Intn(end))
			if deleted[did] {
				continue
			}

			expect("everything", len(list)-len(deleted))

			m.Delete(did)
			if m.Get(did) != nil {
				t.Fatal("expected nil")
			}

			deleted[did] = true
			expect("everything", len(list)-len(deleted))
		}
	}
}

func TestDeleteByID(t *testing.T) {
	m := NewMemOnlyIndex(nil)
	list := []*ExampleCity{
		{Names: []string{"Amsterdam", "Amsterdam"}, Country: "NL", TestID: "a"},
		{Names: []string{"Sofia", "Sofia"}, Country: "NL", TestID: "b"},
		{Names: []string{"Paris", "Paris"}, Country: "FR", TestID: "c"},
	}

	m.Index(toDocuments(list)...)

	expect := func(term string, id int32, expected int) {
		q := iq.And(m.Terms("names", term)...)
		n := 0
		m.Foreach(q, func(did int32, score float32, doc Document) {
			n++
			if did != id {
				t.Fatalf("%s unexpected match %d got %d", term, id, did)
			}
		})
		if n != expected {
			t.Fatalf("%s expected %d got %d", term, expected, n)
		}

	}

	expect("amsterdam", 0, 1)
	expect("sofia", 1, 1)
	expect("paris", 2, 1)

	m.DeleteByID("b")
	if m.Get(1) != nil {
		t.Fatal("expected nil")
	}
	if m.GetByID("b") != nil {
		t.Fatal("expected nil")
	}
	expect("amsterdam", 0, 1)
	expect("sofia", 1, 0)
	expect("paris", 2, 1)
	expect("paris", 2, 1)

	m.DeleteByID("c")
	if m.GetByID("c") != nil {
		t.Fatal("expected nil")
	}

	if m.Get(2) != nil {
		t.Fatal("expected nil")
	}

	expect("amsterdam", 0, 1)
	expect("sofia", 1, 0)
	expect("paris", 2, 0)

	m.Index(toDocuments([]*ExampleCity{{Names: []string{"Sofia", "Sofia"}, Country: "NL"}})...)
	expect("amsterdam", 0, 1)
	expect("sofia", 3, 1)
	expect("paris", 2, 0)

	m.Index(toDocuments([]*ExampleCity{{Names: []string{"Paris", "Paris"}, Country: "NL"}})...)
	expect("paris", 4, 1)

}

func TestExample(t *testing.T) {
	m := NewMemOnlyIndex(nil)
	list := []*ExampleCity{
		{Name: "Amsterdam", Country: "NL"},
		{Name: "Amsterdam, USA", Country: "USA"},
		{Name: "London", Country: "UK"},
		{Name: "Sofia", Country: "BG"},
	}

	m.Index(toDocuments(list)...)
	n := 0
	q := iq.Or(m.Terms("name", "aMSterdam sofia")...)

	m.Foreach(q, func(did int32, score float32, doc Document) {
		city := doc.(*ExampleCity)
		log.Printf("%v matching with score %f", city, score)
		n++
	})
	if n != 3 {
		t.Fatalf("expected 2 got %d", n)
	}
	n = 0

	q = iq.Or(m.Terms("name", "aMSterdam sofia")...)
	top := m.TopN(1, q, func(did int32, score float32, doc Document) float32 {
		city := doc.(*ExampleCity)
		if city.Country == "NL" {
			score += 100
		}
		n++
		return score
	})

	if top.Hits[0].Score < 100 {
		t.Fatalf("expected > 100")
	}
	if top.Total != 3 {
		t.Fatalf("expected 3")
	}
	if len(top.Hits) != 1 {
		t.Fatalf("expected 1")
	}

	q = iq.Or(m.Terms("name", "aMSterdam sofia")...)
	top = m.TopN(0, q, func(did int32, score float32, doc Document) float32 {
		return score
	})

	if len(top.Hits) != 0 {
		t.Fatalf("expected 0")
	}
	if top.Total != 3 {
		t.Fatalf("expected 3")
	}
}

func TestExampleDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "forward")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	m := NewDirIndex(dir, NewFDCache(10), nil)
	list := []*ExampleCity{
		{Name: "Amsterdam", Country: "NL", ID: 0},
		{Name: "Amsterdam, USA", Country: "USA", ID: 1},
		{Name: "London", Country: "UK", ID: 2},
		{Name: "Sofia Amsterdam", Country: "BG", ID: 3},
	}

	for i := len(list); i < 10000; i++ {
		list = append(list, &ExampleCity{Name: fmt.Sprintf("%dLondon", i), Country: "UK", ID: int32(i)})
	}
	err = m.Index(toDocumentsID(list)...)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	q := iq.And(m.Terms("name", "aMSterdam sofia")...)

	m.Foreach(q, func(did int32, score float32) {
		city := list[did]
		log.Printf("%v matching with score %f", city, score)
		n++
	})
	if n != 1 {
		t.Fatalf("expected 1 got %d", n)
	}

	n = 0
	qq := iq.Or(m.Terms("name", "aMSterdam sofia")...)

	m.Foreach(qq, func(did int32, score float32) {
		city := list[did]
		log.Printf("%v matching with score %f", city, score)
		n++
	})
	if n != 3 {
		t.Fatalf("expected 3 got %d", n)
	}

	m.Lazy = true

	n = 0
	qqq := iq.Or(m.Terms("name", "aMSterdam sofia")...)

	m.Foreach(qqq, func(did int32, score float32) {
		city := list[did]
		log.Printf("lazy %v matching with score %f", city, score)
		n++
	})
	if n != 3 {
		t.Fatalf("expected 3 got %d", n)
	}

}

func BenchmarkDirIndexBuild(b *testing.B) {
	b.StopTimer()
	dir, err := ioutil.TempDir("", "forward")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	m := NewDirIndex(dir, NewFDCache(10), nil)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err = m.Index(DocumentWithID(&ExampleCity{Name: "Amsterdam", Country: "NL", ID: int32(i)}))
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()

}

func BenchmarkMemIndexBuild(b *testing.B) {
	m := NewMemOnlyIndex(nil)
	for i := 0; i < b.N; i++ {
		m.Index(DocumentWithID(&ExampleCity{Name: "Amsterdam", Country: "NL", ID: int32(i)}))
	}

}

var dont = 0

func BenchmarkDirIndexSearch10000(b *testing.B) {
	b.StopTimer()
	dir, err := ioutil.TempDir("", "forward")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	m := NewDirIndex(dir, NewFDCache(10), nil)
	for i := 0; i < 10000; i++ {
		err = m.Index(DocumentWithID(&ExampleCity{Name: "Amsterdam", Country: "NL", ID: int32(i)}))
		if err != nil {
			panic(err)
		}
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		q := iq.Or(m.Terms("name", "aMSterdam sofia")...)
		m.Foreach(q, func(did int32, score float32) {
			n++
			dont++

		})
	}
	b.StopTimer()
}

func BenchmarkMemIndexSearch10000(b *testing.B) {
	b.StopTimer()
	m := NewMemOnlyIndex(nil)
	for i := 0; i < 10000; i++ {
		m.Index(Document(&ExampleCity{Name: "Amsterdam", Country: "NL", ID: int32(i)}))
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		q := iq.Or(m.Terms("name", "aMSterdam sofia")...)
		m.Foreach(q, func(did int32, score float32, _d Document) {
			n++
			dont++

		})
	}
	b.StopTimer()
}
