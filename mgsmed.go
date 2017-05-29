// Package mgsmed implements the Misra-Gries Reduce-By-Sample-Median streaming TopK algorithm
/*

   A High-Performance Algorithm for Identifying Frequent Items in Data Streams
   https://arxiv.org/abs/1705.07001

*/
package mgsmed

import (
	"math/rand"
	"sort"

	"github.com/dgryski/go-qselect"
)

type Stream struct {
	t      map[string]int
	keys   []string
	offset int
}

func New(k int) *Stream {
	return &Stream{
		t:    make(map[string]int, k),
		keys: make([]string, 0, k),
	}
}

func (s *Stream) Update(key string, d int) {
	if v, ok := s.t[key]; ok {
		s.t[key] = v + d
		return
	}

	if len(s.t) < cap(s.keys) {
		s.t[key] = d
		s.keys = append(s.keys, key)
		return
	}

	c := s.decrementCounters()

	s.offset += c

	if d >= c {
		s.t[key] = d - c
	}
}

func (s *Stream) decrementCounters() int {
	median := s.computeMedian()
	s.clean(median)
	return median
}

func (s *Stream) clean(median int) {
	s.keys = s.keys[:0]

	for key, count := range s.t {
		count -= median
		if count <= 0 {
			delete(s.t, key)
		} else {
			s.t[key] = count
			s.keys = append(s.keys, key)
		}
	}
}

func (s *Stream) computeMedian() int {
	// constant from paper's implementation section
	l := 1024

	if len(s.keys) < l {
		l = len(s.keys)
	}

	counts := make([]int, l)
	for i := range counts {
		counts[i] = s.t[s.keys[rand.Intn(len(s.keys))]]
	}

	median := len(counts) / 2
	qselect.Select(sort.IntSlice(counts), median)
	return counts[median]
}

func (s *Stream) Estimate(key string) int {
	if count, ok := s.t[key]; ok {
		return count + s.offset
	}
	return 0
}

type Element struct {
	Key   string
	Count int
}

type elementsByCountDescending []Element

func (elts elementsByCountDescending) Len() int { return len(elts) }
func (elts elementsByCountDescending) Less(i, j int) bool {
	return (elts[i].Count > elts[j].Count) || (elts[i].Count == elts[j].Count && elts[i].Key < elts[j].Key)
}
func (elts elementsByCountDescending) Swap(i, j int) { elts[i], elts[j] = elts[j], elts[i] }

func (s *Stream) Keys() []Element {

	elements := make([]Element, 0, len(s.t))

	for k, v := range s.t {
		elements = append(elements, Element{Key: k, Count: v + s.offset})
	}

	sort.Sort(elementsByCountDescending(elements))

	return elements
}
