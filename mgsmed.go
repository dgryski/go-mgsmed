// Package mgsmed implements the Misra-Gries Reduce-By-Sample-Median streaming TopK algorithm
/*

   A High-Performance Algorithm for Identifying Frequent Items in Data Streams
   https://arxiv.org/abs/1705.07001

*/
package mgsmed

import (
	"math/rand"
	"sort"
)

type Stream struct {
	t    map[string]int
	keys []string
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
	s.keys = s.keys[:cap(s.keys)]
	keep, rm := 0, len(s.keys)

	for key, count := range s.t {
		if count <= median {
			rm--
			s.keys[rm] = key
		} else {
			s.keys[keep] = key
			keep++
		}
	}

	for _, key := range s.keys[rm:] {
		delete(s.t, key)
	}

	s.keys = s.keys[:keep]
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

	sort.Ints(counts)

	if l%2 == 0 {
		a := counts[l/2]
		b := counts[l/2-1]

		// integer addition without overflow
		return (a & b) + ((a ^ b) >> 1)
	}

	return counts[len(counts)/2]
}

func (s *Stream) Estimate(key string) int {
	return s.t[key]
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
		elements = append(elements, Element{Key: k, Count: v})
	}

	sort.Sort(elementsByCountDescending(elements))

	return elements
}
