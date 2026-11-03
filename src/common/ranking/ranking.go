package ranking

import (
	"sort"

	"tp-distribuidos/common/protocol/inner"
)

const TopSize = 20

type TopN struct {
	n     int
	items []inner.WordCount
}

func NewTopN(n int) *TopN {
	return &TopN{n: n, items: make([]inner.WordCount, 0, n+1)}
}

func better(a, b inner.WordCount) bool {
	return a.Count > b.Count || (a.Count == b.Count && a.Word < b.Word)
}

func (t *TopN) Add(wc inner.WordCount) {
	if len(t.items) == t.n && !better(wc, t.items[len(t.items)-1]) {
		return
	}
	i := sort.Search(len(t.items), func(i int) bool { return better(wc, t.items[i]) })
	t.items = append(t.items, inner.WordCount{})
	copy(t.items[i+1:], t.items[i:])
	t.items[i] = wc
	if len(t.items) > t.n {
		t.items = t.items[:t.n]
	}
}

func (t *TopN) Items() []inner.WordCount {
	return t.items
}
