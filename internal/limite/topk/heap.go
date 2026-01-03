package topk

// Item represents an element in the Top-K list.
type Item struct {
	Key         string
	Count       uint64
	Fingerprint uint64
}

// list is a min-heap of Items. We implement heap operations manually instead of
// using container/heap to avoid interface{} allocation overhead on every operation.
type list []Item

// linearSearch finds an item by key using a backwards linear scan. For small K
// values (< 500), this beats hash table lookup because the slice fits in L1/L2
// cache and sequential memory access is very fast. We scan backwards because
// recently accessed items tend to cluster toward the end after heap operations.
func (l list) linearSearch(key string) (int, bool) {
	for i := len(l) - 1; i >= 0; i-- {
		if l[i].Key == key {
			return i, true
		}
	}
	return -1, false
}

func (l list) Len() int      { return len(l) }
func (l list) Swap(i, j int) { l[i], l[j] = l[j], l[i] }

// Less orders by count (min-heap) with fingerprint as tiebreaker for determinism.
func (l list) Less(i, j int) bool {
	if l[i].Count != l[j].Count {
		return l[i].Count < l[j].Count
	}
	return l[i].Fingerprint < l[j].Fingerprint
}

// Push appends an item and bubbles it up to restore the heap invariant.
func (l *list) Push(x Item) {
	*l = append(*l, x)
	l.up(len(*l) - 1)
}

// Pop removes and returns the last element.
func (l *list) Pop() Item {
	old := *l
	n := len(old)
	item := old[n-1]
	*l = old[:n-1]
	return item
}

// up bubbles element j toward the root until the heap invariant is restored.
func (l list) up(j int) {
	for {
		i := (j - 1) / 2 // parent index
		if i == j || !l.Less(j, i) {
			break
		}
		l.Swap(i, j)
		j = i
	}
}

// down sinks element i0 toward the leaves until the heap invariant is restored.
// Returns true if the element moved (was swapped at least once).
func (l list) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1          // left child
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1
		j2 := j1 + 1 // right child
		if j2 < n && l.Less(j2, j1) {
			j = j2 // pick smaller child
		}
		if !l.Less(j, i) {
			break
		}
		l.Swap(i, j)
		i = j
	}
	return i > i0
}

// Fix restores the heap invariant after element i has changed its count.
func (l *list) Fix(i int) {
	if !l.down(i, len(*l)) {
		l.up(i)
	}
}
