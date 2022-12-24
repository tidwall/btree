package btree

import "sync/atomic"

type ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 | ~string
}

type copier[T any] interface {
	Copy() T
}

type isoCopier[T any] interface {
	IsoCopy() T
}

func degreeToMinMax(deg int) (min, max int) {
	if deg <= 0 {
		deg = 32
	} else if deg == 1 {
		deg = 2 // must have at least 2
	}
	max = deg*2 - 1 // max items per node. max children is +1
	min = max / 2
	return min, max
}

var gisoid uint64

func newIsoID() uint64 {
	return atomic.AddUint64(&gisoid, 1)
}

type mapPair[K ordered, V any] struct {
	// The `value` field should be before the `key` field because doing so
	// allows for the Go compiler to optimize away the `value` field when
	// it's a `struct{}`, which is the case for `btree.Set`.
	value V
	key   K
}

// Copy the node for safe isolation.
func (tr *Map[K, V]) copy(n *mapNode[K, V]) *mapNode[K, V] {
	n2 := new(mapNode[K, V])
	n2.isoid = tr.isoid
	n2.count = n.count
	n2.items = make([]mapPair[K, V], len(n.items), cap(n.items))
	copy(n2.items, n.items)
	if tr.copyValues {
		for i := 0; i < len(n2.items); i++ {
			n2.items[i].value =
				((interface{})(n2.items[i].value)).(copier[V]).Copy()
		}
	} else if tr.isoCopyValues {
		for i := 0; i < len(n2.items); i++ {
			n2.items[i].value =
				((interface{})(n2.items[i].value)).(isoCopier[V]).IsoCopy()
		}
	}
	if !n.leaf() {
		n2.children = new([]*mapNode[K, V])
		*n2.children = make([]*mapNode[K, V], len(*n.children), tr.max+1)
		copy(*n2.children, *n.children)
	}
	return n2
}

// isoLoad loads the provided node and, if needed, performs a copy-on-write.
func (tr *Map[K, V]) isoLoad(cn **mapNode[K, V], mut bool) *mapNode[K, V] {
	if mut && (*cn).isoid != tr.isoid {
		*cn = tr.copy(*cn)
	}
	return *cn
}

func (tr *Map[K, V]) newNode(leaf bool) *mapNode[K, V] {
	n := new(mapNode[K, V])
	n.isoid = tr.isoid
	if !leaf {
		n.children = new([]*mapNode[K, V])
	}
	return n
}

// leaf returns true if the node is a leaf.
func (n *mapNode[K, V]) leaf() bool {
	return n.children == nil
}

func (tr *Map[K, V]) search(n *mapNode[K, V], key K) (index int, found bool) {
	low, high := 0, len(n.items)
	for low < high {
		h := (low + high) / 2
		if !(key < n.items[h].key) {
			low = h + 1
		} else {
			high = h
		}
	}
	if low > 0 && !(n.items[low-1].key < key) {
		return low - 1, true
	}
	return low, false
}

func (tr *Map[K, V]) init(degree int) {
	if tr.min != 0 {
		return
	}
	tr.min, tr.max = degreeToMinMax(degree)
	_, tr.copyValues = ((interface{})(tr.empty.value)).(copier[V])
	if !tr.copyValues {
		_, tr.isoCopyValues = ((interface{})(tr.empty.value)).(isoCopier[V])
	}
}

func (tr *Map[K, V]) nodeSplit(n *mapNode[K, V],
) (right *mapNode[K, V], median mapPair[K, V]) {
	i := tr.max / 2
	median = n.items[i]

	// right node
	right = tr.newNode(n.leaf())
	right.items = n.items[i+1:]
	if !n.leaf() {
		*right.children = (*n.children)[i+1:]
	}
	right.updateCount()

	// left node
	n.items[i] = tr.empty
	n.items = n.items[:i:i]
	if !n.leaf() {
		*n.children = (*n.children)[: i+1 : i+1]
	}
	n.updateCount()
	return right, median
}

func (n *mapNode[K, V]) updateCount() {
	n.count = len(n.items)
	if !n.leaf() {
		for i := 0; i < len(*n.children); i++ {
			n.count += (*n.children)[i].count
		}
	}
}

func (tr *Map[K, V]) nodeSet(pn **mapNode[K, V], item mapPair[K, V],
) (prev V, replaced bool, split bool) {
	n := tr.isoLoad(pn, true)
	i, found := tr.search(n, item.key)
	if found {
		prev = n.items[i].value
		n.items[i] = item
		return prev, true, false
	}
	if n.leaf() {
		if len(n.items) == tr.max {
			return tr.empty.value, false, true
		}
		n.items = append(n.items, tr.empty)
		copy(n.items[i+1:], n.items[i:])
		n.items[i] = item
		n.count++
		return tr.empty.value, false, false
	}
	prev, replaced, split = tr.nodeSet(&(*n.children)[i], item)
	if split {
		if len(n.items) == tr.max {
			return tr.empty.value, false, true
		}
		right, median := tr.nodeSplit((*n.children)[i])
		*n.children = append(*n.children, nil)
		copy((*n.children)[i+1:], (*n.children)[i:])
		(*n.children)[i+1] = right
		n.items = append(n.items, tr.empty)
		copy(n.items[i+1:], n.items[i:])
		n.items[i] = median
		return tr.nodeSet(&n, item)
	}
	if !replaced {
		n.count++
	}
	return prev, replaced, false
}

func (tr *Map[K, V]) scan(iter func(key K, value V) bool, mut bool) {
	if tr.root == nil {
		return
	}
	tr.nodeScan(&tr.root, iter, mut)
}

func (tr *Map[K, V]) nodeScan(cn **mapNode[K, V],
	iter func(key K, value V) bool, mut bool,
) bool {
	n := tr.isoLoad(cn, mut)
	if n.leaf() {
		for i := 0; i < len(n.items); i++ {
			if !iter(n.items[i].key, n.items[i].value) {
				return false
			}
		}
		return true
	}
	for i := 0; i < len(n.items); i++ {
		if !tr.nodeScan(&(*n.children)[i], iter, mut) {
			return false
		}
		if !iter(n.items[i].key, n.items[i].value) {
			return false
		}
	}
	return tr.nodeScan(&(*n.children)[len(*n.children)-1], iter, mut)
}

func (tr *Map[K, V]) get(key K, mut bool) (V, bool) {
	if tr.root == nil {
		return tr.empty.value, false
	}
	n := tr.isoLoad(&tr.root, mut)
	for {
		i, found := tr.search(n, key)
		if found {
			return n.items[i].value, true
		}
		if n.leaf() {
			return tr.empty.value, false
		}
		n = tr.isoLoad(&(*n.children)[i], mut)
	}
}

func (tr *Map[K, V]) delete(pn **mapNode[K, V], max bool, key K,
) (mapPair[K, V], bool) {
	n := tr.isoLoad(pn, true)
	var i int
	var found bool
	if max {
		i, found = len(n.items)-1, true
	} else {
		i, found = tr.search(n, key)
	}
	if n.leaf() {
		if found {
			// found the items at the leaf, remove it and return.
			prev := n.items[i]
			copy(n.items[i:], n.items[i+1:])
			n.items[len(n.items)-1] = tr.empty
			n.items = n.items[:len(n.items)-1]
			n.count--
			return prev, true
		}
		return tr.empty, false
	}

	var prev mapPair[K, V]
	var deleted bool
	if found {
		if max {
			i++
			prev, deleted = tr.delete(&(*n.children)[i], true, tr.empty.key)
		} else {
			prev = n.items[i]
			maxItem, _ := tr.delete(&(*n.children)[i], true, tr.empty.key)
			deleted = true
			n.items[i] = maxItem
		}
	} else {
		prev, deleted = tr.delete(&(*n.children)[i], max, key)
	}
	if !deleted {
		return tr.empty, false
	}
	n.count--
	if len((*n.children)[i].items) < tr.min {
		tr.nodeRebalance(n, i)
	}
	return prev, true
}

// nodeRebalance rebalances the child nodes following a delete operation.
// Provide the index of the child node with the number of items that fell
// below minItems.
func (tr *Map[K, V]) nodeRebalance(n *mapNode[K, V], i int) {
	if i == len(n.items) {
		i--
	}

	// ensure copy-on-write
	left := tr.isoLoad(&(*n.children)[i], true)
	right := tr.isoLoad(&(*n.children)[i+1], true)

	if len(left.items)+len(right.items) < tr.max {
		// Merges the left and right children nodes together as a single node
		// that includes (left,item,right), and places the contents into the
		// existing left node. Delete the right node altogether and move the
		// following items and child nodes to the left by one slot.

		// merge (left,item,right)
		left.items = append(left.items, n.items[i])
		left.items = append(left.items, right.items...)
		if !left.leaf() {
			*left.children = append(*left.children, *right.children...)
		}
		left.count += right.count + 1

		// move the items over one slot
		copy(n.items[i:], n.items[i+1:])
		n.items[len(n.items)-1] = tr.empty
		n.items = n.items[:len(n.items)-1]

		// move the children over one slot
		copy((*n.children)[i+1:], (*n.children)[i+2:])
		(*n.children)[len(*n.children)-1] = nil
		(*n.children) = (*n.children)[:len(*n.children)-1]
	} else if len(left.items) > len(right.items) {
		// move left -> right over one slot

		// Move the item of the parent node at index into the right-node first
		// slot, and move the left-node last item into the previously moved
		// parent item slot.
		right.items = append(right.items, tr.empty)
		copy(right.items[1:], right.items)
		right.items[0] = n.items[i]
		right.count++
		n.items[i] = left.items[len(left.items)-1]
		left.items[len(left.items)-1] = tr.empty
		left.items = left.items[:len(left.items)-1]
		left.count--

		if !left.leaf() {
			// move the left-node last child into the right-node first slot
			*right.children = append(*right.children, nil)
			copy((*right.children)[1:], *right.children)
			(*right.children)[0] = (*left.children)[len(*left.children)-1]
			(*left.children)[len(*left.children)-1] = nil
			(*left.children) = (*left.children)[:len(*left.children)-1]
			left.count -= (*right.children)[0].count
			right.count += (*right.children)[0].count
		}
	} else {
		// move left <- right over one slot

		// Same as above but the other direction
		left.items = append(left.items, n.items[i])
		left.count++
		n.items[i] = right.items[0]
		copy(right.items, right.items[1:])
		right.items[len(right.items)-1] = tr.empty
		right.items = right.items[:len(right.items)-1]
		right.count--

		if !left.leaf() {
			*left.children = append(*left.children, (*right.children)[0])
			copy(*right.children, (*right.children)[1:])
			(*right.children)[len(*right.children)-1] = nil
			*right.children = (*right.children)[:len(*right.children)-1]
			left.count += (*left.children)[len(*left.children)-1].count
			right.count -= (*left.children)[len(*left.children)-1].count
		}
	}
}

func (tr *Map[K, V]) ascend(pivot K, iter func(key K, value V) bool, mut bool) {
	if tr.root == nil {
		return
	}
	tr.nodeAscend(&tr.root, pivot, iter, mut)
}

// The return value of this function determines whether we should keep iterating
// upon this functions return.
func (tr *Map[K, V]) nodeAscend(cn **mapNode[K, V], pivot K,
	iter func(key K, value V) bool, mut bool,
) bool {
	n := tr.isoLoad(cn, mut)
	i, found := tr.search(n, pivot)
	if !found {
		if !n.leaf() {
			if !tr.nodeAscend(&(*n.children)[i], pivot, iter, mut) {
				return false
			}
		}
	}
	// We are either in the case that
	// - node is found, we should iterate through it starting at `i`,
	//   the index it was located at.
	// - node is not found, and TODO: fill in.
	for ; i < len(n.items); i++ {
		if !iter(n.items[i].key, n.items[i].value) {
			return false
		}
		if !n.leaf() {
			if !tr.nodeScan(&(*n.children)[i+1], iter, mut) {
				return false
			}
		}
	}
	return true
}

func (tr *Map[K, V]) reverse(iter func(key K, value V) bool, mut bool) {
	if tr.root == nil {
		return
	}
	tr.nodeReverse(&tr.root, iter, mut)
}

func (tr *Map[K, V]) nodeReverse(cn **mapNode[K, V],
	iter func(key K, value V) bool, mut bool,
) bool {
	n := tr.isoLoad(cn, mut)
	if n.leaf() {
		for i := len(n.items) - 1; i >= 0; i-- {
			if !iter(n.items[i].key, n.items[i].value) {
				return false
			}
		}
		return true
	}
	if !tr.nodeReverse(&(*n.children)[len(*n.children)-1], iter, mut) {
		return false
	}
	for i := len(n.items) - 1; i >= 0; i-- {
		if !iter(n.items[i].key, n.items[i].value) {
			return false
		}
		if !tr.nodeReverse(&(*n.children)[i], iter, mut) {
			return false
		}
	}
	return true
}

func (tr *Map[K, V]) descend(
	pivot K,
	iter func(key K, value V) bool,
	mut bool,
) {
	if tr.root == nil {
		return
	}
	tr.nodeDescend(&tr.root, pivot, iter, mut)
}

func (tr *Map[K, V]) nodeDescend(cn **mapNode[K, V], pivot K,
	iter func(key K, value V) bool, mut bool,
) bool {
	n := tr.isoLoad(cn, mut)
	i, found := tr.search(n, pivot)
	if !found {
		if !n.leaf() {
			if !tr.nodeDescend(&(*n.children)[i], pivot, iter, mut) {
				return false
			}
		}
		i--
	}
	for ; i >= 0; i-- {
		if !iter(n.items[i].key, n.items[i].value) {
			return false
		}
		if !n.leaf() {
			if !tr.nodeReverse(&(*n.children)[i], iter, mut) {
				return false
			}
		}
	}
	return true
}

func (tr *Map[K, V]) minMut(mut bool) (key K, value V, ok bool) {
	if tr.root == nil {
		return key, value, false
	}
	n := tr.isoLoad(&tr.root, mut)
	for {
		if n.leaf() {
			item := n.items[0]
			return item.key, item.value, true
		}
		n = tr.isoLoad(&(*n.children)[0], mut)
	}
}

func (tr *Map[K, V]) maxMut(mut bool) (K, V, bool) {
	if tr.root == nil {
		return tr.empty.key, tr.empty.value, false
	}
	n := tr.isoLoad(&tr.root, mut)
	for {
		if n.leaf() {
			item := n.items[len(n.items)-1]
			return item.key, item.value, true
		}
		n = tr.isoLoad(&(*n.children)[len(*n.children)-1], mut)
	}
}

func (tr *Map[K, V]) getAt(index int, mut bool) (K, V, bool) {
	if tr.root == nil || index < 0 || index >= tr.count {
		return tr.empty.key, tr.empty.value, false
	}
	n := tr.isoLoad(&tr.root, mut)
	for {
		if n.leaf() {
			return n.items[index].key, n.items[index].value, true
		}
		i := 0
		for ; i < len(n.items); i++ {
			if index < (*n.children)[i].count {
				break
			} else if index == (*n.children)[i].count {
				return n.items[i].key, n.items[i].value, true
			}
			index -= (*n.children)[i].count + 1
		}
		n = tr.isoLoad(&(*n.children)[i], mut)
	}
}

func (tr *Map[K, V]) values(mut bool) []V {
	values := make([]V, 0, tr.Len())
	if tr.root != nil {
		values = tr.nodeValues(&tr.root, values, mut)
	}
	return values
}

func (tr *Map[K, V]) nodeValues(cn **mapNode[K, V], values []V, mut bool) []V {
	n := tr.isoLoad(cn, mut)
	if n.leaf() {
		for i := 0; i < len(n.items); i++ {
			values = append(values, n.items[i].value)
		}
		return values
	}
	for i := 0; i < len(n.items); i++ {
		values = tr.nodeValues(&(*n.children)[i], values, mut)
		values = append(values, n.items[i].value)
	}
	return tr.nodeValues(&(*n.children)[len(*n.children)-1], values, mut)
}

func (n *mapNode[K, V]) keys(keys []K) []K {
	if n.leaf() {
		for i := 0; i < len(n.items); i++ {
			keys = append(keys, n.items[i].key)
		}
		return keys
	}
	for i := 0; i < len(n.items); i++ {
		keys = (*n.children)[i].keys(keys)
		keys = append(keys, n.items[i].key)
	}
	return (*n.children)[len(*n.children)-1].keys(keys)
}

func (tr *Map[K, V]) keyValues(mut bool) ([]K, []V) {
	keys := make([]K, 0, tr.Len())
	values := make([]V, 0, tr.Len())
	if tr.root != nil {
		keys, values = tr.nodeKeyValues(&tr.root, keys, values, mut)
	}
	return keys, values
}

func (tr *Map[K, V]) nodeKeyValues(cn **mapNode[K, V], keys []K, values []V,
	mut bool,
) ([]K, []V) {
	n := tr.isoLoad(cn, mut)
	if n.leaf() {
		for i := 0; i < len(n.items); i++ {
			keys = append(keys, n.items[i].key)
			values = append(values, n.items[i].value)
		}
		return keys, values
	}
	for i := 0; i < len(n.items); i++ {
		keys, values = tr.nodeKeyValues(&(*n.children)[i], keys, values, mut)
		keys = append(keys, n.items[i].key)
		values = append(values, n.items[i].value)
	}
	return tr.nodeKeyValues(&(*n.children)[len(*n.children)-1], keys, values,
		mut)
}

// iterator

func (tr *Map[K, V]) iter(mut bool) MapIter[K, V] {
	var iter MapIter[K, V]
	iter.tr = tr
	iter.mut = mut
	return iter
}
