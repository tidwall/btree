// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
package btree

type Map[K ordered, V any] struct {
	isoid         uint64
	root          *mapNode[K, V]
	count         int
	empty         mapPair[K, V]
	min           int // min items
	max           int // max items
	copyValues    bool
	isoCopyValues bool
}

func NewMap[K ordered, V any](degree int) *Map[K, V] {
	m := new(Map[K, V])
	m.init(degree)
	return m
}

type mapNode[K ordered, V any] struct {
	isoid    uint64
	count    int
	items    []mapPair[K, V]
	children *[]*mapNode[K, V]
}

func (tr *Map[K, V]) Copy() *Map[K, V] {
	return tr.IsoCopy()
}

func (tr *Map[K, V]) IsoCopy() *Map[K, V] {
	tr2 := new(Map[K, V])
	*tr2 = *tr
	tr2.isoid = newIsoID()
	tr.isoid = newIsoID()
	return tr2
}

// Set or replace a value for a key
func (tr *Map[K, V]) Set(key K, value V) (V, bool) {
	item := mapPair[K, V]{key: key, value: value}
	if tr.root == nil {
		tr.init(0)
		tr.root = tr.newNode(true)
		tr.root.items = append([]mapPair[K, V]{}, item)
		tr.root.count = 1
		tr.count = 1
		return tr.empty.value, false
	}
	prev, replaced, split := tr.nodeSet(&tr.root, item)
	if split {
		left := tr.root
		right, median := tr.nodeSplit(left)
		tr.root = tr.newNode(false)
		*tr.root.children = make([]*mapNode[K, V], 0, tr.max+1)
		*tr.root.children = append([]*mapNode[K, V]{}, left, right)
		tr.root.items = append([]mapPair[K, V]{}, median)
		tr.root.updateCount()
		return tr.Set(item.key, item.value)
	}
	if replaced {
		return prev, true
	}
	tr.count++
	return tr.empty.value, false
}

func (tr *Map[K, V]) Scan(iter func(key K, value V) bool) {
	tr.scan(iter, false)
}

func (tr *Map[K, V]) ScanMut(iter func(key K, value V) bool) {
	tr.scan(iter, true)
}

// Get a value for key.
func (tr *Map[K, V]) Get(key K) (V, bool) {
	return tr.get(key, false)
}

// GetMut gets a value for key.
// If needed, this may perform a copy the resulting value before returning.
//
// Mut methods are only useful when all of the following are true:
//   - The interior data of the value requires changes.
//   - The value is a pointer type.
//   - The BTree has been copied using `Copy()` or `IsoCopy()`.
//   - The value itself has a `Copy()` or `IsoCopy()` method.
//
// Mut methods may modify the tree structure and should have the same
// considerations as other mutable operations like Set, Delete, Clear, etc.
func (tr *Map[K, V]) GetMut(key K) (V, bool) {
	return tr.get(key, true)
}

// Len returns the number of items in the tree
func (tr *Map[K, V]) Len() int {
	return tr.count
}

// Delete a value for a key and returns the deleted value.
// Returns false if there was no value by that key found.
func (tr *Map[K, V]) Delete(key K) (V, bool) {
	if tr.root == nil {
		return tr.empty.value, false
	}
	prev, deleted := tr.delete(&tr.root, false, key)
	if !deleted {
		return tr.empty.value, false
	}
	if len(tr.root.items) == 0 && !tr.root.leaf() {
		tr.root = (*tr.root.children)[0]
	}
	tr.count--
	if tr.count == 0 {
		tr.root = nil
	}
	return prev.value, true
}

// Ascend the tree within the range [pivot, last]
// Pass nil for pivot to scan all item in ascending order
// Return false to stop iterating
func (tr *Map[K, V]) Ascend(pivot K, iter func(key K, value V) bool) {
	tr.ascend(pivot, iter, false)
}

func (tr *Map[K, V]) AscendMut(pivot K, iter func(key K, value V) bool) {
	tr.ascend(pivot, iter, true)
}

func (tr *Map[K, V]) Reverse(iter func(key K, value V) bool) {
	tr.reverse(iter, false)
}

func (tr *Map[K, V]) ReverseMut(iter func(key K, value V) bool) {
	tr.reverse(iter, true)
}

// Descend the tree within the range [pivot, first]
// Pass nil for pivot to scan all item in descending order
// Return false to stop iterating
func (tr *Map[K, V]) Descend(pivot K, iter func(key K, value V) bool) {
	tr.descend(pivot, iter, false)
}

func (tr *Map[K, V]) DescendMut(pivot K, iter func(key K, value V) bool) {
	tr.descend(pivot, iter, true)
}

// Load is for bulk loading pre-sorted items
func (tr *Map[K, V]) Load(key K, value V) (V, bool) {
	item := mapPair[K, V]{key: key, value: value}
	if tr.root == nil {
		return tr.Set(item.key, item.value)
	}
	n := tr.isoLoad(&tr.root, true)
	for {
		n.count++ // optimistically update counts
		if n.leaf() {
			if len(n.items) < tr.max {
				if n.items[len(n.items)-1].key < item.key {
					n.items = append(n.items, item)
					tr.count++
					return tr.empty.value, false
				}
			}
			break
		}
		n = tr.isoLoad(&(*n.children)[len(*n.children)-1], true)
	}
	// revert the counts
	n = tr.root
	for {
		n.count--
		if n.leaf() {
			break
		}
		n = (*n.children)[len(*n.children)-1]
	}
	return tr.Set(item.key, item.value)
}

// Min returns the minimum item in tree.
// Returns nil if the treex has no items.
func (tr *Map[K, V]) Min() (K, V, bool) {
	return tr.minMut(false)
}

func (tr *Map[K, V]) MinMut() (K, V, bool) {
	return tr.minMut(true)
}

// Max returns the maximum item in tree.
// Returns nil if the tree has no items.
func (tr *Map[K, V]) Max() (K, V, bool) {
	return tr.maxMut(false)
}

func (tr *Map[K, V]) MaxMut() (K, V, bool) {
	return tr.maxMut(true)
}

// PopMin removes the minimum item in tree and returns it.
// Returns nil if the tree has no items.
func (tr *Map[K, V]) PopMin() (K, V, bool) {
	if tr.root == nil {
		return tr.empty.key, tr.empty.value, false
	}
	n := tr.isoLoad(&tr.root, true)
	var item mapPair[K, V]
	for {
		n.count-- // optimistically update counts
		if n.leaf() {
			item = n.items[0]
			if len(n.items) == tr.min {
				break
			}
			copy(n.items[:], n.items[1:])
			n.items[len(n.items)-1] = tr.empty
			n.items = n.items[:len(n.items)-1]
			tr.count--
			if tr.count == 0 {
				tr.root = nil
			}
			return item.key, item.value, true
		}
		n = tr.isoLoad(&(*n.children)[0], true)
	}
	// revert the counts
	n = tr.root
	for {
		n.count++
		if n.leaf() {
			break
		}
		n = (*n.children)[0]
	}
	value, deleted := tr.Delete(item.key)
	if deleted {
		return item.key, value, true
	}
	return tr.empty.key, tr.empty.value, false
}

// PopMax removes the maximum item in tree and returns it.
// Returns nil if the tree has no items.
func (tr *Map[K, V]) PopMax() (K, V, bool) {
	if tr.root == nil {
		return tr.empty.key, tr.empty.value, false
	}
	n := tr.isoLoad(&tr.root, true)
	var item mapPair[K, V]
	for {
		n.count-- // optimistically update counts
		if n.leaf() {
			item = n.items[len(n.items)-1]
			if len(n.items) == tr.min {
				break
			}
			n.items[len(n.items)-1] = tr.empty
			n.items = n.items[:len(n.items)-1]
			tr.count--
			if tr.count == 0 {
				tr.root = nil
			}
			return item.key, item.value, true
		}
		n = tr.isoLoad(&(*n.children)[len(*n.children)-1], true)
	}
	// revert the counts
	n = tr.root
	for {
		n.count++
		if n.leaf() {
			break
		}
		n = (*n.children)[len(*n.children)-1]
	}
	value, deleted := tr.Delete(item.key)
	if deleted {
		return item.key, value, true
	}
	return tr.empty.key, tr.empty.value, false
}

// GetAt returns the value at index.
// Return nil if the tree is empty or the index is out of bounds.
func (tr *Map[K, V]) GetAt(index int) (K, V, bool) {
	return tr.getAt(index, false)
}

func (tr *Map[K, V]) GetAtMut(index int) (K, V, bool) {
	return tr.getAt(index, true)
}

// DeleteAt deletes the item at index.
// Return nil if the tree is empty or the index is out of bounds.
func (tr *Map[K, V]) DeleteAt(index int) (K, V, bool) {
	if tr.root == nil || index < 0 || index >= tr.count {
		return tr.empty.key, tr.empty.value, false
	}
	var pathbuf [8]uint8 // track the path
	path := pathbuf[:0]
	var item mapPair[K, V]
	n := tr.isoLoad(&tr.root, true)
outer:
	for {
		n.count-- // optimistically update counts
		if n.leaf() {
			// the index is the item position
			item = n.items[index]
			if len(n.items) == tr.min {
				path = append(path, uint8(index))
				break outer
			}
			copy(n.items[index:], n.items[index+1:])
			n.items[len(n.items)-1] = tr.empty
			n.items = n.items[:len(n.items)-1]
			tr.count--
			if tr.count == 0 {
				tr.root = nil
			}
			return item.key, item.value, true
		}
		i := 0
		for ; i < len(n.items); i++ {
			if index < (*n.children)[i].count {
				break
			} else if index == (*n.children)[i].count {
				item = n.items[i]
				path = append(path, uint8(i))
				break outer
			}
			index -= (*n.children)[i].count + 1
		}
		path = append(path, uint8(i))
		n = tr.isoLoad(&(*n.children)[i], true)
	}
	// revert the counts
	n = tr.root
	for i := 0; i < len(path); i++ {
		n.count++
		if !n.leaf() {
			n = (*n.children)[uint8(path[i])]
		}
	}
	value, deleted := tr.Delete(item.key)
	if deleted {
		return item.key, value, true
	}
	return tr.empty.key, tr.empty.value, false
}

// Height returns the height of the tree.
// Returns zero if tree has no items.
func (tr *Map[K, V]) Height() int {
	var height int
	if tr.root != nil {
		n := tr.root
		for {
			height++
			if n.leaf() {
				break
			}
			n = (*n.children)[0]
		}
	}
	return height
}

// Iter returns a read-only iterator.
func (tr *Map[K, V]) Iter() MapIter[K, V] {
	return tr.iter(false)
}

func (tr *Map[K, V]) IterMut() MapIter[K, V] {
	return tr.iter(true)
}

// Values returns all the values in order.
func (tr *Map[K, V]) Values() []V {
	return tr.values(false)
}

func (tr *Map[K, V]) ValuesMut() []V {
	return tr.values(true)
}

// Keys returns all the keys in order.
func (tr *Map[K, V]) Keys() []K {
	keys := make([]K, 0, tr.Len())
	if tr.root != nil {
		keys = tr.root.keys(keys)
	}
	return keys
}

// KeyValues returns all the keys and values in order.
func (tr *Map[K, V]) KeyValues() ([]K, []V) {
	return tr.keyValues(false)
}

func (tr *Map[K, V]) KeyValuesMut() ([]K, []V) {
	return tr.keyValues(true)
}

// Clear will delete all items.
func (tr *Map[K, V]) Clear() {
	tr.count = 0
	tr.root = nil
}
